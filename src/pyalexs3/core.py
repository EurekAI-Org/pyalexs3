from __future__ import annotations
from typing import Dict, Generator, List, Optional

import duckdb
import boto3
import time
import os
import re
import signal
import datetime
import shutil

from botocore import UNSIGNED
from botocore.config import Config
from threading import Event
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich import print as rprint

from .schemas import WORKS_SCHEMA


done_event = Event()


def handle_sigint(signum, frame):
    done_event.set()


signal.signal(signal.SIGINT, handle_sigint)


class OpenAlexS3Processor:
    """
    Download OpenAlex NDJSON dumps from S3 and load them into DuckDB.

    Flow:
      1) List S3 keys for an object type, filter by date (and optional parts).
      2) Download in parallel with Rich progress bars.
      3) Load with DuckDB `read_ndjson_auto(...)` using a known schema.
      4) Create or append to a DuckDB table, then clean up temp files.

    Side Effects:
      - Installs/loads DuckDB `httpfs` extension.
      - Creates (and may drop) DuckDB tables.
      - Deletes `download_dir` after each operation.
      - Performs network I/O to S3; uses a thread pool.
    """

    def __init__(
        self,
        n_workers: int = 4,
        persist_path: Optional[str] = None,
    ):

        self.__s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        self.__n_workers = n_workers
        self.__persist_path = persist_path

        if self.__persist_path is not None:
            os.makedirs(os.path.dirname(persist_path), exist_ok=True)
            self.__conn = duckdb.connect(self.__persist_path)
        else:
            self.__conn = duckdb.connect()

        self.__conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.__conn.execute("PRAGMA enable_progress_bar=true;")
        self.__conn.execute(f"PRAGMA threads={self.__n_workers};")

        self.__progress = Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            DownloadColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
        )

    def __get_schema(self, obj_type: str) -> Dict:
        if obj_type == "works":
            return WORKS_SCHEMA

    def __extract_date(self, txt: str):
        pat = re.compile(r"(updated_date=([0-9]+-[0-9]+-[0-9]+))")
        mat = pat.search(txt)
        return mat.group(2) if mat is not None else ""

    def __get_start_date(self) -> str:

        start_date = datetime.date.today()

        paginator = self.__s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket="openalex", Prefix=f"data/{self.__obj_type}/"
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue

                dat = self.__extract_date(obj["Key"])
                if dat != "" and datetime.date.fromisoformat(dat) < start_date:
                    return dat

    def __check_date_fmt(self, txt: str) -> bool:
        try:
            datetime.date.fromisoformat(txt)
            return True
        except ValueError:
            return False

    def __extract_date(self, txt: str) -> str:
        pat = re.compile(r"(updated_date=([0-9]+-[0-9]+-[0-9]+))")
        mat = pat.search(txt)

        return mat.group(2) if mat is not None else ""

    def __get_files(self, obj_type: str, start_date: str, end_date: str) -> List[str]:
        file_list = list()
        start_date = datetime.date.fromisoformat(start_date)
        end_date = datetime.date.fromisoformat(end_date)

        paginator = self.__s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue
                dat = self.__extract_date(obj["Key"])
                if start_date <= datetime.date.fromisoformat(dat) <= end_date:
                    file_list.append(obj["Key"])

        return file_list

    def __get_batch_files(
        self, obj_type: str, start_date: str, end_date: str, batch_sz: int
    ) -> Generator[List[str], None, None]:

        file_list = list()

        start_date = datetime.date.fromisoformat(start_date)
        end_date = datetime.date.fromisoformat(end_date)

        paginator = self.__s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):

                if len(file_list) >= batch_sz:
                    yield file_list
                    file_list = []

                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue

                dat = self.__extract_date(obj["Key"])
                if start_date <= datetime.date.fromisoformat(dat) <= end_date:
                    file_list.append(obj["Key"])

        if len(file_list):
            yield file_list

    def __copy_data(self, taskId: TaskID, key: str, download_dir: str):

        def update_progress(bytes_amt: float):
            self.__progress.update(task_id=taskId, advance=bytes_amt)

        size = self.__s3_client.head_object(Bucket="openalex", Key=key)["ContentLength"]
        file_name = os.path.join(download_dir, "_".join(key.split("/")[-2:]))
        self.__s3_client.download_file(
            Filename=file_name,
            Bucket="openalex",
            Key=key,
            Callback=update_progress,
        )

        self.__progress.update(taskId, completed=size)

    def __download_files(
        self,
        obj_type: str,
        start_date: str,
        end_date: str,
        parts: str | List[int],
        download_dir: str,
    ):
        files = self.__get_files(
            obj_type=obj_type, start_date=start_date, end_date=end_date
        )

        if parts != "*":
            files = [
                f
                for f in files
                if int(f.split("/")[-1].replace("part_", "").replace(".gz", ""))
                in set(parts)
            ]

        futures = list()

        with self.__progress:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for f in files:
                    try:
                        file_sz = self.__s3_client.head_object(
                            Bucket="openalex", Key=f
                        )["ContentLength"]
                        task_id = self.__progress.add_task(
                            f"Downloading", filename=f, total=file_sz
                        )
                        future = pool.submit(self.__copy_data, task_id, f, download_dir)
                        futures.append(future)
                    except Exception as e:
                        self.__progress.log(
                            f"[bold red] ERROR getting size for {f}: {e}[/bold red]"
                        )
            wait(futures, return_when=ALL_COMPLETED)

    def __batch_download_files(
        self,
        files: List[str],
        parts: str | List[int],
        download_dir: str,
    ):

        if parts != "*":
            files = [
                _f
                for _f in files
                if int(_f.split("/")[-1].replace("part_", "").replace(".gz", ""))
                in set(parts)
            ]

        futures = list()
        with self.__progress:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for f in files:
                    try:
                        file_sz = self.__s3_client.head_object(
                            Bucket="openalex", Key=f
                        )["ContentLength"]
                        task_id = self.__progress.add_task(
                            f"Downloading", filename=f, total=file_sz
                        )
                        future = pool.submit(self.__copy_data, task_id, f, download_dir)
                        futures.append(future)
                    except Exception as e:
                        self.__progress.log(
                            f"[bold red] ERROR getting size for {f}: {e}[/bold red]"
                        )
            wait(futures, return_when=ALL_COMPLETED)

    def __type_check(
        self,
        obj_type: str,
        download_dir: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parts: Optional[List[int]] = None,
        cols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        batch_sz: Optional[int] = None,
        where_clause: Optional[str] = None,
    ):
        assert isinstance(
            obj_type, str
        ), f"Expected obj_type to be str. Found {type(obj_type)}"

        accepted_types = [
            "works",
            "authors",
            "sources",
            "institutions",
            "topics",
            "keywords",
            "publishers",
            "funders",
            "geo",
        ]

        assert (
            obj_type in accepted_types
        ), f"Expected obj_type to either {'/'.join(accepted_types)}. Found {obj_type}"

        assert isinstance(
            download_dir, str
        ), f"Expected download_dir to be of type <class 'str'>. Found type:{type(download_dir)}"

        if start_date is not None and not isinstance(start_date, str):
            raise ValueError(
                f"Expected start_date to be of type 'str'. Found type {type(start_date)}"
            )
        if end_date is not None and not isinstance(end_date, str):
            raise ValueError(
                f"Expected end_date to be of type 'str'. Found type {type(end_date)}"
            )

        if start_date is not None and not self.__check_date_fmt(start_date):
            raise ValueError(f"Expected end_date of the format 'YYYY-mm-dd'")

        if end_date is not None and not self.__check_date_fmt(end_date):
            raise ValueError(f"Expected end_date of the format 'YYYY-mm-dd'")

        if parts is not None and not isinstance(parts, list):
            raise ValueError(
                f"Expected parts to be of type 'list'. Found {type(parts)}"
            )

        if parts is not None and not all([isinstance(p, int) for p in parts]):
            raise ValueError("Expected parts to be a list<int>.")

        if cols is not None and not isinstance(cols, list):
            raise ValueError(
                f"Expected cols to be of type 'list'. Found type {type(cols)}"
            )

        if cols is not None and not all([isinstance(col, str) for col in cols]):
            raise ValueError(f"Expected cols to be of type 'list<str>'")

        if limit is not None and not isinstance(limit, int):
            raise ValueError(
                f"Expected limit to be of type 'int'. Found type {type(limit)}"
            )

        if batch_sz is not None and not isinstance(batch_sz, int):
            raise ValueError(
                f"Expected batch_sz to be of type 'int'. Found type {type(batch_sz)}"
            )

        if where_clause is not None and not isinstance(where_clause, str):
            raise ValueError(
                f"Expected where_clause to be of type 'str'. Found type {type(where_clause)}"
            )

    def load_table(
        self,
        obj_type: str,
        cols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parts: Optional[List[int]] = None,
        download_dir: str = "./.cache/oa",
        where_clause: Optional[str] = None,
    ):
        """
        Loads all the *.gz files in OpenAlex S3 directories as one complete table.

        Parameters:
        -----------
        obj_type: str
            The OpenAlex object type i.e. 'works', 'authors', 'sources', etc.

        cols: Optional[List[str]] = None
            Specific list of columns that needs to be loaded from the table.

        limit: Optional[int] = None
            Limit the number of records to be loaded into the table.

        start_date: Optional[str] = None
            The starting date from which the processing should begin.

        end_date: Optional[str] = None
            The ending date at which the processing should stop.

        parts: Optional[List[int]] = None
            The part number to load from each date.

        download_dir: str; default="./.cache/oa"
            Folder path where the gzip files will be downloaded temporarily.

        where_clause: Optional[str] = None
            A SQL-like where clause to filter out the necessary table.
        """
        self.__type_check(
            obj_type=obj_type,
            download_dir=download_dir,
            start_date=start_date,
            end_date=end_date,
            parts=parts,
            cols=cols,
            limit=limit,
            where_clause=where_clause,
        )

        os.makedirs(download_dir, exist_ok=True)

        parts = "*" if parts is None else parts
        start_date = self.__get_start_date() if start_date is None else start_date
        end_date = datetime.date.today() if end_date is None else end_date
        cols = "*" if cols is None else ",".join(cols)
        limit = f" LIMIT {limit}" if limit is not None else ""
        where_clause = f" {where_clause.strip()}" if where_clause is not None else ""

        rprint("Downloading the files from s3...")

        self.__download_files(
            obj_type=obj_type,
            start_date=start_date,
            end_date=end_date,
            parts=parts,
            download_dir=download_dir,
        )

        rprint("[yellow]Creating table...")

        t0 = time.time()

        select_clause = f"SELECT {cols} FROM read_ndjson_auto('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type)}){where_clause}{limit}"

        table_exists = (
            self.__conn.execute(
                f"SELECT count(*) FROM duckdb_tables() WHERE table_name='{obj_type}'"
            ).fetchone()[0]
            > 0
        )

        if table_exists:
            sql_query = f"INSERT INTO {obj_type} {select_clause}"
        else:
            if self.__persist_path is None:
                sql_query = f"""
                CREATE TEMPORARY TABLE {obj_type} AS 
                {select_clause}
                """
            else:
                sql_query = f"""
                CREATE TABLE {obj_type} AS 
                {select_clause}
                """

        self.__conn.execute(sql_query)

        shutil.rmtree(download_dir)

        rprint(f"[green]Table creation complete in {time.time() - t0:.3f} secs")

    def batch_load_table(
        self,
        obj_type: str,
        batch_sz: int = 10,
        cols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parts: Optional[List[int]] = None,
        download_dir: Optional[str] = "./.cache/oa",
        where_clause: Optional[str] = None,
    ):
        """
        Loads all the *.gz files in OpenAlex S3 directories in batches and appends to one complete table.

        Parameters:
        -----------
        obj_type: str
            The OpenAlex object type i.e. 'works', 'authors', 'sources', etc.

        batch_sz: int; default=10
            The batch size for each batch.

        cols: Optional[List[str]] = None
            Specific list of columns that needs to be loaded from the table.

        limit: Optional[int] = None
            Limit the number of records to be loaded into the table.

        start_date: Optional[str] = None
            The starting date from which the processing should begin.

        end_date: Optional[str] = None
            The ending date at which the processing should stop.

        parts: Optional[List[int]] = None
            The part number to load from each date.

        download_dir: str; default="./.cache/oa"
            Folder path where the gzip files will be downloaded temporarily.

        where_clause: Optional[str] = None
            A SQL-like where clause to filter out the necessary table.
        """
        self.__type_check(
            obj_type=obj_type,
            download_dir=download_dir,
            start_date=start_date,
            end_date=end_date,
            parts=parts,
            cols=cols,
            limit=limit,
            batch_sz=batch_sz,
            where_clause=where_clause,
        )

        parts = "*" if parts is None else parts
        start_date = self.__get_start_date() if start_date is None else start_date
        end_date = datetime.date.today() if end_date is None else end_date
        cols = "*" if cols is None else ",".join(cols)
        limit = f" LIMIT {limit}" if limit is not None else ""
        where_clause = f" {where_clause.strip()}" if where_clause is not None else ""

        files_gen = self.__get_batch_files(
            obj_type=obj_type,
            batch_sz=batch_sz,
            start_date=start_date,
            end_date=end_date,
        )

        t0 = time.time()
        select_clause = f"SELECT {cols} FROM read_ndjson_auto('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type)}){where_clause}{limit}"

        for file_ls in files_gen:

            table_exists = (
                self.__conn.execute(
                    f"SELECT count(*) FROM duckdb_tables() WHERE table_name='{obj_type}'"
                ).fetchone()[0]
                > 0
            )

            os.makedirs(download_dir, exist_ok=True)
            self.__batch_download_files(
                files=file_ls,
                parts=parts,
                download_dir=download_dir,
            )

            if table_exists:
                sql_query = f"INSERT INTO {obj_type} {select_clause}"
            else:
                if self.__persist_path:
                    sql_query = f"""
                    CREATE TEMPORARY TABLE {obj_type} AS 
                    {select_clause}
                    """
                else:
                    sql_query = f"""
                    CREATE TABLE {obj_type} AS 
                    {select_clause}
                    """

            self.__conn.execute(sql_query)
            shutil.rmtree(download_dir)

        rprint(f"[bold green] Table loading complete in {time.time() - t0:.4f} secs")

    def lazy_load(
        self,
        obj_type: str,
        cols: Optional[List[str]] = None,
        batch_sz: int = 10,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parts: Optional[List[int]] = None,
        download_dir: str = "./.cache/oa",
    ) -> Generator[duckdb.DuckDBPyRelation, None, None]:
        """
        Lazy Loads all the *.gz files in OpenAlex S3 directories in batches.

        Parameters:
        -----------
        obj_type: str
            The OpenAlex object type i.e. 'works', 'authors', 'sources', etc.

        cols: Optional[List[str]] = None
            Specific list of columns that needs to be loaded from the table.

        batch_sz: int; default=10
            The batch size for each batch.

        limit: Optional[int] = None
            Limit the number of records to be loaded into the table.

        start_date: Optional[str] = None
            The starting date from which the processing should begin.

        end_date: Optional[str] = None
            The ending date at which the processing should stop.

        parts: Optional[List[int]] = None
            The part number to load from each date.

        download_dir: str; default="./.cache/oa"
            Folder path where the gzip files will be downloaded temporarily.

        where_clause: Optional[str] = None
            A SQL-like where clause to filter out the necessary table.

        Returns:
        --------
        A DuckDBPyRelation object
        """

        self.__type_check(
            obj_type=obj_type,
            download_dir=download_dir,
            start_date=start_date,
            end_date=end_date,
            parts=parts,
            cols=cols,
            limit=limit,
            batch_sz=batch_sz,
            where_clause=where_clause,
        )

        parts = "*" if parts is None else parts
        start_date = self.__get_start_date() if start_date is None else start_date
        end_date = datetime.date.today() if end_date is None else end_date
        cols = "*" if cols is None else ",".join(cols)
        limit = f" LIMIT {limit}" if limit is not None else ""
        where_clause = f" {where_clause.strip()}" if where_clause is not None else ""

        files_gen = self.__get_batch_files(
            obj_type=obj_type,
            batch_sz=batch_sz,
            start_date=start_date,
            end_date=end_date,
        )

        select_clause = f"SELECT {cols} FROM read_ndjson_auto('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type)}){where_clause}{limit}"

        for fb in files_gen:
            os.makedirs(download_dir, exist_ok=True)

            self.__batch_download_files(
                files=fb,
                parts=parts,
                download_dir=download_dir,
            )

            rel = self.__conn.sql(select_clause)

            try:
                yield rel

            finally:

                shutil.rmtree(download_dir)

    def get_table(
        self, obj_type: str, cols: Optional[List[str]] = None
    ) -> duckdb.DuckDBPyRelation:

        assert isinstance(
            obj_type, str
        ), f"Expected obj_type to be str. Found {type(obj_type)}"

        accepted_types = [
            "works",
            "authors",
            "sources",
            "institutions",
            "topics",
            "keywords",
            "publishers",
            "funders",
            "geo",
        ]

        assert (
            obj_type in accepted_types
        ), f"Expected obj_type to either {'/'.join(accepted_types)}. Found {obj_type}"

        if cols is not None and not isinstance(cols, list):
            raise ValueError(
                f"Expected cols to be of type 'list'. Found type {type(cols)}"
            )

        if cols is not None and not all([isinstance(col, str) for col in cols]):
            raise ValueError(f"Expected cols to be of type 'list<str>'")

        cols = "*" if cols is None else cols

        return self.__conn.sql(f"SELECT {cols} FROM {obj_type}")

    @property
    def s3_obj_types(self) -> str:
        objs = [
            "works",
            "authors",
            "sources",
            "institutions",
            "topics",
            "keywords",
            "publishers",
            "funders",
            "geo",
        ]

        return objs
