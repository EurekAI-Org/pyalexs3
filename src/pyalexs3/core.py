from __future__ import annotations

import datetime
import glob
import os
import re
import shutil
import signal
import time
from collections.abc import Generator
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from threading import Event

import boto3
import duckdb
from botocore import UNSIGNED
from botocore.config import Config
from rich import print as rprint
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .schemas import (
    AUTHORS_SCHEMA,
    CONCEPT_SCHEMA,
    FUNDER_SCHEMA,
    INSTITUION_SCHEMA,
    KEYWORD_SCHEMA,
    PUBLISHER_SCHEMA,
    SOURCE_SCHEMA,
    TOPIC_SCHEMA,
    WORKS_SCHEMA,
)

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
      3) Load with DuckDB `read_json(...)` using a known schema.
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
        persist_path: str | None = None,
    ):

        self.__s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        self.__n_workers = n_workers
        self.__persist_path = persist_path

        if self.__persist_path is not None:
            # os.makedirs(os.path.dirname(persist_path), exist_ok=True)
            path = Path(self.__persist_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.__conn = duckdb.connect(self.__persist_path)
        else:
            self.__conn = duckdb.connect()

        self.__conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.__conn.execute("PRAGMA enable_progress_bar=true;")
        self.__conn.execute("PRAGMA enable_object_cache=true;")
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

    def __get_sub_schema(self, schema: dict, cols: str) -> dict:
        new_schema = {}
        for col in cols.split(","):
            new_schema[col] = schema[col]

        return new_schema

    def __get_schema(self, obj_type: str, cols: str) -> dict:
        accepted_types = [
            "works",
            "authors",
            "sources",
            "institutions",
            "topics",
            "keywords",
            "publishers",
            "funders",
            "concepts",
        ]
        schemas = {
            "works": WORKS_SCHEMA,
            "authors": AUTHORS_SCHEMA,
            "sources": SOURCE_SCHEMA,
            "institutions": INSTITUION_SCHEMA,
            "topics": TOPIC_SCHEMA,
            "keywords": KEYWORD_SCHEMA,
            "publishers": PUBLISHER_SCHEMA,
            "funders": FUNDER_SCHEMA,
            "concepts": CONCEPT_SCHEMA,
        }

        if obj_type in accepted_types:
            return (
                schemas[obj_type]
                if cols == "*"
                else self.__get_sub_schema(schema=schemas[obj_type], cols=cols)
            )

        raise ValueError(f"Unsupported obj_type: {obj_type!r}")

    def __extract_date(self, txt: str):
        pat = re.compile(r"(updated_date=([0-9]+-[0-9]+-[0-9]+))")
        mat = pat.search(txt)
        return mat.group(2) if mat is not None else ""

    def __get_start_date(self, obj_type: str) -> str:

        start_date = datetime.date.today()

        paginator = self.__s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue

                dat = self.__extract_date(obj["Key"])
                if dat != "" and datetime.date.fromisoformat(dat) < start_date:
                    return dat

        return start_date.isoformat()

    def __check_date_fmt(self, txt: str) -> bool:
        try:
            datetime.date.fromisoformat(txt)
            return True
        except ValueError:
            return False

    def __get_files(self, obj_type: str, start_date: str, end_date: str) -> list[str]:
        file_list = []
        st = datetime.date.fromisoformat(start_date)
        ed = datetime.date.fromisoformat(end_date)

        paginator = self.__s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue
                dat = self.__extract_date(obj["Key"])
                if st <= datetime.date.fromisoformat(dat) <= ed:
                    file_list.append(obj["Key"])

        return file_list

    def __get_batch_files(
        self, obj_type: str, start_date: str, end_date: str, batch_sz: int
    ) -> Generator[list[str], None, None]:

        file_list: list[str] = []

        st = datetime.date.fromisoformat(start_date)
        ed = datetime.date.fromisoformat(end_date)

        paginator = self.__s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):

                if len(file_list) >= batch_sz:
                    yield file_list
                    file_list = []

                if obj["Key"].split("/")[-1].lower() == "manifest":
                    continue

                dat = self.__extract_date(obj["Key"])
                if st <= datetime.date.fromisoformat(dat) <= ed:
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
        parts: str | list[int],
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

        futures = []

        with self.__progress:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for f in files:
                    try:
                        file_sz = self.__s3_client.head_object(
                            Bucket="openalex", Key=f
                        )["ContentLength"]
                        task_id = self.__progress.add_task(
                            "Downloading", filename=f, total=file_sz
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
        files: list[str],
        parts: str | list[int],
        download_dir: str,
    ):

        if parts != "*":
            files = [
                _f
                for _f in files
                if int(_f.split("/")[-1].replace("part_", "").replace(".gz", ""))
                in set(parts)
            ]

        futures = []
        with self.__progress:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for f in files:
                    try:
                        file_sz = self.__s3_client.head_object(
                            Bucket="openalex", Key=f
                        )["ContentLength"]
                        task_id = self.__progress.add_task(
                            "Downloading", filename=f, total=file_sz
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
        start_date: str | None = None,
        end_date: str | None = None,
        parts: list[int] | None = None,
        cols: list[str] | None = None,
        limit: int | None = None,
        batch_sz: int | None = None,
        where_clause: str | None = None,
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
            "concepts",
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
            raise ValueError("Expected end_date of the format 'YYYY-mm-dd'")

        if end_date is not None and not self.__check_date_fmt(end_date):
            raise ValueError("Expected end_date of the format 'YYYY-mm-dd'")

        if parts is not None and not isinstance(parts, list):
            raise ValueError(
                f"Expected parts to be of type 'list'. Found {type(parts)}"
            )

        if parts is not None and not all(isinstance(p, int) for p in parts):
            raise ValueError("Expected parts to be a list<int>.")

        if cols is not None and not isinstance(cols, list):
            raise ValueError(
                f"Expected cols to be of type 'list'. Found type {type(cols)}"
            )

        if cols is not None and not all(isinstance(col, str) for col in cols):
            raise ValueError("Expected cols to be of type 'list<str>'")

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
        if (
            start_date is not None
            and end_date is not None
            and datetime.date.fromisoformat(start_date)
            > datetime.date.fromisoformat(end_date)
        ):
            raise ValueError(
                f"start_date cannot be greater than end_date. Found start_date={start_date}, end_date={end_date}"
            )

    def load_table(
        self,
        obj_type: str,
        cols: list[str] | None = None,
        limit: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        parts: list[int] | None = None,
        download_dir: str = "./.cache/oa",
        where_clause: str | None = None,
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

        parts_sel = "*" if parts is None else parts
        start_date_sel = (
            self.__get_start_date(obj_type) if start_date is None else start_date
        )
        end_date_sel = (
            datetime.date.today().isoformat() if end_date is None else end_date
        )
        cols_sel = "*" if cols is None else ",".join(cols)
        limit_sel = f" LIMIT {limit}" if limit is not None else ""
        where_sel = f" {where_clause.strip()}" if where_clause is not None else ""

        rprint("Downloading the files from s3...")

        self.__download_files(
            obj_type=obj_type,
            start_date=start_date_sel,
            end_date=end_date_sel,
            parts=parts_sel,
            download_dir=download_dir,
        )
        dwnld_files = glob.glob(download_dir + "/*.gz")

        rprint("[yellow]Creating table...")

        t0 = time.time()

        select_clause = f"SELECT {cols_sel} FROM read_json('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type, cols=cols_sel)}, format='newline_delimited', hive_partitioning=true){where_sel}{limit_sel}"

        exists_cmd = self.__conn.execute(
            f"SELECT count(*) FROM duckdb_tables() WHERE table_name='{obj_type}'"
        ).fetchone()
        if exists_cmd is not None:
            table_exists = exists_cmd[0] > 0

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

        if len(dwnld_files):
            self.__conn.execute(sql_query)
            rprint(f"[green]Table creation complete in {time.time() - t0:.3f} secs")
        else:
            rprint(
                f"[red]Could not find any files on S3 for {obj_type} between {start_date_sel} - {end_date_sel}"
            )

        shutil.rmtree(download_dir)

    def batch_load_table(
        self,
        obj_type: str,
        batch_sz: int = 10,
        cols: list[str] | None = None,
        limit: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        parts: list[int] | None = None,
        download_dir: str = "./.cache/oa",
        where_clause: str | None = None,
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

        parts_sel = "*" if parts is None else parts
        start_date_sel = (
            self.__get_start_date(obj_type) if start_date is None else start_date
        )
        end_date_sel = (
            datetime.date.today().isoformat() if end_date is None else end_date
        )
        cols_sel = "*" if cols is None else ",".join(cols)
        limit_sel = f" LIMIT {limit}" if limit is not None else ""
        where_sel = f" {where_clause.strip()}" if where_clause is not None else ""

        files_gen = self.__get_batch_files(
            obj_type=obj_type,
            batch_sz=batch_sz,
            start_date=start_date_sel,
            end_date=end_date_sel,
        )

        t0 = time.time()
        select_clause = f"SELECT {cols_sel} FROM read_json('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type, cols=cols_sel)}, format='newline_delimited', hive_partitioning=true){where_sel}{limit_sel}"

        for file_ls in files_gen:

            exists_cmd = self.__conn.execute(
                f"SELECT count(*) FROM duckdb_tables() WHERE table_name='{obj_type}'"
            ).fetchone()
            if exists_cmd is not None:
                table_exists = exists_cmd[0] > 0

            os.makedirs(download_dir, exist_ok=True)
            self.__batch_download_files(
                files=file_ls,
                parts=parts_sel,
                download_dir=download_dir,
            )
            dwnld_files = glob.glob(download_dir + "/*.gz")

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
            if len(dwnld_files):
                self.__conn.execute(sql_query)
                rprint(
                    f"[bold green] Table loading complete in {time.time() - t0:.4f} secs"
                )
            else:
                rprint(
                    f"[red]Could not find any files on S3 for {obj_type} between {start_date_sel} - {end_date_sel}"
                )

            shutil.rmtree(download_dir)

    def lazy_load(
        self,
        obj_type: str,
        cols: list[str] | None = None,
        batch_sz: int = 10,
        where_clause: str | None = None,
        limit: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        parts: list[int] | None = None,
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

        parts_sel = "*" if parts is None else parts
        start_date_sel = (
            self.__get_start_date(obj_type) if start_date is None else start_date
        )
        end_date_sel = (
            datetime.date.today().isoformat() if end_date is None else end_date
        )
        cols_sel = "*" if cols is None else ",".join(cols)
        limit_sel = f" LIMIT {limit}" if limit is not None else ""
        where_sel = f" {where_clause.strip()}" if where_clause is not None else ""

        files_gen = self.__get_batch_files(
            obj_type=obj_type,
            batch_sz=batch_sz,
            start_date=start_date_sel,
            end_date=end_date_sel,
        )

        select_clause = f"SELECT {cols_sel} FROM read_json('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type, cols=cols_sel)}, format='newline_delimited', hive_partitioning=true){where_sel}{limit_sel}"

        for fb in files_gen:
            os.makedirs(download_dir, exist_ok=True)

            self.__batch_download_files(
                files=fb,
                parts=parts_sel,
                download_dir=download_dir,
            )

            rel = self.__conn.sql(select_clause)

            try:
                yield rel

            finally:

                shutil.rmtree(download_dir)

    def get_table(
        self, obj_type: str, cols: list[str] | None = None
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

        if cols is not None and not all(isinstance(col, str) for col in cols):
            raise ValueError("Expected cols to be of type 'list<str>'")

        cols_sel = "*" if cols is None else cols

        return self.__conn.sql(f"SELECT {cols_sel} FROM {obj_type}")

    @property
    def s3_obj_types(self) -> list[str]:
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
