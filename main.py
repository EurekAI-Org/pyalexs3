import shutil
from typing import Dict, List, Optional

import duckdb
import boto3
import time
import os
import re
import signal
import datetime

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

from utils import WORKS_SCHEMA


done_event = Event()


def handle_sigint(signum, frame):
    done_event.set()


signal.signal(signal.SIGINT, handle_sigint)


class OpenAlexParser:
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
    ) -> List[str]:
        files = self.__get_files(
            obj_type=obj_type, start_date=start_date, end_date=end_date
        )

        if parts != "*":
            files = [
                f
                for f in files
                if int(f.split("/")[-1].replace("part_", "").replace(".gz", ""))
                in parts
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

    def load_table(
        self,
        obj_type: str,
        cols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parts: Optional[List[int]] = None,
        download_dir: str = "./.cache/oa",
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
            raise ValueError("Expected parts to be a list of integers.")

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

        os.makedirs(download_dir, exist_ok=True)

        parts = "*" if parts is None else parts
        start_date = self.__get_start_date() if start_date is None else start_date
        end_date = datetime.date.today() if end_date is None else end_date
        cols = "*" if cols is None else cols
        limit = f" LIMIT {limit}" if limit is not None else ""

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

        select_clause = f"SELECT {cols} FROM read_ndjson_auto('{download_dir}/*', columns={self.__get_schema(obj_type=obj_type)}){limit}"

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


oaparser = OpenAlexParser()
oaparser.load_table(obj_type="works", start_date="2025-08-04", end_date="2025-08-05")

table = oaparser.get_table(obj_type="works")
table.show()
