import datetime
import re
from collections.abc import Generator
from typing import TypedDict

import boto3
import botocore
import duckdb
from botocore.config import Config
from rich.progress import (
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from typing_extensions import Unpack


class OpenAlexS3ProcessorOptions(TypedDict, total=False):
    show_progress: bool
    pragma_show_progress: bool


class OpenAlexS3Processor:
    def __init__(
        self, n_workers: int = 4, **kwargs: Unpack[OpenAlexS3ProcessorOptions]
    ):
        self.__s3_client = boto3.client(
            "s3", config=Config(signature_version=botocore.UNSIGNED)
        )

        self.__conn = duckdb.connect()

        self.__conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.__conn.execute("SET s3_region='us-east-1';")
        self.__conn.execute("SET s3_access_key_id='';")
        self.__conn.execute("SET s3_secret_access_key='';")
        self.__conn.execute("SET s3_session_token='';")
        self.__conn.execute(f"PRAGMA threads={n_workers};")
        if kwargs.get("pragma_show_progress", False):
            self.__conn.execute("PRAGMA enable_progress_bar=true;")

        self.__conn.execute("PRAGMA enable_object_cache=true;")

        self.__progress = None

        if kwargs.get("show_progress", False):
            self.__progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue] {task.description}"),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
            )

    def __extract_date(self, txt: str):
        """
        Extracts the updated_date value from an S3 key.

        Parameters
        ----------
        txt : str
            S3 object key e.g. 'data/works/updated_date=2025-01-15/part_001.gz'

        Returns
        -------
        str
            Date string in 'YYYY-mm-dd' format, or empty string if not found.
        """
        pat = re.compile(r"(updated_date=([0-9]+-[0-9]+-[0-9]+))")
        mat = pat.search(txt)

        return mat.group(2) if mat is not None else ""

    def __get_batch_files(
        self,
        obj_type: str,
        start_date: str,
        end_date: str,
        batch_sz: int,
        parts: list[int] | None = None,
        resume_from: str | None = None,  # Format: YYYY-mm-dd/1
    ) -> Generator[list[str], None, None]:
        """
        Lists S3 file keys for the given object type within a date range
        and yields them in batches.

        Parameters
        ----------
        obj_type : str
            OpenAlex object type e.g. 'works', 'authors', 'sources'.
        start_date : str
            Start of date range in 'YYYY-mm-dd' format (inclusive).
        end_date : str
            End of date range in 'YYYY-mm-dd' format (inclusive).
        batch_sz : int
            Number of files per batch.
        parts : list[int] | None
            If provided, only files whose part number is in this list are included.
            e.g. [1, 2] loads only part_001.gz and part_002.gz from each date.
        resume_from : str | None
            Resume from a specific date and part number. Format: 'YYYY-mm-dd/<part_number>'
            e.g. '2025-01-15/5' skips all files before 2025-01-15 part 5.

        Yields
        ------
        list[str]
            A batch of S3 file keys.
        """

        files: list[str] = []

        st = datetime.date.fromisoformat(start_date)
        et = datetime.date.fromisoformat(end_date)

        resume_date = None
        resume_part = None
        if resume_from is not None:
            resume_date = datetime.date.fromisoformat(resume_from.split("/")[0])
            resume_part = int(resume_from.split("/")[-1])

        paginator = self.__s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket="openalex", Prefix=f"data/{obj_type}/"):
            for obj in page.get("Contents", []):

                if len(files) >= batch_sz:
                    yield files
                    files = []

                key = obj.get("Key") or ""

                if not key:
                    continue

                if key and key.split("/")[-1].lower() == "manifest":
                    continue

                _date_str = self.__extract_date(key)

                if not _date_str:
                    continue

                _date = datetime.date.fromisoformat(_date_str)

                if not (st <= _date <= et):
                    continue

                part_num = int(
                    key.split("/")[-1].replace("part_", "").replace(".gz", "")
                )

                if parts is not None and part_num not in parts:
                    continue

                if resume_date is not None and resume_part is not None:
                    if _date < resume_date:
                        continue
                    if _date == resume_date and part_num < resume_part:
                        continue

                files.append(key)

        if len(files):
            yield files

    def lazy_load(
        self,
        obj_type: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        parts: list[int] | None = None,
        where_clause: str | None = None,
        resume_from: str | None = None,
        batch_size: int = 10,
    ) -> Generator[tuple[list[str], duckdb.DuckDBPyRelation], None, None]:
        """
        Lazily loads OpenAlex data directly from S3 in batches without downloading.

        Each batch downloads a set of files from S3 and returns them as a
        DuckDBPyRelation for further processing. The relation is read lazily —
        data is only fetched when you call .arrow() or .fetchall() on it.

        Parameters
        ----------
        obj_type : str
            OpenAlex object type e.g. 'works', 'authors', 'sources'.
        columns : list[str] | None
            Specific columns to select. If None, all columns are selected.
            e.g. ['id', 'title', 'publication_year']
        limit : int | None
            Maximum number of records to return per batch.
        start_date : str | None
            Start of date range in 'YYYY-mm-dd' format. Defaults to '2016-06-24'.
        end_date : str | None
            End of date range in 'YYYY-mm-dd' format. Defaults to today.
        parts : list[int] | None
            If provided, only files whose part number is in this list are loaded.
            e.g. [1, 2] loads only part_001.gz and part_002.gz from each date.
        where_clause : str | None
            SQL WHERE clause to filter records. Do not include the WHERE keyword.
            e.g. "title IS NOT NULL AND publication_year >= 2020"
        resume_from : str | None
            Resume from a specific date and part number. Format: 'YYYY-mm-dd/<part_number>'
            e.g. '2025-01-15/5' skips all files before 2025-01-15 part 5.
            Useful for resuming after a network failure.
        batch_size : int
            Number of S3 files to process per batch. Default is 10.

        Yields
        ------
        tuple[list[str], duckdb.DuckDBPyRelation]
            A tuple of (file_batch, relation) where:
            - file_batch is the list of S3 keys in this batch, useful for tracking progress.
            - relation is a DuckDBPyRelation over the batch data.

        Examples
        --------
        Basic usage:

            processor = OpenAlexS3Processor(n_workers=4)
            for file_batch, rel in processor.lazy_load(
                obj_type='works',
                start_date='2025-01-01',
                end_date='2025-03-01',
                columns=['id', 'title', 'publication_year'],
                where_clause="title IS NOT NULL AND language='en'",
            ):
                reader = rel.arrow(batch_size=1000)
                for batch in reader:
                    # process batch

        Resume after failure:

            for file_batch, rel in processor.lazy_load(
                obj_type='works',
                start_date='2025-01-01',
                end_date='2025-03-01',
                resume_from='2025-01-15/5',
            ):
                ...
        """

        start_date = "2016-06-24" if start_date is None else start_date
        end_date = datetime.date.today().isoformat() if end_date is None else end_date

        cols = ",".join(columns) if columns is not None else "*"

        all_batches = list(
            self.__get_batch_files(
                obj_type=obj_type,
                start_date=start_date,
                end_date=end_date,
                batch_sz=batch_size,
                parts=parts,
                resume_from=resume_from,
            )
        )

        where_sel = (
            f" WHERE {where_clause.strip().replace('WHERE', '')}"
            if where_clause is not None
            else ""
        )
        limit_sel = f" LIMIT {limit}" if limit is not None else ""

        def _run():
            for fb in all_batches:

                if self.__progress is not None and task is not None:
                    file_names = [fb[0].split("/")[-1], fb[-1].split("/")[-1]]
                    self.__progress.update(
                        task, description=f"{obj_type} - {'-'.join(file_names)}"
                    )

                s3_urls = [f"s3://openalex/{f}" for f in fb]
                rel = self.__conn.sql(
                    f"""
                                      SELECT {cols}
                                      FROM read_json_auto({s3_urls}, ignore_errors=true)
                                      {where_sel}{limit_sel}
                                      """
                )

                yield fb, rel

                if self.__progress is not None and task is not None:
                    self.__progress.update(task, advance=1)

        if self.__progress is not None:
            with self.__progress:
                task = self.__progress.add_task(f"{obj_type}", total=len(all_batches))
                yield from _run()

        else:
            task = None
            yield from _run()
