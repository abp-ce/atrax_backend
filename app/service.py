import asyncio
import logging
import ssl

import httpx
import numpy as np
import pandas as pd
import redis
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from . import crud
from .config import settings

CHUNK_SIZE = 1000
PREFIX_MP = 10000000  # multiplier

logging.basicConfig(level=logging.INFO)


class Parse:
    REMOTE_URLS = [
        "https://opendata.digital.gov.ru/downloads/ABC-3xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-4xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-8xx.csv",
        "https://opendata.digital.gov.ru/downloads/DEF-9xx.csv",
    ]

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.conn = None

    @staticmethod
    def filter_remote_urls(db: int = 0) -> list:
        r = redis.StrictRedis(
            host=settings.redis_host,
            encoding="utf-8",
            decode_responses=True,
            db=db,
        )
        remote_urls = []
        with httpx.Client(verify=False) as client:
            for url in Parse.REMOTE_URLS:
                response = client.head(url)
                if response.headers["ETag"] != r.get(url):
                    r.set(url, response.headers["ETag"])
                    r.persist(url)
                    remote_urls.append(url)
        return remote_urls

    @staticmethod
    def clear_redis(db: int = 0) -> list:
        r = redis.StrictRedis(
            host=settings.redis_host,
            encoding="utf-8",
            decode_responses=True,
            db=db,
        )
        for url in Parse.REMOTE_URLS:
            r.delete(url)

    def __get_operator_values(self, chunk: pd.DataFrame) -> list[dict]:
        chunk["ИНН"] = chunk["ИНН"].replace(np.nan, None)
        values_set = set(
            [(x, y) for x, y in zip(chunk["ИНН"], chunk["Оператор"])]
        )
        return [{"inn": x, "name": y} for x, y in values_set]

    def __get_region_values(self, chunk: pd.DataFrame) -> list[dict]:
        values_set = set(
            [
                tuple(x)
                for x in map(lambda item: item.split("|"), chunk["Регион"])
            ]
        )
        return [
            {"name": x[0], "sub_name": ""}
            if len(x) < 2
            else {"name": x[1], "sub_name": x[0]}
            for x in values_set
        ]

    def __get_phone_values(self, chunk: pd.DataFrame) -> list[dict]:
        return [
            {
                "range": Range(
                    (pm := int(prefix * PREFIX_MP)) + int(start),
                    pm + int(end),
                    bounds="[]",
                ),
                "reg_name": reg_splitted[1]
                if len(reg_splitted := region.split("|")) > 1
                else reg_splitted[0],
                "reg_sub_name": reg_splitted[0]
                if len(reg_splitted) > 1
                else "",
                "operator_inn": inn,
                "operator_name": name,
            }
            for prefix, start, end, region, inn, name in zip(
                chunk["АВС/ DEF"],
                chunk["От"],
                chunk["До"],
                chunk["Регион"],
                chunk["ИНН"],
                chunk["Оператор"],
            )
        ]

    async def _delete_file_data(self, file_name: str):
        ranges = [
            Range(3000000000, 4000000000),
            Range(4000000000, 5000000000),
            Range(8000000000, 9000000000),
            Range(9000000000, 10000000000),
        ]
        nums_ranges = dict(zip(self.REMOTE_URLS, ranges))
        await crud.delete_range(self.conn, nums_ranges[file_name])

    async def _process_chunk(self, chunk: pd.DataFrame):
        await crud.upsert_operators(
            self.conn, self.__get_operator_values(chunk)
        )
        await crud.upsert_regions(self.conn, self.__get_region_values(chunk))
        await crud.upsert_phones(self.conn, self.__get_phone_values(chunk))

    async def parse_csv(self, file_name: str) -> None:
        ssl._create_default_https_context = ssl._create_unverified_context
        async with self.engine.connect() as self.conn:
            try:
                await self._delete_file_data(file_name)
                for chunk in pd.read_csv(
                    file_name,
                    sep=";",
                    chunksize=CHUNK_SIZE,
                    on_bad_lines="skip",
                ):
                    await self._process_chunk(chunk)
                await self.conn.commit()
            except DBAPIError as err:
                logging.warning(f"Ошибка в обработке файла: {err}")
                await self.conn.rollback()

    async def parse_all_csv(self, is_filtered: bool = False) -> None:
        files_to_parse = (
            self.filter_remote_urls() if is_filtered else self.REMOTE_URLS
        )
        tasks = [self.parse_csv(file_name) for file_name in files_to_parse]
        await asyncio.gather(*tasks)
