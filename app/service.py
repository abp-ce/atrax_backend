"""Реализация класса Parse,который парсит и загружает в базу данных
csv файл в формате Реестра российской системы и плана нумерации.
"""
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
    """Парсит и загружает в базу данных csv файл в формате
    Реестра российской системы и плана нумерации.
    """

    REMOTE_URLS = [
        "https://opendata.digital.gov.ru/downloads/ABC-3xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-4xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-8xx.csv",
        "https://opendata.digital.gov.ru/downloads/DEF-9xx.csv",
    ]

    def __init__(self, engine: AsyncEngine) -> None:
        """Метод конструктора

        :param engine: Асинхронный engine базы данных
        :type engine: AsyncEngine
        """
        self.engine = engine
        self.conn = None

    @staticmethod
    def get_file_etag(url: str) -> str:
        """Получает ETag файла по url

        :param url: url файла
        :type url: str
        :return: ETag файла
        :rtype: str
        """
        response = httpx.head(url, verify=False)
        return response.headers["ETag"]

    @staticmethod
    def get_redis_etag(url: str, db: int = 0) -> str:
        """Получает ETag, сохранённый в Redis

        :param url: url-ключ файла
        :type url: str
        :param db: Номер базы в Redis, defaults to 0
        :type db: int, optional
        :return: ETag файла
        :rtype: str
        """
        r = redis.StrictRedis(
            host=settings.redis_host,
            encoding="utf-8",
            decode_responses=True,
            db=db,
        )
        return r.get(url)

    @staticmethod
    def set_redis_etag(url: str, etag: str, db: int = 0) -> None:
        """Сохраняет ETag в Redis

        :param url: url-ключ файла
        :type url: str
        :param etag: ETag для сохранения
        :type etag: str
        :param db: Номер базы в Redis, defaults to 0
        :type db: int, optional
        """
        r = redis.StrictRedis(
            host=settings.redis_host,
            encoding="utf-8",
            decode_responses=True,
            db=db,
        )
        r.set(url, etag)

    @staticmethod
    def clear_redis(db: int = 0) -> None:
        """Чистит ключи url в Redis.

        :param db: Номер базы в Redis, defaults to 0
        :type db: int, optional
        """
        r = redis.StrictRedis(
            host=settings.redis_host,
            encoding="utf-8",
            decode_responses=True,
            db=db,
        )
        for url in Parse.REMOTE_URLS:
            r.delete(url)

    def __get_operator_values(self, chunk: pd.DataFrame) -> list[dict]:
        """Готовит данные для пакетной загрузки в таблицу Operator

        :param chunk: исходные данные
        :type chunk: pd.DataFrame
        :return: данные для загрузки
        :rtype: list[dict]
        """
        chunk["ИНН"] = chunk["ИНН"].replace(np.nan, None)
        values_set = set(
            [(x, y) for x, y in zip(chunk["ИНН"], chunk["Оператор"])]
        )
        return [{"inn": x, "name": y} for x, y in values_set]

    def __get_region_values(self, chunk: pd.DataFrame) -> list[dict]:
        """Готовит данные для пакетной загрузки в таблицу Region

        :param chunk: исходные данные
        :type chunk: pd.DataFrame
        :return: данные для загрузки
        :rtype: list[dict]
        """
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
        """Готовит данные для пакетной загрузки в таблицу Phone

        :param chunk: исходные данные
        :type chunk: pd.DataFrame
        :return: данные для загрузки
        :rtype: list[dict]
        """
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
        """Удаляет в базе данных данные файла, который будет загружаться.

        :param file_name: имя или url файла для загрузки
        :type file_name: str
        """
        ranges = [
            Range(3000000000, 4000000000),
            Range(4000000000, 5000000000),
            Range(8000000000, 9000000000),
            Range(9000000000, 10000000000),
        ]
        nums_ranges = dict(zip(self.REMOTE_URLS, ranges))
        await crud.delete_range(self.conn, nums_ranges[file_name])

    async def _process_chunk(self, chunk: pd.DataFrame):
        """Грузит порцию данных из файла в базу данных

        :param chunk: Порция исходных данных
        :type chunk: pd.DataFrame
        """
        await crud.upsert_operators(
            self.conn, self.__get_operator_values(chunk)
        )
        await crud.upsert_regions(self.conn, self.__get_region_values(chunk))
        await crud.upsert_phones(self.conn, self.__get_phone_values(chunk))

    async def parse_csv(
        self, file_name: str, is_filtered: bool = False
    ) -> None:
        """Парсит и загружает данные из csv файла в базу данных

        :param file_name: имя или url файла для загрузки
        :type file_name: str
        """
        etag = self.get_file_etag(file_name)
        if is_filtered and etag == self.get_redis_etag(file_name):
            return
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
                self.set_redis_etag(file_name, etag)
            except DBAPIError as err:
                logging.warning(f"Ошибка в обработке файла: {err}")
                await self.conn.rollback()
