import asyncio
import logging
import ssl

import pandas as pd
from sqlalchemy.exc import DBAPIError

from . import crud
from .database import async_session, engine

CHUNK_SIZE = 2000

logging.basicConfig(level=logging.INFO)


class Parse:
    REMOTE_URLS = [
        "https://opendata.digital.gov.ru/downloads/ABC-3xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-4xx.csv",
        "https://opendata.digital.gov.ru/downloads/ABC-8xx.csv",
        "https://opendata.digital.gov.ru/downloads/DEF-9xx.csv",
    ]

    def __get_operator_values(self, chunk: pd.DataFrame) -> list[dict]:
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
                "prefix": prefix,
                "start": start,
                "end": stop,
                "name": reg_splitted[1]
                if len(reg_splitted := region.split("|")) > 1
                else reg_splitted[0],
                "sub_name": reg_splitted[0] if len(reg_splitted) > 1 else "",
                "operator_inn": inn,
            }
            for prefix, start, stop, region, inn in zip(
                chunk["АВС/ DEF"],
                chunk["От"],
                chunk["До"],
                chunk["Регион"],
                chunk["ИНН"],
            )
        ]

    async def __process_chunk(self, chunk: pd.DataFrame):
        async with async_session() as session:
            try:
                await crud.upsert_operators(
                    session, self.__get_operator_values(chunk)
                )
                await crud.upsert_regions(
                    session, self.__get_region_values(chunk)
                )
                async with engine.connect() as conn:
                    await crud.upsert_phones(
                        conn, self.__get_phone_values(chunk)
                    )
            except DBAPIError as err:
                logging.warning(f"Ошибка в обработке файла: {err}")

    async def parse_csv(self, file_num: int) -> None:
        max_num = len(self.REMOTE_URLS) - 1
        if file_num > max_num:
            logging.info(f"Допустимые значения от 0 до {max_num}")
            return
        ssl._create_default_https_context = ssl._create_unverified_context
        for chunk in pd.read_csv(
            self.REMOTE_URLS[file_num],
            sep=";",
            chunksize=CHUNK_SIZE,
            on_bad_lines="skip",
        ):
            await self.__process_chunk(chunk)

    async def parse_all_csv(self) -> None:
        tasks = [self.parse_csv(i) for i in range(len(self.REMOTE_URLS))]
        await asyncio.gather(*tasks)
