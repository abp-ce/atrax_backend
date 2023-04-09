import pandas as pd
from sqlalchemy import func, select

from .. import crud
from ..models import Phone
from ..service import Parse
from .conftest import async_session_test, engine_test


async def get_count(model) -> int:
    async with async_session_test() as session:
        result = await session.scalars(select(func.count()).select_from(model))
        return result.first()


async def insert_data(data: pd.DataFrame):
    parse = Parse(engine_test)
    async with engine_test.connect() as parse.conn:
        await parse._process_chunk(data)
        await parse.conn.commit()


async def test_parse(test_data: pd.DataFrame):
    row, _ = test_data.shape
    await insert_data(test_data)
    inserted = await get_count(Phone)
    assert inserted == row, f"Ошибка: из {row} записей добавлено {inserted}."
    async with async_session_test() as session:
        result = await crud.get_info(session, 3832857880)
    assert result == (
        5902202276,
        'АО "ЭР-Телеком Холдинг"',
        "Новосибирская обл.",
        "г. Новосибирск",
    )


async def test_divided_data_insertion(divided_test_data: pd.DataFrame):
    existed = await get_count(Phone)
    await insert_data(divided_test_data)
    existing = await get_count(Phone)
    assert existed == existing, "Ошибка: плохие данные добавились."
    async with async_session_test() as session:
        result = await crud.get_info(session, 9000032010)
    assert result[2] != "Республика Татарстан"


async def test_united_data_insertion(united_test_data: pd.DataFrame):
    existed = await get_count(Phone)
    await insert_data(united_test_data)
    existing = await get_count(Phone)
    assert existed == existing, "Ошибка: плохие данные добавились."
    async with async_session_test() as session:
        result = await crud.get_info(session, 4963477626)
    assert result[1] == 'ООО "Наука-Связь"'
    assert result[1] != 'ООО "ВВК-Телеком"'


async def test_range_removing():
    before = await get_count(Phone)
    parse = Parse(engine_test)
    async with engine_test.connect() as parse.conn:
        await parse._delete_file_data(Parse.REMOTE_URLS[1])
        await parse.conn.commit()
    after = await get_count(Phone)
    assert (before - after) == 3, f"Ошибка: было {before}, стало {after}."


# Redis stuff
async def test_etags():
    Parse.clear_redis(1)
    test_etags = []
    for file_name in Parse.REMOTE_URLS:
        Parse.set_redis_etag(file_name, Parse.get_file_etag(file_name), 1)
        test_etags.append(
            Parse.get_redis_etag(
                file_name,
                1,
            )
        )
    assert len(test_etags) == len(Parse.REMOTE_URLS)
