from sqlalchemy import bindparam, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from .models import Operator, Phone, Region


async def get_info(session: AsyncSession, prefix: int, middle: int):
    result = await session.execute(
        select(Operator.inn, Operator.name, Region.name, Region.sub_name)
        .select_from(Phone)
        .join(Region)
        .join(Operator)
        .where(
            Phone.prefix == prefix, Phone.start <= middle, Phone.end >= middle
        )
    )
    return result.first()


async def upsert_operators(session: AsyncSession, values: list[dict]):
    stmt = insert(Operator)
    stmt = stmt.on_conflict_do_update(
        constraint="operator_pkey",
        set_={
            "name": stmt.excluded.name,
        },
    )
    await session.execute(stmt, values)
    await session.commit()


async def upsert_regions(session: AsyncSession, values: list[dict]):
    stmt = insert(Region)
    stmt = stmt.on_conflict_do_nothing()
    await session.execute(stmt, values)
    await session.commit()


async def upsert_phones(conn: AsyncConnection, values: list[dict]):
    sel_stmt = (
        select(
            Region.id,
        )
        .where(
            Region.name == bindparam("name"),
            Region.sub_name == bindparam("sub_name"),
        )
        .scalar_subquery()
    )
    stmt = insert(Phone).values(region_id=sel_stmt)
    stmt = stmt.on_conflict_do_update(
        constraint="phone_pk",
        set_={
            "operator_inn": stmt.excluded.operator_inn,
            "region_id": stmt.excluded.region_id,
        },
    )
    await conn.execute(stmt, values)
    await conn.commit()
