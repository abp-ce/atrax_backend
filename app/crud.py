from sqlalchemy import bindparam, delete, select
from sqlalchemy.dialects.postgresql import Range, insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from .models import Operator, Phone, Region


async def get_info(session: AsyncSession, phone_num: int):
    result = await session.execute(
        select(Operator.inn, Operator.name, Region.name, Region.sub_name)
        .select_from(Phone)
        .join(Region)
        .join(Operator)
        .where(Phone.range.contains(phone_num))
    )
    return result.first()


async def upsert_operators(conn: AsyncConnection, values: list[dict]):
    stmt = insert(Operator)
    stmt = stmt.on_conflict_do_nothing()
    await conn.execute(stmt, values)


async def upsert_regions(conn: AsyncConnection, values: list[dict]):
    stmt = insert(Region)
    stmt = stmt.on_conflict_do_nothing()
    await conn.execute(stmt, values)


async def upsert_phones(conn: AsyncConnection, values: list[dict]):
    sel_stmt_reg = (
        select(
            Region.id,
        )
        .where(
            Region.name == bindparam("reg_name"),
            Region.sub_name == bindparam("reg_sub_name"),
        )
        .scalar_subquery()
    )
    sel_stmt_oper = (
        select(
            Operator.id,
        )
        .where(
            Operator.inn == bindparam("operator_inn"),
            Operator.name == bindparam("operator_name"),
        )
        .scalar_subquery()
    )
    stmt = insert(Phone).values(
        region_id=sel_stmt_reg, operator_id=sel_stmt_oper
    )
    stmt = stmt.on_conflict_do_nothing()
    await conn.execute(stmt, values)


async def delete_range(conn: AsyncConnection, range: Range):
    await conn.execute(delete(Phone).where(Phone.range.contained_by(range)))
