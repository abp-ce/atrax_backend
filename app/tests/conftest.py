import asyncio

import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from ...main import app
from ..config import settings
from ..dependencies import get_session
from ..models import Base

engine_test = create_async_engine(
    "postgresql+asyncpg://{}:{}@{}:{}/{}".format(
        settings.postgres_user,
        settings.postgres_password,
        settings.postgres_host,
        settings.postgres_port,
        settings.test_postgres_db,
    ),
    echo=True,
    poolclass=NullPool,
)

Base.metadata.bind = engine_test

async_session_test = sessionmaker(
    engine_test, expire_on_commit=False, class_=AsyncSession
)


async def override_get_session() -> AsyncSession:
    async with async_session_test() as session:
        yield session


app.dependency_overrides[get_session] = override_get_session


@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope="session")
async def prepare_database():
    async with engine_test.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine_test.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def test_data() -> pd.DataFrame:
    return pd.read_csv("./app/tests/data/test_data.csv", sep=";")


@pytest.fixture
def divided_test_data() -> pd.DataFrame:
    return pd.read_csv("./app/tests/data/divided_test_data.csv", sep=";")


@pytest.fixture
def united_test_data() -> pd.DataFrame:
    return pd.read_csv("./app/tests/data/united_test_data.csv", sep=";")
