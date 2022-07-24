import typing_extensions
from airflow.util import get_secret_data
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

if typing_extensions.TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def get_async_engine():
    secret_data = get_secret_data()
    username = secret_data["username"]
    password = secret_data["password"]

    return create_async_engine(
        f"mysql+asyncmy://{username}:{password}@mysql:3306/air_pollution",
        echo_pool=True,
        echo=True,
        isolation_level="AUTOCOMMIT",
    )


def get_sync_engine():
    secret_data = get_secret_data()
    username = secret_data["username"]
    password = secret_data["password"]

    return create_engine(
        f"mysql+pymysql://{username}:{password}@mysql:3306/air_pollution",
        echo_pool=True,
        echo=True,
        isolation_level="AUTOCOMMIT",
    )


async_engine: "AsyncEngine" = get_async_engine()
engine: "Engine" = get_sync_engine()
