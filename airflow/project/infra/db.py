from project.util import get_secret_data
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


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


engine: "Engine" = get_sync_engine()
