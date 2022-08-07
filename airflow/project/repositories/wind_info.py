import typing

from project.dtos.wind_info import WindInfoDTO
from project.entities.wind_info import WindInfo, WindInfoMeasureCenter
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine


class WindInfoRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def insert_wind_info(self, wind_info_dto_list: typing.List[WindInfoDTO]):
        _stmt = insert(WindInfo)
        stmt = _stmt.on_duplicate_key_update(
            id=_stmt.inserted.id,
            measure_datetime=_stmt.inserted.measure_datetime,
            station_id=_stmt.inserted.station_id,
            station_name=_stmt.inserted.station_name,
        )
        data_list = [
            {**dto.dict(), "measure_datetime": dto.measure_datetime}
            for dto in wind_info_dto_list
        ]
        with self.engine.connect() as conn:
            conn.execute(stmt, data_list)

    def get_measure_center_id_list(self) -> typing.List[int]:
        stmt = select(WindInfoMeasureCenter.official_code).all()

        with self.engine.connect() as conn:
            res = conn.execute(stmt)
        return [row[0] for row in res.fetchall()]
