import datetime
import typing
from datetime import timedelta

from functional import seq
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert

from ..dtos.air_quality import AirQualityDTO
from ..entities.air_quality import AirQuality, AirQualityMeasureCenter


class AirQualityRepository:
    def __init__(self, engine):
        self.engine = engine

    def insert_air_quality(
        self,
        air_quality_dto_list: typing.List[AirQualityDTO],
        air_quality_measure_center_list: typing.List[AirQualityMeasureCenter],
    ):
        _stmt = insert(AirQuality)
        stmt = _stmt.on_duplicate_key_update(
            id=_stmt.inserted.id,
            measure_datetime=_stmt.inserted.measure_datetime,
            location=_stmt.inserted.location,
            no2=_stmt.inserted.no2,
            o3=_stmt.inserted.o3,
            co=_stmt.inserted.co,
            so2=_stmt.inserted.so2,
            pm10=_stmt.inserted.pm10,
            pm25=_stmt.inserted.pm25,
        )
        measure_center_dict = {
            center.location: center for center in air_quality_measure_center_list
        }
        data_list = seq(air_quality_dto_list).map(
            lambda dto: {
                "measure_datetime": dto.measure_datetime_str,
                "measure_center_id": measure_center_dict[dto.location].id,
                "no2": dto.no2,
                "o3": dto.o3,
                "co": dto.co,
                "so2": dto.so2,
                "pm10": dto.pm10,
                "pm25": dto.pm25,
            }
        )
        with self.engine.connect() as conn:
            conn.execute(stmt, data_list)

    def get_by_measure_date(
        self, measure_date: datetime.date
    ) -> typing.List[AirQuality]:
        today, tomorrow = measure_date, measure_date + timedelta(days=1)
        stmt = (
            select(AirQuality)
            .where(
                AirQuality.measure_datetime >= today,
                AirQuality.measure_datetime < tomorrow,
            )
            .all()
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
        return result.fetchall()

    def list_measure_center(self) -> typing.List[AirQualityMeasureCenter]:
        stmt = select(AirQualityMeasureCenter).all()
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
        return result.fetchall()
