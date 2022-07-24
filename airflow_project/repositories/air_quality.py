import datetime
import typing
from datetime import timedelta

from sqlalchemy import select

from airflow_project.entities.air_quality import AirQuality, AirQualityMeasureCenter


class AirQualityRepository:
    def __init__(self, engine):
        self.engine = engine

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
