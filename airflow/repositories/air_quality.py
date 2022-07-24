import datetime
import typing
from datetime import timedelta

from sqlalchemy import select

from airflow.entities.air_quality import AirQuality, AirQualityMeasureCenter
from airflow.infra.db import async_engine


class AirQualityRepository:
    async def get_by_measure_date(
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
        async with async_engine.connect() as conn:
            result = await conn.execute(stmt)
        return result.fetchall()

    async def list_measure_center(self) -> typing.List[AirQualityMeasureCenter]:
        stmt = select(AirQualityMeasureCenter).all()
        async with async_engine.connect() as conn:
            result = await conn.execute(stmt)
        return result.fetchall()


air_quality_repository = AirQualityRepository()
