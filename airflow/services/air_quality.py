import json
import typing
from datetime import datetime
from functools import lru_cache

import httpx
from functional import seq

from airflow.dtos.air_quality import AirQualityDTO, AirQualityIntegratedDTO
from airflow.entities.air_quality import AirQuality, AirQualityMeasureCenter
from airflow.repositories.air_quality import AirQualityRepository
from airflow.util import get_secret_data

API_KEY = get_secret_data()["open_api_key"]
API_ROOT = f"http://openAPI.seoul.go.kr:8088/{API_KEY}/json/TimeAverageAirQuality"


def get_api_url(target_date_str: str, start_idx: int, end_idx: int) -> str:
    url = f"{API_ROOT}/{start_idx}/{end_idx}/{target_date_str}"
    return url


class AirQualityService:
    DATE_FORMAT = "%Y%m%d"

    def __init__(self, repository: AirQualityRepository):
        self.repository = repository

    @lru_cache(maxsize=365)
    async def get_api_result_count(self, target_date: datetime.date):
        target_date_str: str = target_date.strftime(self.DATE_FORMAT)
        base_url: str = get_api_url(target_date_str, 1, 1)

        async with httpx.AsyncClient() as client:
            r = await client.get(base_url)

        raw_data = json.loads(r.text)
        total_count = raw_data["TimeAverageAirQuality"]["list_total_count"]
        return total_count

    async def get_air_quality_dto_list(
        self, target_date: datetime.date, start_idx: int, end_idx: int
    ):
        target_date_str: str = target_date.strftime(self.DATE_FORMAT)
        url = get_api_url(target_date_str, start_idx, end_idx)

        async with httpx.AsyncClient() as client:
            r = await client.get(url)
            if r.status_code != 200:
                raise RuntimeError(f"{r.status_code} {r.text}")
            j = r.json()

        data_list = j["TimeAverageAirQuality"]["row"]
        return [AirQualityDTO(**d) for d in data_list]

    async def get_air_quality_list(
        self, target_date: datetime.date
    ) -> typing.List[AirQuality]:
        orm_list = await self.repository.get_by_measure_date(target_date)
        return orm_list

    async def get_air_quality_measure_center_list(
        self,
    ) -> typing.List[AirQualityMeasureCenter]:
        orm_list = await self.repository.list_measure_center()
        return orm_list

    def get_integrated_air_quality_list(
        self, air_quality_orm_list: typing.List[AirQuality], measure_center_orm_list
    ) -> typing.List[AirQualityIntegratedDTO]:
        measure_center_dict = {
            center.location: center for center in measure_center_orm_list
        }
        result = (
            seq(air_quality_orm_list)
            .map(
                lambda a: AirQualityIntegratedDTO(
                    **dict(a),
                    measure_center_address=measure_center_dict[a.location].address,
                    measure_center_official_code=measure_center_dict[
                        a.location
                    ].official_code,
                    measure_center_latitude=measure_center_dict[a.location].latitude,
                    measure_center_longitude=measure_center_dict[a.location].longitude,
                ),
            )
            .to_list()
        )
        return result
