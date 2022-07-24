import json
import typing
from datetime import datetime

import httpx

from airflow_project.dtos.air_quality import AirQualityDTO
from airflow_project.entities.air_quality import AirQuality, AirQualityMeasureCenter
from airflow_project.repositories.air_quality import AirQualityRepository
from airflow_project.util import get_secret_data

API_KEY = get_secret_data()["open_api_key"]
API_ROOT = f"http://openAPI.seoul.go.kr:8088/{API_KEY}/json/TimeAverageAirQuality"


def get_api_url(target_date_str: str, start_idx: int, end_idx: int) -> str:
    url = f"{API_ROOT}/{start_idx}/{end_idx}/{target_date_str}"
    return url


class AirQualityService:
    DATE_FORMAT = "%Y%m%d"

    def __init__(self, repository: AirQualityRepository):
        self.repository = repository

    def get_api_result_count(self, target_date: datetime.date) -> int:
        target_date_str: str = target_date.strftime(self.DATE_FORMAT)
        base_url: str = get_api_url(target_date_str, 1, 1)

        with httpx.Client() as client:
            r = client.get(base_url)
        if r.status_code != 200:
            raise RuntimeError(f"{r.status_code} {r.text}")

        raw_data = json.loads(r.text)
        total_count = raw_data["TimeAverageAirQuality"]["list_total_count"]
        return total_count

    def get_air_quality_dto_list(
        self, target_date: datetime.date, start_idx: int, end_idx: int
    ):
        target_date_str: str = target_date.strftime(self.DATE_FORMAT)
        url = get_api_url(target_date_str, start_idx, end_idx)

        with httpx.Client() as client:
            r = client.get(url)

        if r.status_code != 200:
            raise RuntimeError(f"{r.status_code} {r.text}")

        j = r.json()

        data_list = j["TimeAverageAirQuality"]["row"]
        return [AirQualityDTO(**d) for d in data_list]

    def get_air_quality_list(
        self, target_date: datetime.date
    ) -> typing.List[AirQuality]:
        orm_list = self.repository.get_by_measure_date(target_date)
        return orm_list

    def get_air_quality_measure_center_list(
        self,
    ) -> typing.List[AirQualityMeasureCenter]:
        orm_list = self.repository.list_measure_center()
        return orm_list

    def insert_air_quality(
        self,
        air_quality_dto_list: typing.List[AirQualityDTO],
        air_quality_measure_center_list: typing.List[AirQualityMeasureCenter],
    ):
        self.repository.insert_air_quality(
            air_quality_dto_list, air_quality_measure_center_list
        )
