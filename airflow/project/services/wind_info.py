import typing
from datetime import datetime
from json import JSONDecodeError

import httpx
import project.util as utils
from functional import seq
from project.dtos.wind_info import WindInfoDTO
from project.repositories.wind_info import WindInfoRepository
from project.util import get_secret_data

DATE_FORMAT = "%Y%m%d"

API_KEY = get_secret_data()["asos_api_key"]
API_ROOT = (
    f"http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?"
    f"serviceKey={API_KEY}&dataType=JSON&dataCd=ASOS&dateCd=HR&pageNo=1&numOfRows=999"
)


class LimitExceededError(Exception):
    pass


def get_api_url(target_datetime: datetime, station_id: int) -> str:
    start_dt_str = end_dt_str = target_datetime.strftime(DATE_FORMAT)
    start_hh, end_hh = "00", "23"

    query_string = (
        f"startDt={start_dt_str}&startHh={start_hh}&endDt={end_dt_str}&endHh={end_hh}"
    )

    url = f"{API_ROOT}&{query_string}&stnIds={station_id}"
    return url


def request_data(url) -> typing.List[dict]:
    with httpx.Client() as client:
        r = client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"{r.status_code} {r.text}")
    try:
        j = r.json()
    except JSONDecodeError as e:
        response_text = r.text
        if "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in response_text:
            raise LimitExceededError from e
        raise e

    try:
        data_list: typing.List[dict] = j["response"]["body"]["items"]["item"]
    except KeyError as e:
        if j["response"]["header"]["resultMsg"] == "NO_DATA":
            data_list = []
        else:
            raise e
    return data_list


class WindInfoService:
    def __init__(self, repository: WindInfoRepository):
        self.repository = repository

    def get_measure_center_id_list(self):
        return self.repository.get_measure_center_id_list()

    def get_wind_info_dto_list(
        self, measure_center_id_list: typing.List[int], target_datetime: datetime
    ):
        dto_list = (
            seq(measure_center_id_list)
            .map(lambda center_id: get_api_url(target_datetime, center_id))
            .flat_map(request_data)
            .map(utils.convert_empty_string_value_to_null)
            .map(lambda d: WindInfoDTO(**d))
            .to_list()
        )
        return dto_list

    def insert_wind_info(self, wind_info_dto_list: typing.List[WindInfoDTO]):
        self.repository.insert_wind_info(wind_info_dto_list)
