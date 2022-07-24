import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

import airflow_project.util as utils
from airflow_project.constant import KST
from airflow_project.dtos.wind_info import WindInfoDTO
from airflow_project.infra.db import engine
from airflow_project.repositories.wind_info import WindInfoRepository
from airflow_project.services.wind_info import WindInfoService


@task()
def get_measure_center_id_list(service: WindInfoService) -> typing.List[int]:
    return service.get_measure_center_id_list()


@task(task_concurrency=1)
def insert_data_to_db(
    center_id_list: typing.List[int], service: WindInfoService, **context
) -> None:
    dtz = utils.convert_utc_to_kst_datetime(context["ds"], "%Y-%m-%d")
    wind_info_dto_list: typing.List[WindInfoDTO] = service.get_wind_info_dto_list(
        center_id_list, dtz
    )
    service.insert_wind_info(wind_info_dto_list)


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "insert_wind_info_data_to_db",
    default_args=default_args,
    description="기상청 ASOS API의 풍향, 풍속 자료를 DB에 업데이트합니다.",
    schedule_interval="@daily",
    start_date=datetime(2018, 1, 1, tzinfo=KST),
    catchup=True,
    max_active_runs=10,
    tags=["wind_info", "DB"],
) as dag:
    repository = WindInfoRepository(engine)
    service = WindInfoService(repository)

    start = DummyOperator(task_id="start")

    with TaskGroup("update_db") as tg:
        measure_center_id_list = get_measure_center_id_list()
        dumb_result = insert_data_to_db(measure_center_id_list)

    end = DummyOperator(task_id="end")

    start >> tg >> end
