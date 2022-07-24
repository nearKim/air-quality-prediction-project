import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import airflow_project.util as utils
from airflow_project.dtos.air_quality import AirQualityDTO
from airflow_project.infra.db import engine
from airflow_project.repositories.air_quality import AirQualityRepository
from airflow_project.services.air_quality import AirQualityService


def get_api_result_count(
    datetime_str: str, service: AirQualityService, **context
) -> int:
    dtz = utils.convert_utc_to_kst_datetime(datetime_str, "%Y-%m-%d")
    cnt = service.get_api_result_count(dtz)
    return cnt


def insert_data_to_db(datetime_str: str, service: AirQualityService, **context):
    dtz = utils.convert_utc_to_kst_datetime(datetime_str, "%Y-%m-%d")
    cnt = context["task_instance"].xcom_pull(task_ids="get_api_result_count")
    dto_list: typing.List[AirQualityDTO] = service.get_air_quality_dto_list(dtz, 1, cnt)

    service.insert_air_quality(dto_list)
    # _stmt = insert(AirQuality)
    # stmt = _stmt.on_duplicate_key_update(
    #     id=_stmt.inserted.id,
    #     measure_datetime=_stmt.inserted.measure_datetime,
    #     location=_stmt.inserted.location,
    #     no2=_stmt.inserted.no2,
    #     o3=_stmt.inserted.o3,
    #     co=_stmt.inserted.co,
    #     so2=_stmt.inserted.so2,
    #     pm10=_stmt.inserted.pm10,
    #     pm25=_stmt.inserted.pm25,
    # )
    #
    # with engine.connect() as conn:
    #     conn.execute(stmt, dict_list)


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "insert_data_to_db",
    default_args=default_args,
    description="서울시 대기환경 API의 리스폰스를 DB에 업데이트합니다.",
    schedule_interval="@daily",
    start_date=datetime(2018, 1, 1, tzinfo=None),
    catchup=True,
    max_active_runs=5,
    tags=["air_quality", "DB"],
) as dag:
    repository = AirQualityRepository(engine)
    service = AirQualityService(repository)

    t1 = PythonOperator(
        task_id="get_api_result_count",
        python_callable=get_api_result_count,
        op_kwargs={"datetime_str": "{{ ds }}", "service": service},
    )

    t2 = PythonOperator(
        task_id="insert_data_to_db",
        python_callable=insert_data_to_db,
        op_kwargs={"datetime_str": "{{ ds }}"},
        retry_delay=timedelta(days=1),
    )

    t1 >> t2
