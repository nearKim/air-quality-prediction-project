from datetime import datetime

from pydantic import BaseModel, Field, validator


class AirQualityDTO(BaseModel):
    measure_datetime_str: str = Field(..., alias="MSRDT")
    location: str = Field(..., alias="MSRSTE_NM")
    no2: float
    o3: float
    co: float
    so2: float
    pm10: float
    pm25: float

    @validator("measure_datetime_str")
    def strip_double_zeros(cls, value: str):
        if value.endswith("00"):
            value = value[:-2]
        return value

    class Config:
        @classmethod
        def alias_generator(cls, string: str) -> str:
            # alias가 없는 변수들을 대문자를 사용하여 instantiate 할 수 있게 한다
            return string.upper()


class AirQualityIntegratedDTO(BaseModel):
    id: int
    measure_datetime: datetime
    location: str
    no2: float
    o3: float
    co: float
    so2: float
    pm10: float
    pm25: float
    upd_ts: datetime
    reg_ts: datetime
    measure_center_address: str
    measure_center_official_code: int
    measure_center_latitude: float
    measure_center_longitude: float
