from enum import Enum

import pendulum
from geoalchemy2 import Geometry

KST = pendulum.timezone("Asia/Seoul")


class MysqlGeometry(Geometry):
    as_binary = "ST_AsWKB"


class QualityEnum(str, Enum):
    정상 = "NORMAL"
    오류 = "ERROR"
    결측 = "NULL"
