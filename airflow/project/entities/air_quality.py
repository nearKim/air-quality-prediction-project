from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

from ..constant import MysqlGeometry

Base = declarative_base()

metadata = MetaData()


class AirQuality(Base):
    __tablename__ = "air_quality"

    id = Column("id", BigInteger, primary_key=True)
    measure_datetime = Column("measure_datetime", TIMESTAMP)
    measure_center_id = Column(BigInteger, ForeignKey("air_quality_measure_center.id"))
    no2 = Column("no2", Float)
    o3 = Column("o3", Float)
    co = Column("co", Float)
    so2 = Column("so2", Float)
    pm10 = Column("pm10", Float)
    pm25 = Column("pm25", Float)


class AirQualityMeasureCenter(Base):
    __tablename__ = "air_quality_measure_center"

    id = Column("id", BigInteger, primary_key=True)
    address = Column("address", String(255))
    location = Column("location", String(255))
    official_code = Column("official_code", Integer, unique=True)
    latitude = Column("latitude", Float)
    coordinate = Column("coordinate", MysqlGeometry("POINT"))

    @property
    def shape(self):
        from geoalchemy2.shape import to_shape

        return to_shape(self.coordinate)

    @property
    def latitude(self) -> float:
        return self.shape.x

    @property
    def longitude(self) -> float:
        return self.shape.y
