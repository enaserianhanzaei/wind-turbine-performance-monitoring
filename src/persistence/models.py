from sqlalchemy import Column, Integer, Float, Boolean, DateTime, UniqueConstraint, String
from persistence.database import Base


class TurbineReading(Base):
    __tablename__ = 'turbine_readings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    turbine_id = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    wind_direction = Column(Float, nullable=False)
    power_output = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint('timestamp', 'turbine_id', name='unique_timestamp_turbine'),
    )


class DailySummary(Base):
    __tablename__ = "daily_summary"
    turbine_id = Column(Integer, primary_key=True)
    date = Column(DateTime, primary_key=True)
    min_power_output = Column(Float, nullable=False)
    max_power_output = Column(Float, nullable=False)
    mean_power_output = Column(Float, nullable=False)


class DailyAnomaly(Base):
    __tablename__ = "daily_anomalies"
    turbine_id = Column(Integer, primary_key=True)
    date = Column(DateTime, primary_key=True)
    is_anomaly = Column(Boolean, nullable=False)
    total_power_output = Column(Float, nullable=False)
    hist_mean_daily_output = Column(Float, nullable=False)
    hist_std_daily_output = Column(Float, nullable=False)
