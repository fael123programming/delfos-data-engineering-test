from typing import List
from datetime import datetime
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class SourceBase(DeclarativeBase):
    pass


class TargetBase(DeclarativeBase):
    pass


class SourceData(SourceBase):
    __tablename__ = "data"

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    wind_speed: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    power: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    ambient_temprature: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )


class Signal(TargetBase):
    __tablename__ = "signal"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
    )
    data: Mapped[List["TargetData"]] = relationship(
        "TargetData",
        back_populates="signal",
        cascade="all, delete-orphan",
    )


class TargetData(TargetBase):
    __tablename__ = "data"

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    signal_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("signal.id"),
        primary_key=True,
    )
    value: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    signal: Mapped["Signal"] = relationship(
        back_populates="data"
    )