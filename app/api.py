from datetime import datetime
from enum import StrEnum
from typing import Annotated
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import select
from app.database import source_engine
from app.models import SourceData


app = FastAPI(
    title="Delfos Source Data API",
    version="1.0.0",
)


class VariableName(StrEnum):
    WIND_SPEED = "wind_speed"
    POWER = "power"
    AMBIENT_TEMPRATURE = "ambient_temprature"


@app.get("/data")
def get_data(
    start: datetime,
    end: datetime,
    variables: Annotated[list[VariableName], Query()],
) -> list[dict]:
    if start >= end:
        raise HTTPException(
            status_code=400,  # Bad Request.
            detail="The 'start' parameter must be earlier than the 'end' parameter.",
        )
    selected_columns = [SourceData.timestamp]
    for var in variables:
        selected_columns.append(getattr(SourceData, var.value))
    with source_engine.connect() as connection:
        query = (
            select(*selected_columns)
            .where(
                SourceData.timestamp >= start,
                SourceData.timestamp < end,
            ).order_by(SourceData.timestamp)
        )
        result = connection.execute(query)
        return [dict(row) for row in result.mappings().all()]