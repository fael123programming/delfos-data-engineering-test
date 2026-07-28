import argparse
from datetime import date, datetime, time, timedelta, timezone
import httpx
import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from app.models import Signal, TargetData
from app.database import target_engine as default_target_engine
from app.config import SOURCE_API_URL


VARIABLES = ("wind_speed", "power")
AGGREGATIONS = ("mean", "min", "max", "std")


def extract_data(
    process_date: date,
    *,
    source_client: httpx.Client,
    source_api_url: str,
) -> pd.DataFrame:
    start = datetime.combine(process_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    params = [
        ("start", start.isoformat()),
        ("end", end.isoformat()),
        ("variables", "wind_speed"),
        ("variables", "power"),
    ]
    response = source_client.get(
        f"{source_api_url}/data",
        params=params,
    )
    response.raise_for_status()
    data = response.json()
    if not data:
        raise ValueError(f"No data found for date {process_date}")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def transform_data(source_df: pd.DataFrame) -> pd.DataFrame:
    aggregated = (
        source_df
        .set_index("timestamp")[list(VARIABLES)]
        .resample("10min")
        .agg(list(AGGREGATIONS))
    )
    transformed_rows = list()
    for row in aggregated.iterrows():
        timestamp, values = row
        for variable in VARIABLES:
            for aggregation in AGGREGATIONS:
                transformed_rows.append({
                    "timestamp": timestamp,
                    "signal_name": f"{variable}.{aggregation}",
                    "value": values[variable][aggregation]
                })
    return pd.DataFrame(transformed_rows)


def load_data(
    process_date: date,
    transformed_df: pd.DataFrame,
    *,
    target_engine: Engine,
) -> None:
    with Session(target_engine) as session:
        try:
            unique_signals = transformed_df["signal_name"].unique()
            start = datetime.combine(process_date, time.min, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            select_stmt = select(Signal).where(Signal.name.in_(unique_signals))
            existing_signal_objects = session.scalars(select_stmt).all()
            signal_mapping = {sig.name: sig.id for sig in existing_signal_objects}
            existing_names = set(signal_mapping.keys())
            signals_to_create = set(unique_signals) - existing_names
            if signals_to_create:
                new_signal_objects = [Signal(name=name) for name in signals_to_create]
                session.add_all(new_signal_objects)
                session.flush()
                for sig in new_signal_objects:
                    signal_mapping[sig.name] = sig.id
            delete_stmt = delete(TargetData).where(
                TargetData.timestamp >= start,
                TargetData.timestamp < end,
            )
            session.execute(delete_stmt)
            new_target_data_objects = list()
            for _, row in transformed_df.iterrows():
                signal_id = signal_mapping[row["signal_name"]]
                target_data_obj = TargetData(
                    timestamp=row["timestamp"],
                    signal_id=signal_id,
                    value=row["value"]
                )
                new_target_data_objects.append(target_data_obj)
            session.add_all(new_target_data_objects)
            session.commit()
        except Exception:
            session.rollback()
            raise


def run_etl(
    process_date: date,
    *,
    source_client: httpx.Client,
    source_api_url: str,
    target_engine: Engine,
) -> None:
    print("Starting ETL process for date:", process_date)
    source_df = extract_data(
        process_date,
        source_client=source_client,
        source_api_url=source_api_url,
    )
    print(f"Extracted {len(source_df)} row(s)")
    transformed_df = transform_data(source_df)
    print(f"Transformed {len(transformed_df)} row(s)")
    load_data(
        process_date,
        transformed_df,
        target_engine=target_engine
    )
    print(f"Loaded {len(transformed_df)} row(s) of data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=date.fromisoformat)
    args = parser.parse_args()
    with httpx.Client(timeout=30.0) as source_client:
        run_etl(
            process_date=args.date,
            source_client=source_client,
            source_api_url=SOURCE_API_URL,
            target_engine=default_target_engine,
        )