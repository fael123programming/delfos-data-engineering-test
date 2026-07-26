import random
import pandas as pd
from sqlalchemy import delete
from app.config import SOURCE_DATA_START
from app.database import source_engine
from app.models import SourceData


MINUTES_IN_TEN_DAYS = 10 * 24 * 60
RANDOM_SEED = 42


def build_source_dataframe() -> pd.DataFrame:
    timestamps = pd.date_range(
        start=SOURCE_DATA_START,
        periods=MINUTES_IN_TEN_DAYS,
        freq="min",
    )
    rng = random.Random(RANDOM_SEED)
    wind_speeds = list()
    powers = list()
    ambient_tempratures = list()
    for _ in range(len(timestamps)):
        wind_speed = round(rng.uniform(0, 25), 3)
        power = round(rng.uniform(0, 5000), 3)
        ambient_temprature = round(rng.uniform(15, 35), 3)
        wind_speeds.append(wind_speed)
        powers.append(power)
        ambient_tempratures.append(ambient_temprature)
    df_data = {
        "timestamp": timestamps,
        "wind_speed": wind_speeds,
        "power": powers,
        "ambient_temprature": ambient_tempratures,
    }
    return pd.DataFrame(df_data)


def seed_source_data() -> None:
    df = build_source_dataframe()
    with source_engine.begin() as connection:
        connection.execute(delete(SourceData))
        df.to_sql(
            SourceData.__tablename__,
            con=connection,
            if_exists="append",
            index=False,
            chunksize=1000,
        )
        print(
            f"Seeded {len(df)} rows of source data into the database "
            f"from {df['timestamp'].min()} to {df['timestamp'].max()}."
        )


if __name__ == "__main__":
    seed_source_data()