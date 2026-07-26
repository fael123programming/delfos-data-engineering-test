from sqlalchemy import create_engine
from app.config import SOURCE_DATABASE_URL, TARGET_DATABASE_URL


source_engine = create_engine(
    SOURCE_DATABASE_URL,
    pool_pre_ping=True,
)

target_engine = create_engine(
    TARGET_DATABASE_URL,
    pool_pre_ping=True,
)