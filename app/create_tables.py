from app.database import source_engine, target_engine
from app.models import SourceBase, TargetBase


def create_tables() -> None:
    SourceBase.metadata.create_all(source_engine)
    TargetBase.metadata.create_all(target_engine)
    print("Tables created successfully in both source and target databases.")


if __name__ == "__main__":
    create_tables()