from datetime import date
from dagster import Config, Definitions, OpExecutionContext, job, op
from pydantic import PrivateAttr
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from app.etl import run_etl
import dagster as dg
import httpx
from app.config import (
    SOURCE_API_URL,
    SOURCE_DATA_START,
    TARGET_DATABASE_URL,
)


class SourceApiResource(dg.ConfigurableResource):
    api_url: str
    timeout : float = 30.0

    _client: httpx.Client | None = PrivateAttr(default=None)

    def setup_for_execution(
        self,
        context: dg.InitResourceContext,
    ) -> None:
        self._client = httpx.Client(timeout=self.timeout)

    def teardown_after_execution(
        self,
        context: dg.InitResourceContext,
    ) -> None:
        if self._client is not None:
            self._client.close()

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("Source API cliente was not initialized first")
        return self._client


class TargetDatabaseResource(dg.ConfigurableResource):
    database_url: str

    _engine: Engine | None = PrivateAttr(default=None)

    def setup_for_execution(
        self,
        context: dg.InitResourceContext,
    ) -> None:
        self._engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
        )

    def teardown_after_execution(
        self,
        context: dg.InitResourceContext,
    ) -> None:
        if self._engine is not None:
            self._engine.dispose()

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("Target database engine was not first initialized")
        return self._engine


daily_partitions = dg.DailyPartitionsDefinition(
    start_date=SOURCE_DATA_START.date().isoformat(),
    timezone="UTC",
)


@dg.asset(partitions_def=daily_partitions)
def daily_etl_asset(
    context: dg.AssetExecutionContext,
    source_api: SourceApiResource,
    target_database: TargetDatabaseResource,
) -> None:
    process_date = date.fromisoformat(context.partition_key)
    context.log.info(f"Starting ETL for partition {process_date}")
    run_etl(
        process_date,
        source_client=source_api.client,
        source_api_url=source_api.api_url,
        target_engine=target_database.engine,
    )
    context.log.info(f"Completed ETL for partition {process_date}")


daily_etl_job = dg.define_asset_job(
    name="daily_etl_job",
    selection=[daily_etl_asset],
)


daily_etl_schedule = dg.build_schedule_from_partitioned_job(
    daily_etl_job,
    hour_of_day=1,
    minute_of_hour=0,
)


defs = dg.Definitions(
    assets=[daily_etl_asset],
    jobs=[daily_etl_job],
    schedules=[daily_etl_schedule],
    resources={
        "source_api": SourceApiResource(
            api_url=SOURCE_API_URL,
        ),
        "target_database": TargetDatabaseResource(
            database_url=TARGET_DATABASE_URL,
        )
    }
)