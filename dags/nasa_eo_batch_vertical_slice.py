"""Phase 7 Airflow DAG for the proven bounded batch vertical slice."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

from eo_event_platform.common.metadata import detect_pipeline_version
from eo_event_platform.orchestration.runtime import (
    DAG_ID,
    RunParameters,
    execute_stage,
    finalize_run,
    initialize_run,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADATA_ROOT = Path(os.environ.get("ASTRAYAN_AIRFLOW_METADATA_ROOT", PROJECT_ROOT / "data/local/airflow/runs"))
PIPELINE_REVISION = os.environ.get("ASTRAYAN_PIPELINE_REVISION", detect_pipeline_version(PROJECT_ROOT))


def _task_context() -> tuple[dict, RunParameters]:
    airflow_context = get_current_context()
    params = RunParameters.from_mapping(airflow_context["params"])
    return airflow_context, params


@dag(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    params={
        "gate_size": Param(4, type="integer", minimum=1, maximum=1_000_000),
        "source_detection_count": Param(4, type="integer", minimum=1, maximum=10_000),
        "replay_factor": Param(1, type="integer", minimum=1, maximum=100),
        "execution_profile": Param("integration", type="string", enum=["local", "integration"]),
        "source_date": Param("2026-08-01", type="string", format="date"),
    },
    default_args={"retry_delay": timedelta(minutes=1)},
    tags=["astrayan", "batch", "phase-7"],
)
def nasa_eo_batch_vertical_slice():
    @task(task_id="initialize_run", retries=0, execution_timeout=timedelta(minutes=2))
    def initialize():
        airflow_context, params = _task_context()
        logical_date = airflow_context["logical_date"].isoformat()
        return initialize_run(
            metadata_root=METADATA_ROOT,
            airflow_run_id=airflow_context["run_id"],
            logical_date=logical_date,
            params=params,
            pipeline_revision=PIPELINE_REVISION,
        )

    def stage_task(task_id: str, retries: int, timeout_minutes: int):
        @task(task_id=task_id, retries=retries, execution_timeout=timedelta(minutes=timeout_minutes))
        def run(upstream=None):
            context = get_current_context()["ti"].xcom_pull(task_ids="initialize_run")
            return execute_stage(context=context, stage=task_id, upstream=upstream)

        return run

    extraction = stage_task("nasa_extraction", 2, 10)
    canonical = stage_task("canonical_transformation", 1, 15)
    replay = stage_task("controlled_replay", 1, 30)
    spark = stage_task("spark_processing", 1, 120)
    gold = stage_task("gold_generation", 1, 120)
    postgres = stage_task("postgres_load", 1, 120)
    verify = stage_task("verification", 0, 60)

    @task(task_id="finalize_run", retries=0, execution_timeout=timedelta(minutes=2))
    def finalize(verification):
        context = get_current_context()["ti"].xcom_pull(task_ids="initialize_run")
        return finalize_run(context=context, verification=verification)

    run_context = initialize()
    extracted = extraction()
    run_context >> extracted
    canonicalized = canonical(extracted)
    replayed = replay(canonicalized)
    silver = spark(replayed)
    golden = gold(silver)
    loaded = postgres(golden)
    verified = verify(loaded)
    finalize(verified)


dag = nasa_eo_batch_vertical_slice()
