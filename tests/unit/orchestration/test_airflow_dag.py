import importlib.util
import os
import unittest
from datetime import timedelta
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Airflow is installed only in .venv-airflow")
class AirflowDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
        dag_path = Path(__file__).resolve().parents[3] / "dags" / "nasa_eo_batch_vertical_slice.py"
        spec = importlib.util.spec_from_file_location("phase7_dag", dag_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        cls.dag = module.dag

    def test_fixed_topology(self):
        order = [
            "initialize_run",
            "nasa_extraction",
            "canonical_transformation",
            "controlled_replay",
            "spark_processing",
            "gold_generation",
            "postgres_load",
            "verification",
            "finalize_run",
        ]
        self.assertEqual(set(self.dag.task_ids), set(order))
        for upstream, downstream in zip(order, order[1:]):
            self.assertEqual(self.dag.get_task(upstream).downstream_task_ids, {downstream})

    def test_schedule_concurrency_and_deadline(self):
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.dagrun_timeout, timedelta(hours=8))

    def test_retry_and_timeout_contract(self):
        expected = {
            "initialize_run": (0, 2),
            "nasa_extraction": (2, 10),
            "canonical_transformation": (1, 15),
            "controlled_replay": (1, 30),
            "spark_processing": (1, 120),
            "gold_generation": (1, 120),
            "postgres_load": (1, 120),
            "verification": (0, 60),
            "finalize_run": (0, 2),
        }
        for task_id, (retries, minutes) in expected.items():
            task = self.dag.get_task(task_id)
            self.assertEqual(task.retries, retries)
            self.assertEqual(task.execution_timeout, timedelta(minutes=minutes))
            self.assertEqual(task.trigger_rule.value, "all_success")
