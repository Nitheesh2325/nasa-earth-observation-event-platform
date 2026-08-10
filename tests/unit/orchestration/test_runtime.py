import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.orchestration.runtime import (
    PROCESSING_STAGES,
    RunParameters,
    StageFailure,
    build_run_identity,
    execute_stage,
    finalize_run,
    initialize_run,
)


PARAMS = RunParameters(4, 4, 1, "integration", "2026-08-01")


class RunIdentityTests(unittest.TestCase):
    def test_identity_is_stable_and_parameter_sensitive(self):
        first = build_run_identity(logical_date="2026-08-01T00:00:00+00:00", params=PARAMS)
        second = build_run_identity(logical_date="2026-08-01T00:00:00+00:00", params=PARAMS)
        changed = build_run_identity(
            logical_date="2026-08-02T00:00:00+00:00",
            params=PARAMS,
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)

    def test_parameter_contract_rejects_non_reconciled_gate(self):
        with self.assertRaisesRegex(ValueError, "must equal"):
            RunParameters(10, 4, 2, "integration", "2026-08-01").validate()


class OperationalMetadataTests(unittest.TestCase):
    def _context(self, root: Path):
        return initialize_run(
            metadata_root=root,
            airflow_run_id="manual__phase7-test",
            logical_date="2026-08-01T00:00:00+00:00",
            params=PARAMS,
            pipeline_revision="test-revision",
        )

    def test_full_chain_reconciles_and_rerun_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            context = self._context(Path(directory))
            upstream = None
            receipts = []
            for stage in PROCESSING_STAGES:
                upstream = execute_stage(context=context, stage=stage, upstream=upstream)
                receipts.append(upstream)
            completed = finalize_run(context=context, verification=receipts[-1])
            self.assertEqual(completed["status"], "SUCCEEDED")
            self.assertTrue(completed["reconciliation"]["verified"])

            rerun = execute_stage(context=context, stage=PROCESSING_STAGES[0])
            self.assertTrue(rerun["idempotent_reuse"])
            self.assertEqual(rerun["attempt"], 1)
            repeated_context = initialize_run(
                metadata_root=Path(directory),
                airflow_run_id="manual__phase7-rerun",
                logical_date="2026-08-01T00:00:00+00:00",
                params=PARAMS,
                pipeline_revision="test-revision",
            )
            self.assertTrue(repeated_context["idempotent_reuse"])
            self.assertEqual(len(repeated_context["airflow_run_ids"]), 2)
            self.assertTrue(repeated_context["rerun_events"][0]["idempotent_reuse"])

    def test_failure_is_recorded_and_propagated_then_retry_can_succeed(self):
        with tempfile.TemporaryDirectory() as directory:
            context = self._context(Path(directory))
            with self.assertRaisesRegex(StageFailure, "nasa_extraction failed"):
                execute_stage(context=context, stage="nasa_extraction", fail_for_test=True)
            manifest = json.loads(Path(context["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "FAILED")
            self.assertEqual(manifest["stages"]["nasa_extraction"]["attempt"], 1)
            with self.assertRaisesRegex(StageFailure, "upstream stage"):
                execute_stage(context=context, stage="canonical_transformation")
            receipt = execute_stage(context=context, stage="nasa_extraction")
            self.assertEqual(receipt["status"], "SUCCEEDED")
            self.assertEqual(receipt["attempt"], 2)

    def test_existing_identity_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = self._context(root)
            path = Path(context["manifest_path"])
            manifest = json.loads(path.read_text(encoding="utf-8"))
            manifest["pipeline_revision"] = "different"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "identity conflict"):
                self._context(root)
