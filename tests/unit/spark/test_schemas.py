from __future__ import annotations

import unittest

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from eo_event_platform.spark.schemas import CANONICAL_EVENT_V1_SCHEMA


class CanonicalEventSchemaTests(unittest.TestCase):
    def test_contract_critical_types_are_explicit(self) -> None:
        expected_types = {
            "event_id": StringType,
            "is_synthetic": BooleanType,
            "event_timestamp": TimestampType,
            "ingestion_timestamp": TimestampType,
            "latitude": DoubleType,
            "longitude": DoubleType,
            "raw_row_number": LongType,
            "validation_error_codes": ArrayType,
            "_corrupt_record": StringType,
        }
        actual = {field.name: field.dataType for field in CANONICAL_EVENT_V1_SCHEMA.fields}
        for field_name, expected_type in expected_types.items():
            self.assertIsInstance(actual[field_name], expected_type)

    def test_schema_contains_all_required_lineage_fields(self) -> None:
        names = set(CANONICAL_EVENT_V1_SCHEMA.fieldNames())
        self.assertTrue(
            {
                "source_type",
                "source_dataset",
                "source_record_id",
                "is_synthetic",
                "ingestion_run_id",
                "event_timestamp",
                "ingestion_timestamp",
            }.issubset(names)
        )


if __name__ == "__main__":
    unittest.main()
