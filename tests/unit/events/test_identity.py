"""Tests for deterministic NASA FIRMS identity semantics."""

from __future__ import annotations

import unittest

from eo_event_platform.events.identity import (
    build_source_record_id,
    normalize_acquisition_time,
    normalize_decimal,
)


def identity_row(**overrides: str) -> dict[str, str]:
    row = {
        "satellite": "N",
        "acq_date": "2026-08-06",
        "acq_time": "42",
        "latitude": "34.1000",
        "longitude": "-118.2000",
        "version": "2.0NRT",
    }
    row.update(overrides)
    return row


class IdentityTests(unittest.TestCase):
    def test_equivalent_numeric_and_time_formats_have_same_identity(self) -> None:
        first = build_source_record_id(identity_row(), "VIIRS_SNPP_NRT")
        second = build_source_record_id(
            identity_row(acq_time="0042", latitude="34.1", longitude="-118.2"),
            "VIIRS_SNPP_NRT",
        )
        self.assertEqual(first, second)

    def test_source_product_version_changes_identity(self) -> None:
        first = build_source_record_id(identity_row(), "VIIRS_SNPP_NRT")
        second = build_source_record_id(
            identity_row(version="2.1NRT"), "VIIRS_SNPP_NRT"
        )
        self.assertNotEqual(first, second)

    def test_decimal_normalization_avoids_binary_float_conversion(self) -> None:
        self.assertEqual(normalize_decimal("-0.000"), "0")
        self.assertEqual(normalize_decimal("34.100000"), "34.1")

    def test_acquisition_time_rejects_invalid_clock_value(self) -> None:
        with self.assertRaises(ValueError):
            normalize_acquisition_time("2460")


if __name__ == "__main__":
    unittest.main()

