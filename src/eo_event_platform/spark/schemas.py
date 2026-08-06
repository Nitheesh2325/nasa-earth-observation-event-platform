"""Explicit Spark schemas for versioned platform contracts."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CANONICAL_EVENT_V1_FIELDS = (
    StructField("event_id", StringType(), True),
    StructField("detection_id", StringType(), True),
    StructField("lineage_root_id", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("source_dataset", StringType(), True),
    StructField("source_record_id", StringType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("ingestion_run_id", StringType(), True),
    StructField("replay_run_id", StringType(), True),
    StructField("synthetic_generation_id", StringType(), True),
    StructField("parent_event_id", StringType(), True),
    StructField("scheduled_replay_timestamp", TimestampType(), True),
    StructField("replay_iteration", LongType(), True),
    StructField("replay_sequence_number", LongType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bright_ti4_kelvin", DoubleType(), True),
    StructField("bright_ti5_kelvin", DoubleType(), True),
    StructField("fire_radiative_power_mw", DoubleType(), True),
    StructField("scan_km", DoubleType(), True),
    StructField("track_km", DoubleType(), True),
    StructField("confidence", StringType(), True),
    StructField("day_night", StringType(), True),
    StructField("satellite", StringType(), True),
    StructField("instrument", StringType(), True),
    StructField("source_product_version", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("raw_object_uri", StringType(), True),
    StructField("raw_file_name", StringType(), True),
    StructField("raw_row_number", LongType(), True),
    StructField("raw_payload_hash", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("validation_status", StringType(), True),
    StructField("validation_error_codes", ArrayType(StringType(), False), True),
    StructField("deduplication_status", StringType(), True),
    StructField("enrichment_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
)

CANONICAL_EVENT_V1_SCHEMA = StructType(CANONICAL_EVENT_V1_FIELDS)
