from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.glue_jobs.bronze_to_silver_spend_glue import (
    parse_job_args,
    transform_bronze_spend_to_silver,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("test-bronze-to-silver-spend-glue")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def build_bronze_spend_schema():
    return StructType(
        [
            StructField("source_system", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("raw_s3_key", StringType(), True),
            StructField(
                "raw_payload",
                ArrayType(
                    StructType(
                        [
                            StructField("date", StringType(), True),
                            StructField("channel", StringType(), True),
                            StructField("spend", DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )


def build_sample_bronze_rows():
    return [
        {
            "source_system": "marketing_spend_public_s3",
            "ingestion_timestamp": "2026-07-30T18:44:30+00:00",
            "source_url": (
                "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
                "calendly_spend_data/spend_data_2026-07-29.json"
            ),
            "source_file_name": "spend_data_2026-07-29.json",
            "raw_s3_key": (
                "bronze/marketing_spend/"
                "ingest_date=2026-07-30/"
                "spend_data_2026-07-29.json"
            ),
            "raw_payload": [
                {
                    "date": "2026-07-29",
                    "channel": "facebook_paid_ads",
                    "spend": 653.284,
                },
                {
                    "date": "2026-07-29",
                    "channel": "youtube_paid_ads",
                    "spend": 487.59,
                },
                {
                    "date": "2026-07-29",
                    "channel": "tiktok_paid_ads",
                    "spend": 345.12,
                },
            ],
        }
    ]


def test_parse_job_args_reads_required_and_optional_args():
    args = parse_job_args(
        [
            "--bronze_input_path",
            "s3://example-bucket/bronze/marketing_spend/",
            "--silver_output_path",
            "s3://example-bucket/silver/marketing_spend/",
            "--write_mode",
            "append",
        ]
    )

    assert args["bronze_input_path"] == "s3://example-bucket/bronze/marketing_spend/"
    assert args["silver_output_path"] == "s3://example-bucket/silver/marketing_spend/"
    assert args["write_mode"] == "append"


def test_parse_job_args_defaults_write_mode_to_overwrite():
    args = parse_job_args(
        [
            "--bronze_input_path",
            "s3://example-bucket/bronze/marketing_spend/",
            "--silver_output_path",
            "s3://example-bucket/silver/marketing_spend/",
        ]
    )

    assert args["write_mode"] == "overwrite"


def test_parse_job_args_rejects_missing_required_args():
    with pytest.raises(ValueError, match="Missing required Glue job arguments"):
        parse_job_args(
            [
                "--bronze_input_path",
                "s3://example-bucket/bronze/marketing_spend/",
            ]
        )


def test_transform_bronze_spend_to_silver_flattens_payload_rows(spark):
    bronze_df = spark.createDataFrame(
        build_sample_bronze_rows(),
        schema=build_bronze_spend_schema(),
    )

    silver_df = transform_bronze_spend_to_silver(bronze_df)

    rows = sorted(
        silver_df.collect(),
        key=lambda row: row["channel"],
    )

    assert len(rows) == 3

    facebook_row = next(row for row in rows if row["channel"] == "facebook_paid_ads")

    assert facebook_row["spend_date"] == date(2026, 7, 29)
    assert facebook_row["channel"] == "facebook_paid_ads"
    assert facebook_row["spend_usd"] == 653.28
    assert facebook_row["source_file"] == "spend_data_2026-07-29.json"

    assert facebook_row["bronze_source_system"] == "marketing_spend_public_s3"
    assert facebook_row["bronze_ingestion_timestamp"] == "2026-07-30T18:44:30+00:00"
    assert facebook_row["bronze_source_url"].endswith(
        "calendly_spend_data/spend_data_2026-07-29.json"
    )
    assert facebook_row["bronze_raw_s3_key"].startswith("bronze/marketing_spend/")
    assert "bronze_source_file_path" in facebook_row.asDict()
    assert facebook_row["silver_processed_at"] is not None


def test_transform_bronze_spend_to_silver_keeps_only_valid_rows(spark):
    bronze_rows = [
        {
            "source_system": "marketing_spend_public_s3",
            "ingestion_timestamp": "2026-07-30T18:44:30+00:00",
            "source_url": "https://example.com/spend_data_2026-07-29.json",
            "source_file_name": "spend_data_2026-07-29.json",
            "raw_s3_key": "bronze/marketing_spend/spend_data_2026-07-29.json",
            "raw_payload": [
                {
                    "date": "2026-07-29",
                    "channel": "facebook_paid_ads",
                    "spend": 100.00,
                },
                {
                    "date": "not-a-date",
                    "channel": "facebook_paid_ads",
                    "spend": 100.00,
                },
                {
                    "date": "2026-07-29",
                    "channel": "unsupported_channel",
                    "spend": 100.00,
                },
                {
                    "date": "2026-07-29",
                    "channel": "youtube_paid_ads",
                    "spend": -50.00,
                },
                {
                    "date": "2026-07-29",
                    "channel": "tiktok_paid_ads",
                    "spend": None,
                },
            ],
        }
    ]

    bronze_df = spark.createDataFrame(
        bronze_rows,
        schema=build_bronze_spend_schema(),
    )

    silver_df = transform_bronze_spend_to_silver(bronze_df)

    rows = silver_df.collect()

    assert len(rows) == 1
    assert rows[0]["spend_date"] == date(2026, 7, 29)
    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["spend_usd"] == 100.00