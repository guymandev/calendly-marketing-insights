from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.glue_jobs.silver_to_gold_spend_glue import (
    build_gold_channel_spend_summary,
    build_gold_daily_spend_by_channel,
    parse_job_args,
    prepare_valid_silver_spend_records,
    validate_silver_spend_columns,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-to-gold-spend-glue")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def build_silver_spend_schema():
    return StructType(
        [
            StructField("spend_date", DateType(), True),
            StructField("channel", StringType(), True),
            StructField("spend_usd", DoubleType(), True),
            StructField("source_file", StringType(), True),
        ]
    )


def build_sample_silver_spend_rows():
    return [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 653.28,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "spend_usd": 487.59,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "tiktok_paid_ads",
            "spend_usd": 345.12,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "facebook_paid_ads",
            "spend_usd": 700.00,
            "source_file": "spend_data_2026-07-30.json",
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "youtube_paid_ads",
            "spend_usd": 500.00,
            "source_file": "spend_data_2026-07-30.json",
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "tiktok_paid_ads",
            "spend_usd": 300.00,
            "source_file": "spend_data_2026-07-30.json",
        },
    ]


def test_parse_job_args_reads_required_and_optional_args():
    args = parse_job_args(
        [
            "--silver_spend_input_path",
            "s3://example-bucket/silver/marketing_spend/",
            "--gold_daily_spend_output_path",
            "s3://example-bucket/gold/daily_spend_by_channel/",
            "--gold_channel_summary_output_path",
            "s3://example-bucket/gold/channel_spend_summary/",
            "--write_mode",
            "append",
        ]
    )

    assert args["silver_spend_input_path"] == (
        "s3://example-bucket/silver/marketing_spend/"
    )
    assert args["gold_daily_spend_output_path"] == (
        "s3://example-bucket/gold/daily_spend_by_channel/"
    )
    assert args["gold_channel_summary_output_path"] == (
        "s3://example-bucket/gold/channel_spend_summary/"
    )
    assert args["write_mode"] == "append"


def test_parse_job_args_defaults_write_mode_to_overwrite():
    args = parse_job_args(
        [
            "--silver_spend_input_path",
            "s3://example-bucket/silver/marketing_spend/",
            "--gold_daily_spend_output_path",
            "s3://example-bucket/gold/daily_spend_by_channel/",
            "--gold_channel_summary_output_path",
            "s3://example-bucket/gold/channel_spend_summary/",
        ]
    )

    assert args["write_mode"] == "overwrite"


def test_parse_job_args_rejects_missing_required_args():
    with pytest.raises(ValueError, match="Missing required Glue job arguments"):
        parse_job_args(
            [
                "--silver_spend_input_path",
                "s3://example-bucket/silver/marketing_spend/",
                "--gold_daily_spend_output_path",
                "s3://example-bucket/gold/daily_spend_by_channel/",
            ]
        )


def test_validate_silver_spend_columns_accepts_required_columns(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )

    validate_silver_spend_columns(silver_df)


def test_validate_silver_spend_columns_rejects_missing_columns(spark):
    bad_df = spark.createDataFrame(
        [
            {
                "spend_date": date(2026, 7, 29),
                "channel": "facebook_paid_ads",
            }
        ]
    )

    with pytest.raises(ValueError, match="missing columns"):
        validate_silver_spend_columns(bad_df)


def test_prepare_valid_silver_spend_records_filters_invalid_rows(spark):
    rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": None,
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": None,
            "spend_usd": 100.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "spend_usd": None,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "tiktok_paid_ads",
            "spend_usd": -10.00,
            "source_file": "spend_data_2026-07-29.json",
        },
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_spend_schema())

    valid_df = prepare_valid_silver_spend_records(silver_df)

    collected_rows = valid_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["spend_date"] == date(2026, 7, 29)
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert collected_rows[0]["spend_usd"] == 100.00


def test_build_gold_daily_spend_by_channel_groups_by_date_and_channel(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )

    gold_df = build_gold_daily_spend_by_channel(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: (row["spend_date"], row["channel"]),
    )

    assert len(rows) == 6

    assert rows[0]["spend_date"] == date(2026, 7, 29)
    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["total_spend_usd"] == 653.28
    assert rows[0]["gold_processed_at"] is not None

    assert rows[1]["spend_date"] == date(2026, 7, 29)
    assert rows[1]["channel"] == "tiktok_paid_ads"
    assert rows[1]["total_spend_usd"] == 345.12

    assert rows[2]["spend_date"] == date(2026, 7, 29)
    assert rows[2]["channel"] == "youtube_paid_ads"
    assert rows[2]["total_spend_usd"] == 487.59


def test_build_gold_daily_spend_by_channel_sums_duplicate_rows(spark):
    rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 50.25,
            "source_file": "spend_data_2026-07-29.json",
        },
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_spend_schema())

    gold_df = build_gold_daily_spend_by_channel(silver_df)

    collected_rows = gold_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["spend_date"] == date(2026, 7, 29)
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert collected_rows[0]["total_spend_usd"] == 150.25


def test_build_gold_channel_spend_summary_groups_by_channel(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )

    gold_df = build_gold_channel_spend_summary(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["channel"],
    )

    assert rows == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 1353.28,
            "spend_day_count": 2,
            "average_daily_spend_usd": 676.64,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 645.12,
            "spend_day_count": 2,
            "average_daily_spend_usd": 322.56,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "channel": "youtube_paid_ads",
            "total_spend_usd": 987.59,
            "spend_day_count": 2,
            "average_daily_spend_usd": 493.8,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
    ]

    assert rows[0]["gold_processed_at"] is not None
    assert rows[1]["gold_processed_at"] is not None
    assert rows[2]["gold_processed_at"] is not None


def test_build_gold_channel_spend_summary_counts_distinct_spend_days(spark):
    rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 50.00,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "facebook_paid_ads",
            "spend_usd": 200.00,
            "source_file": "spend_data_2026-07-30.json",
        },
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_spend_schema())

    gold_df = build_gold_channel_spend_summary(silver_df)

    collected_rows = gold_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert collected_rows[0]["total_spend_usd"] == 350.00
    assert collected_rows[0]["spend_day_count"] == 2
    assert collected_rows[0]["average_daily_spend_usd"] == 175.00