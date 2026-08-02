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

from src.glue_jobs.silver_to_gold_combined_glue import (
    build_gold_combined_dashboard_kpis,
    build_gold_cpb_by_channel,
    build_gold_daily_cpb_by_channel,
    parse_job_args,
    prepare_valid_silver_booking_records,
    prepare_valid_silver_spend_records,
    validate_silver_booking_columns,
    validate_silver_spend_columns,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-to-gold-combined-glue")
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
        ]
    )


def build_silver_booking_schema():
    return StructType(
        [
            StructField("booking_id", StringType(), True),
            StructField("booking_date", DateType(), True),
            StructField("channel", StringType(), True),
        ]
    )


def build_sample_silver_spend_rows():
    return [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 653.28,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "spend_usd": 487.59,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "tiktok_paid_ads",
            "spend_usd": 345.12,
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "facebook_paid_ads",
            "spend_usd": 700.00,
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "youtube_paid_ads",
            "spend_usd": 500.00,
        },
        {
            "spend_date": date(2026, 7, 30),
            "channel": "tiktok_paid_ads",
            "spend_usd": 300.00,
        },
    ]


def build_sample_silver_booking_rows():
    return [
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-002",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-003",
            "booking_date": date(2026, 7, 30),
            "channel": "youtube_paid_ads",
        },
        {
            "booking_id": "booking-004",
            "booking_date": date(2026, 7, 30),
            "channel": "tiktok_paid_ads",
        },
    ]


def test_parse_job_args_reads_required_and_optional_args():
    args = parse_job_args(
        [
            "--silver_spend_input_path",
            "s3://example-bucket/silver/marketing_spend/",
            "--silver_bookings_input_path",
            "s3://example-bucket/silver/calendly_bookings/",
            "--gold_cpb_by_channel_output_path",
            "s3://example-bucket/gold/cpb_by_channel/",
            "--gold_daily_cpb_by_channel_output_path",
            "s3://example-bucket/gold/daily_cpb_by_channel/",
            "--gold_combined_kpis_output_path",
            "s3://example-bucket/gold/combined_dashboard_kpis/",
            "--write_mode",
            "append",
        ]
    )

    assert args["silver_spend_input_path"] == (
        "s3://example-bucket/silver/marketing_spend/"
    )
    assert args["silver_bookings_input_path"] == (
        "s3://example-bucket/silver/calendly_bookings/"
    )
    assert args["gold_cpb_by_channel_output_path"] == (
        "s3://example-bucket/gold/cpb_by_channel/"
    )
    assert args["gold_daily_cpb_by_channel_output_path"] == (
        "s3://example-bucket/gold/daily_cpb_by_channel/"
    )
    assert args["gold_combined_kpis_output_path"] == (
        "s3://example-bucket/gold/combined_dashboard_kpis/"
    )
    assert args["write_mode"] == "append"


def test_parse_job_args_defaults_write_mode_to_overwrite():
    args = parse_job_args(
        [
            "--silver_spend_input_path",
            "s3://example-bucket/silver/marketing_spend/",
            "--silver_bookings_input_path",
            "s3://example-bucket/silver/calendly_bookings/",
            "--gold_cpb_by_channel_output_path",
            "s3://example-bucket/gold/cpb_by_channel/",
            "--gold_daily_cpb_by_channel_output_path",
            "s3://example-bucket/gold/daily_cpb_by_channel/",
            "--gold_combined_kpis_output_path",
            "s3://example-bucket/gold/combined_dashboard_kpis/",
        ]
    )

    assert args["write_mode"] == "overwrite"


def test_parse_job_args_rejects_missing_required_args():
    with pytest.raises(ValueError, match="Missing required Glue job arguments"):
        parse_job_args(
            [
                "--silver_spend_input_path",
                "s3://example-bucket/silver/marketing_spend/",
                "--silver_bookings_input_path",
                "s3://example-bucket/silver/calendly_bookings/",
            ]
        )


def test_validate_silver_spend_columns_accepts_required_columns(spark):
    spend_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )

    validate_silver_spend_columns(spend_df)


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


def test_validate_silver_booking_columns_accepts_required_columns(spark):
    booking_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    validate_silver_booking_columns(booking_df)


def test_validate_silver_booking_columns_rejects_missing_columns(spark):
    bad_df = spark.createDataFrame(
        [
            {
                "booking_id": "booking-001",
                "booking_date": date(2026, 7, 29),
            }
        ]
    )

    with pytest.raises(ValueError, match="missing columns"):
        validate_silver_booking_columns(bad_df)


def test_prepare_valid_silver_spend_records_filters_invalid_rows(spark):
    rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": None,
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": None,
            "spend_usd": 100.00,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "spend_usd": None,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "tiktok_paid_ads",
            "spend_usd": -10.00,
        },
    ]

    spend_df = spark.createDataFrame(rows, schema=build_silver_spend_schema())

    valid_df = prepare_valid_silver_spend_records(spend_df)

    collected_rows = valid_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["spend_date"] == date(2026, 7, 29)
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert float(collected_rows[0]["spend_usd"]) == 100.00


def test_prepare_valid_silver_booking_records_filters_invalid_rows(spark):
    rows = [
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": None,
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-002",
            "booking_date": None,
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-003",
            "booking_date": date(2026, 7, 29),
            "channel": None,
        },
    ]

    booking_df = spark.createDataFrame(rows, schema=build_silver_booking_schema())

    valid_df = prepare_valid_silver_booking_records(booking_df)

    collected_rows = valid_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["booking_id"] == "booking-001"
    assert collected_rows[0]["booking_date"] == date(2026, 7, 29)
    assert collected_rows[0]["channel"] == "facebook_paid_ads"


def test_build_gold_cpb_by_channel_combines_spend_and_bookings(spark):
    spend_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )
    booking_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_cpb_by_channel(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["channel"],
    )

    assert rows == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 1353.28,
            "booked_call_count": 2,
            "cost_per_booking_usd": 676.64,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 645.12,
            "booked_call_count": 1,
            "cost_per_booking_usd": 645.12,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "channel": "youtube_paid_ads",
            "total_spend_usd": 987.59,
            "booked_call_count": 1,
            "cost_per_booking_usd": 987.59,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
    ]

    assert rows[0]["gold_processed_at"] is not None
    assert rows[1]["gold_processed_at"] is not None
    assert rows[2]["gold_processed_at"] is not None


def test_build_gold_cpb_by_channel_includes_spend_with_no_bookings(spark):
    spend_rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "spend_usd": 50.00,
        },
    ]
    booking_rows = [
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        }
    ]

    spend_df = spark.createDataFrame(spend_rows, schema=build_silver_spend_schema())
    booking_df = spark.createDataFrame(
        booking_rows,
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_cpb_by_channel(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["channel"],
    )

    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["total_spend_usd"] == 100.00
    assert rows[0]["booked_call_count"] == 1
    assert rows[0]["cost_per_booking_usd"] == 100.00

    assert rows[1]["channel"] == "youtube_paid_ads"
    assert rows[1]["total_spend_usd"] == 50.00
    assert rows[1]["booked_call_count"] == 0
    assert rows[1]["cost_per_booking_usd"] is None


def test_build_gold_cpb_by_channel_includes_bookings_with_no_spend(spark):
    spend_rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        }
    ]
    booking_rows = [
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-002",
            "booking_date": date(2026, 7, 29),
            "channel": "organic_referral",
        },
    ]

    spend_df = spark.createDataFrame(spend_rows, schema=build_silver_spend_schema())
    booking_df = spark.createDataFrame(
        booking_rows,
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_cpb_by_channel(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["channel"],
    )

    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["total_spend_usd"] == 100.00
    assert rows[0]["booked_call_count"] == 1
    assert rows[0]["cost_per_booking_usd"] == 100.00

    assert rows[1]["channel"] == "organic_referral"
    assert rows[1]["total_spend_usd"] == 0.00
    assert rows[1]["booked_call_count"] == 1
    assert rows[1]["cost_per_booking_usd"] == 0.00


def test_build_gold_cpb_by_channel_counts_distinct_booking_ids(spark):
    spend_rows = [
        {
            "spend_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        }
    ]
    booking_rows = [
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-001",
            "booking_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
        },
    ]

    spend_df = spark.createDataFrame(spend_rows, schema=build_silver_spend_schema())
    booking_df = spark.createDataFrame(
        booking_rows,
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_cpb_by_channel(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = gold_df.collect()

    assert len(rows) == 1
    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["total_spend_usd"] == 100.00
    assert rows[0]["booked_call_count"] == 1
    assert rows[0]["cost_per_booking_usd"] == 100.00


def test_build_gold_daily_cpb_by_channel_combines_by_date_and_channel(spark):
    spend_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )
    booking_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_daily_cpb_by_channel(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: (row["metric_date"], row["channel"]),
    )

    assert rows == [
        {
            "metric_date": date(2026, 7, 29),
            "channel": "facebook_paid_ads",
            "total_spend_usd": 653.28,
            "booked_call_count": 2,
            "cost_per_booking_usd": 326.64,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "metric_date": date(2026, 7, 29),
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 345.12,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "metric_date": date(2026, 7, 29),
            "channel": "youtube_paid_ads",
            "total_spend_usd": 487.59,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
        {
            "metric_date": date(2026, 7, 30),
            "channel": "facebook_paid_ads",
            "total_spend_usd": 700.00,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
            "gold_processed_at": rows[3]["gold_processed_at"],
        },
        {
            "metric_date": date(2026, 7, 30),
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 300.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 300.00,
            "gold_processed_at": rows[4]["gold_processed_at"],
        },
        {
            "metric_date": date(2026, 7, 30),
            "channel": "youtube_paid_ads",
            "total_spend_usd": 500.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 500.00,
            "gold_processed_at": rows[5]["gold_processed_at"],
        },
    ]

    assert all(row["gold_processed_at"] is not None for row in rows)


def test_build_gold_combined_dashboard_kpis_returns_expected_summary(spark):
    spend_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )
    booking_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_combined_dashboard_kpis(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = gold_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["total_spend_usd"] == 2985.99
    assert row["total_bookings"] == 4
    assert row["average_cost_per_booking_usd"] == 746.50
    assert row["channel_count"] == 3
    assert row["spend_date_count"] == 2
    assert row["booking_date_count"] == 2
    assert row["gold_processed_at"] is not None


def test_build_gold_combined_dashboard_kpis_handles_zero_bookings(spark):
    spend_df = spark.createDataFrame(
        build_sample_silver_spend_rows(),
        schema=build_silver_spend_schema(),
    )
    booking_df = spark.createDataFrame([], schema=build_silver_booking_schema())

    gold_df = build_gold_combined_dashboard_kpis(
        silver_spend_df=spend_df,
        silver_bookings_df=booking_df,
    )

    rows = gold_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["total_spend_usd"] == 2985.99
    assert row["total_bookings"] == 0
    assert row["average_cost_per_booking_usd"] is None
    assert row["channel_count"] == 3
    assert row["spend_date_count"] == 2
    assert row["booking_date_count"] == 0
    assert row["gold_processed_at"] is not None