from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.glue_jobs.silver_to_gold_bookings_glue import (
    build_gold_booking_dashboard_kpis,
    build_gold_booking_trends,
    build_gold_booking_volume_by_time_slot,
    build_gold_channel_attribution,
    build_gold_daily_calls_by_source,
    build_gold_employee_meeting_load,
    parse_job_args,
    prepare_valid_silver_booking_records,
    validate_silver_booking_columns,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-to-gold-bookings-glue")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def build_silver_booking_schema():
    return StructType(
        [
            StructField("booking_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("booking_date", DateType(), True),
            StructField("meeting_date", DateType(), True),
            StructField("meeting_day_of_week", StringType(), True),
            StructField("meeting_hour", IntegerType(), True),
            StructField("utm_campaign", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("employee_email", StringType(), True),
        ]
    )


def build_sample_silver_booking_rows():
    return [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-002",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 18,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-003",
            "channel": "youtube_paid_ads",
            "booking_date": date(2026, 7, 30),
            "meeting_date": date(2026, 8, 2),
            "meeting_day_of_week": "Sunday",
            "meeting_hour": 10,
            "utm_campaign": "youtube_campaign",
            "employee_id": "employee-002",
            "employee_name": "Employee Two",
            "employee_email": "employee.two@example.com",
        },
        {
            "booking_id": "booking-004",
            "channel": "tiktok_paid_ads",
            "booking_date": date(2026, 7, 30),
            "meeting_date": date(2026, 8, 3),
            "meeting_day_of_week": "Monday",
            "meeting_hour": 9,
            "utm_campaign": None,
            "employee_id": None,
            "employee_name": None,
            "employee_email": None,
        },
    ]


def test_parse_job_args_reads_required_and_optional_args():
    args = parse_job_args(
        [
            "--silver_bookings_input_path",
            "s3://example-bucket/silver/calendly_bookings/",
            "--gold_daily_calls_output_path",
            "s3://example-bucket/gold/daily_calls_by_source/",
            "--gold_booking_trends_output_path",
            "s3://example-bucket/gold/booking_trends/",
            "--gold_channel_attribution_output_path",
            "s3://example-bucket/gold/channel_attribution/",
            "--gold_time_slot_output_path",
            "s3://example-bucket/gold/booking_volume_by_time_slot/",
            "--gold_employee_load_output_path",
            "s3://example-bucket/gold/employee_meeting_load/",
            "--gold_booking_kpis_output_path",
            "s3://example-bucket/gold/booking_dashboard_kpis/",
            "--write_mode",
            "append",
        ]
    )

    assert args["silver_bookings_input_path"] == (
        "s3://example-bucket/silver/calendly_bookings/"
    )
    assert args["gold_daily_calls_output_path"] == (
        "s3://example-bucket/gold/daily_calls_by_source/"
    )
    assert args["gold_booking_trends_output_path"] == (
        "s3://example-bucket/gold/booking_trends/"
    )
    assert args["gold_channel_attribution_output_path"] == (
        "s3://example-bucket/gold/channel_attribution/"
    )
    assert args["gold_time_slot_output_path"] == (
        "s3://example-bucket/gold/booking_volume_by_time_slot/"
    )
    assert args["gold_employee_load_output_path"] == (
        "s3://example-bucket/gold/employee_meeting_load/"
    )
    assert args["gold_booking_kpis_output_path"] == (
        "s3://example-bucket/gold/booking_dashboard_kpis/"
    )
    assert args["write_mode"] == "append"


def test_parse_job_args_defaults_write_mode_to_overwrite():
    args = parse_job_args(
        [
            "--silver_bookings_input_path",
            "s3://example-bucket/silver/calendly_bookings/",
            "--gold_daily_calls_output_path",
            "s3://example-bucket/gold/daily_calls_by_source/",
            "--gold_booking_trends_output_path",
            "s3://example-bucket/gold/booking_trends/",
            "--gold_channel_attribution_output_path",
            "s3://example-bucket/gold/channel_attribution/",
            "--gold_time_slot_output_path",
            "s3://example-bucket/gold/booking_volume_by_time_slot/",
            "--gold_employee_load_output_path",
            "s3://example-bucket/gold/employee_meeting_load/",
            "--gold_booking_kpis_output_path",
            "s3://example-bucket/gold/booking_dashboard_kpis/",
        ]
    )

    assert args["write_mode"] == "overwrite"


def test_parse_job_args_rejects_missing_required_args():
    with pytest.raises(ValueError, match="Missing required Glue job arguments"):
        parse_job_args(
            [
                "--silver_bookings_input_path",
                "s3://example-bucket/silver/calendly_bookings/",
                "--gold_daily_calls_output_path",
                "s3://example-bucket/gold/daily_calls_by_source/",
            ]
        )


def test_validate_silver_booking_columns_accepts_required_columns(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    validate_silver_booking_columns(silver_df)


def test_validate_silver_booking_columns_rejects_missing_columns(spark):
    bad_df = spark.createDataFrame(
        [
            {
                "booking_id": "booking-001",
                "channel": "facebook_paid_ads",
                "booking_date": date(2026, 7, 29),
            }
        ]
    )

    with pytest.raises(ValueError, match="missing columns"):
        validate_silver_booking_columns(bad_df)


def test_prepare_valid_silver_booking_records_filters_invalid_rows(spark):
    rows = [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": None,
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-002",
            "channel": None,
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-003",
            "channel": "youtube_paid_ads",
            "booking_date": None,
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-004",
            "channel": "tiktok_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": None,
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-005",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": None,
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-006",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": None,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-007",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 24,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_booking_schema())

    valid_df = prepare_valid_silver_booking_records(silver_df)

    collected_rows = valid_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["booking_id"] == "booking-001"
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert collected_rows[0]["booking_date"] == date(2026, 7, 29)
    assert collected_rows[0]["meeting_date"] == date(2026, 8, 1)
    assert collected_rows[0]["meeting_day_of_week"] == "Saturday"
    assert collected_rows[0]["meeting_hour"] == 17


def test_prepare_valid_silver_booking_records_defaults_optional_values(spark):
    rows = [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": None,
            "employee_id": None,
            "employee_name": None,
            "employee_email": None,
        }
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_booking_schema())

    valid_df = prepare_valid_silver_booking_records(silver_df)

    collected_rows = valid_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["utm_campaign"] == "unknown_campaign"
    assert collected_rows[0]["employee_id"] == "unknown_employee"
    assert collected_rows[0]["employee_name"] == "Unknown Employee"
    assert collected_rows[0]["employee_email"] == "unknown_email"


def test_build_gold_daily_calls_by_source_groups_by_date_and_channel(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_daily_calls_by_source(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: (row["booking_date"], row["channel"]),
    )

    assert len(rows) == 3

    assert rows[0]["booking_date"] == date(2026, 7, 29)
    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["booked_call_count"] == 2
    assert rows[0]["gold_processed_at"] is not None

    assert rows[1]["booking_date"] == date(2026, 7, 30)
    assert rows[1]["channel"] == "tiktok_paid_ads"
    assert rows[1]["booked_call_count"] == 1

    assert rows[2]["booking_date"] == date(2026, 7, 30)
    assert rows[2]["channel"] == "youtube_paid_ads"
    assert rows[2]["booked_call_count"] == 1


def test_build_gold_daily_calls_by_source_counts_distinct_booking_ids(spark):
    rows = [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": date(2026, 7, 29),
            "meeting_date": date(2026, 8, 1),
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
    ]

    silver_df = spark.createDataFrame(rows, schema=build_silver_booking_schema())

    gold_df = build_gold_daily_calls_by_source(silver_df)

    collected_rows = gold_df.collect()

    assert len(collected_rows) == 1
    assert collected_rows[0]["booking_date"] == date(2026, 7, 29)
    assert collected_rows[0]["channel"] == "facebook_paid_ads"
    assert collected_rows[0]["booked_call_count"] == 1


def test_build_gold_booking_trends_groups_by_booking_date(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_booking_trends(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["booking_date"],
    )

    assert len(rows) == 2

    assert rows[0]["booking_date"] == date(2026, 7, 29)
    assert rows[0]["booked_call_count"] == 2
    assert rows[0]["gold_processed_at"] is not None

    assert rows[1]["booking_date"] == date(2026, 7, 30)
    assert rows[1]["booked_call_count"] == 2


def test_build_gold_channel_attribution_groups_by_channel_and_campaign(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_channel_attribution(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: (row["channel"], row["utm_campaign"]),
    )

    assert rows == [
        {
            "channel": "facebook_paid_ads",
            "utm_campaign": "summer_campaign",
            "booked_call_count": 2,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "channel": "tiktok_paid_ads",
            "utm_campaign": "unknown_campaign",
            "booked_call_count": 1,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "channel": "youtube_paid_ads",
            "utm_campaign": "youtube_campaign",
            "booked_call_count": 1,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
    ]

    assert rows[0]["gold_processed_at"] is not None
    assert rows[1]["gold_processed_at"] is not None
    assert rows[2]["gold_processed_at"] is not None


def test_build_gold_booking_volume_by_time_slot_groups_by_day_and_hour(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_booking_volume_by_time_slot(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: (row["meeting_day_of_week"], row["meeting_hour"]),
    )

    assert rows == [
        {
            "meeting_day_of_week": "Monday",
            "meeting_hour": 9,
            "booked_call_count": 1,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "booked_call_count": 1,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 18,
            "booked_call_count": 1,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
        {
            "meeting_day_of_week": "Sunday",
            "meeting_hour": 10,
            "booked_call_count": 1,
            "gold_processed_at": rows[3]["gold_processed_at"],
        },
    ]

    assert rows[0]["gold_processed_at"] is not None
    assert rows[1]["gold_processed_at"] is not None
    assert rows[2]["gold_processed_at"] is not None
    assert rows[3]["gold_processed_at"] is not None


def test_build_gold_employee_meeting_load_groups_by_employee(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_employee_meeting_load(silver_df)

    rows = sorted(
        [row.asDict() for row in gold_df.collect()],
        key=lambda row: row["employee_id"],
    )

    assert rows == [
        {
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
            "booked_call_count": 2,
            "gold_processed_at": rows[0]["gold_processed_at"],
        },
        {
            "employee_id": "employee-002",
            "employee_name": "Employee Two",
            "employee_email": "employee.two@example.com",
            "booked_call_count": 1,
            "gold_processed_at": rows[1]["gold_processed_at"],
        },
        {
            "employee_id": "unknown_employee",
            "employee_name": "Unknown Employee",
            "employee_email": "unknown_email",
            "booked_call_count": 1,
            "gold_processed_at": rows[2]["gold_processed_at"],
        },
    ]

    assert rows[0]["gold_processed_at"] is not None
    assert rows[1]["gold_processed_at"] is not None
    assert rows[2]["gold_processed_at"] is not None


def test_build_gold_booking_dashboard_kpis_returns_booking_summary(spark):
    silver_df = spark.createDataFrame(
        build_sample_silver_booking_rows(),
        schema=build_silver_booking_schema(),
    )

    gold_df = build_gold_booking_dashboard_kpis(silver_df)

    rows = gold_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["total_bookings"] == 4
    assert row["channel_count"] == 3
    assert row["booking_date_count"] == 2
    assert row["meeting_date_count"] == 3
    assert row["gold_processed_at"] is not None