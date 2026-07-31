from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

from src.glue_jobs.bronze_to_silver_calendly_glue import (
    FACEBOOK_EVENT_TYPE_URI,
    parse_job_args,
    transform_bronze_calendly_to_silver,
)

# Set timezone to UTC so that execution is consistent, regardless of local timezone. 
@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("test-bronze-to-silver-calendly-glue")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def build_event_membership_schema():
    return ArrayType(
        StructType(
            [
                StructField("user", StringType(), True),
                StructField("user_name", StringType(), True),
                StructField("user_email", StringType(), True),
            ]
        )
    )


def build_location_schema():
    return StructType(
        [
            StructField("type", StringType(), True),
            StructField("location", StringType(), True),
        ]
    )


def build_scheduled_event_schema():
    return StructType(
        [
            StructField("uri", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("location", build_location_schema(), True),
            StructField("event_memberships", build_event_membership_schema(), True),
        ]
    )


def build_tracking_schema():
    return StructType(
        [
            StructField("utm_source", StringType(), True),
            StructField("utm_medium", StringType(), True),
            StructField("utm_campaign", StringType(), True),
            StructField("utm_content", StringType(), True),
            StructField("utm_term", StringType(), True),
            StructField("salesforce_uuid", StringType(), True),
        ]
    )


def build_payload_schema():
    return StructType(
        [
            StructField("uri", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("email", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("canceled", BooleanType(), True),
            StructField("cancel_reason", StringType(), True),
            StructField("scheduled_event", build_scheduled_event_schema(), True),
            StructField("tracking", build_tracking_schema(), True),
        ]
    )


def build_raw_event_schema():
    return StructType(
        [
            StructField("event", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("created_by", StringType(), True),
            StructField("payload", build_payload_schema(), True),
        ]
    )


def build_bronze_calendly_schema():
    return StructType(
        [
            StructField("source_system", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True),
            StructField("raw_s3_key", StringType(), True),
            StructField("raw_event", build_raw_event_schema(), True),
        ]
    )


def build_sample_bronze_rows():
    return [
        {
            "source_system": "calendly",
            "ingestion_timestamp": "2026-07-30T17:00:00+00:00",
            "raw_s3_key": (
                "bronze/calendly_webhooks/"
                "event_type=invitee.created/"
                "ingest_date=2026-07-30/"
                "22a0f2d6-1bde-4fc1-95c1-d969df1da21d.json"
            ),
            "raw_event": {
                "event": "invitee.created",
                "created_at": "2025-07-01T12:00:00.000000Z",
                "created_by": "https://api.calendly.com/users/webhook-owner",
                "payload": {
                    "uri": (
                        "https://api.calendly.com/scheduled_events/"
                        "1ac9e88e-eae3-4e4b-b979-d770cff02d72/"
                        "invitees/"
                        "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
                    ),
                    "created_at": "2025-07-01T12:00:00.000000Z",
                    "updated_at": "2025-07-01T12:05:00.000000Z",
                    "email": "test.invitee@example.com",
                    "name": "Test Invitee",
                    "status": "active",
                    "canceled": False,
                    "cancel_reason": None,
                    "scheduled_event": {
                        "uri": (
                            "https://api.calendly.com/scheduled_events/"
                            "1ac9e88e-eae3-4e4b-b979-d770cff02d72"
                        ),
                        "name": "Demo Call",
                        "status": "active",
                        "event_type": FACEBOOK_EVENT_TYPE_URI,
                        "start_time": "2025-07-09T17:00:00.000000Z",
                        "end_time": "2025-07-09T17:30:00.000000Z",
                        "location": {
                            "type": "zoom",
                            "location": "https://zoom.example.com/test-meeting",
                        },
                        "event_memberships": [
                            {
                                "user": (
                                    "https://api.calendly.com/users/"
                                    "employee-user-123"
                                ),
                                "user_name": "Employee One",
                                "user_email": "employee.one@example.com",
                            }
                        ],
                    },
                    "tracking": {
                        "utm_source": "facebook",
                        "utm_medium": "paid_social",
                        "utm_campaign": "summer_campaign",
                        "utm_content": "video_ad_a",
                        "utm_term": "demo_call",
                        "salesforce_uuid": "sf-123",
                    },
                },
            },
        }
    ]


def test_parse_job_args_reads_required_and_optional_args():
    args = parse_job_args(
        [
            "--bronze_input_path",
            "s3://example-bucket/bronze/calendly_webhooks/",
            "--silver_output_path",
            "s3://example-bucket/silver/calendly_bookings/",
            "--write_mode",
            "append",
        ]
    )

    assert args["bronze_input_path"] == "s3://example-bucket/bronze/calendly_webhooks/"
    assert args["silver_output_path"] == "s3://example-bucket/silver/calendly_bookings/"
    assert args["write_mode"] == "append"


def test_parse_job_args_defaults_write_mode_to_overwrite():
    args = parse_job_args(
        [
            "--bronze_input_path",
            "s3://example-bucket/bronze/calendly_webhooks/",
            "--silver_output_path",
            "s3://example-bucket/silver/calendly_bookings/",
        ]
    )

    assert args["write_mode"] == "overwrite"


def test_parse_job_args_rejects_missing_required_args():
    with pytest.raises(ValueError, match="Missing required Glue job arguments"):
        parse_job_args(
            [
                "--bronze_input_path",
                "s3://example-bucket/bronze/calendly_webhooks/",
            ]
        )


def test_transform_bronze_calendly_to_silver_flattens_webhook_payload(spark):
    bronze_df = spark.createDataFrame(
        build_sample_bronze_rows(),
        schema=build_bronze_calendly_schema(),
    )

    silver_df = transform_bronze_calendly_to_silver(bronze_df)

    rows = silver_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["webhook_event"] == "invitee.created"
    assert row["webhook_created_at"] == "2025-07-01T12:00:00.000000Z"
    assert row["webhook_created_by"] == "https://api.calendly.com/users/webhook-owner"

    assert row["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert row["invitee_email"] == "test.invitee@example.com"
    assert row["invitee_name"] == "Test Invitee"
    assert row["invitee_status"] == "active"
    assert row["invitee_canceled"] is False

    assert row["scheduled_event_id"] == "1ac9e88e-eae3-4e4b-b979-d770cff02d72"
    assert row["scheduled_event_name"] == "Demo Call"
    assert row["scheduled_event_status"] == "active"
    assert row["event_type_uri"] == FACEBOOK_EVENT_TYPE_URI
    assert row["channel"] == "facebook_paid_ads"

    assert row["booking_date"] == date(2025, 7, 1)
    assert row["meeting_date"] == date(2025, 7, 9)
    assert row["meeting_day_of_week"] == "Wednesday"
    assert row["meeting_hour"] == 17
    assert row["meeting_week_start_date"] == date(2025, 7, 7)

    assert row["meeting_location_type"] == "zoom"
    assert row["meeting_location"] == "https://zoom.example.com/test-meeting"

    assert row["utm_source"] == "facebook"
    assert row["utm_medium"] == "paid_social"
    assert row["utm_campaign"] == "summer_campaign"
    assert row["utm_content"] == "video_ad_a"
    assert row["utm_term"] == "demo_call"
    assert row["salesforce_uuid"] == "sf-123"

    assert row["employee_uri"] == "https://api.calendly.com/users/employee-user-123"
    assert row["employee_id"] == "employee-user-123"
    assert row["employee_name"] == "Employee One"
    assert row["employee_email"] == "employee.one@example.com"

    assert row["bronze_source_system"] == "calendly"
    assert row["bronze_ingestion_timestamp"] == "2026-07-30T17:00:00+00:00"
    assert row["bronze_raw_s3_key"].startswith("bronze/calendly_webhooks/")
    assert "bronze_source_file_path" in row.asDict()
    assert row["silver_processed_at"] is not None


def test_transform_bronze_calendly_to_silver_filters_invalid_records(spark):
    valid_row = build_sample_bronze_rows()[0]

    non_invitee_created_row = {
        **valid_row,
        "raw_event": {
            **valid_row["raw_event"],
            "event": "invitee.canceled",
        },
    }

    unsupported_channel_row = {
        **valid_row,
        "raw_event": {
            **valid_row["raw_event"],
            "payload": {
                **valid_row["raw_event"]["payload"],
                "scheduled_event": {
                    **valid_row["raw_event"]["payload"]["scheduled_event"],
                    "event_type": (
                        "https://api.calendly.com/event_types/"
                        "unsupported-event-type"
                    ),
                },
            },
        },
    }

    missing_meeting_start_row = {
        **valid_row,
        "raw_event": {
            **valid_row["raw_event"],
            "payload": {
                **valid_row["raw_event"]["payload"],
                "scheduled_event": {
                    **valid_row["raw_event"]["payload"]["scheduled_event"],
                    "start_time": None,
                },
            },
        },
    }

    bronze_df = spark.createDataFrame(
        [
            valid_row,
            non_invitee_created_row,
            unsupported_channel_row,
            missing_meeting_start_row,
        ],
        schema=build_bronze_calendly_schema(),
    )

    silver_df = transform_bronze_calendly_to_silver(bronze_df)

    rows = silver_df.collect()

    assert len(rows) == 1
    assert rows[0]["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert rows[0]["channel"] == "facebook_paid_ads"