import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    create_map,
    current_timestamp,
    date_format,
    date_sub,
    dayofweek,
    element_at,
    input_file_name,
    lit,
    pmod,
    regexp_extract,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import IntegerType, StringType


FACEBOOK_EVENT_TYPE_URI = (
    "https://api.calendly.com/event_types/"
    "d639ecd3-8718-4068-955a-436b10d72c78"
)

YOUTUBE_EVENT_TYPE_URI = (
    "https://api.calendly.com/event_types/"
    "dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098"
)

TIKTOK_EVENT_TYPE_URI = (
    "https://api.calendly.com/event_types/"
    "bb339e98-7a67-4af2-b584-8dbf95564312"
)


def parse_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Parse Glue-style command line arguments without requiring awsglue locally.

    Expected arguments:
        --bronze_input_path s3://.../bronze/calendly_webhooks/
        --silver_output_path s3://.../silver/calendly_bookings/

    Optional:
        --write_mode overwrite|append
    """
    args: Dict[str, str] = {}

    index = 0
    while index < len(argv):
        token = argv[index]

        if token.startswith("--"):
            key = token[2:]

            if index + 1 >= len(argv):
                raise ValueError(f"Missing value for argument: {token}")

            value = argv[index + 1]
            args[key] = value
            index += 2
        else:
            index += 1

    required_args = [
        "bronze_input_path",
        "silver_output_path",
    ]

    missing_args = [name for name in required_args if not args.get(name)]

    if missing_args:
        raise ValueError(f"Missing required Glue job arguments: {missing_args}")

    args.setdefault("write_mode", "overwrite")

    return args

# Set timezone to UTC so that execution is consistent, regardless of local timezone. 
def create_spark_session(app_name: str = "bronze-to-silver-calendly") -> SparkSession:
    """
    Create a SparkSession configured for Delta Lake.

    In AWS Glue, the job should also be configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_bronze_calendly_records(
    spark: SparkSession,
    bronze_input_path: str,
) -> DataFrame:
    """
    Read Bronze Calendly webhook JSON files from S3.

    The Bronze records were written by the calendly-webhook-ingest Lambda and
    have this shape:

        {
          "source_system": "calendly",
          "ingestion_timestamp": "...",
          "raw_s3_key": "bronze/calendly_webhooks/...",
          "raw_event": {
            "event": "invitee.created",
            "created_at": "...",
            "created_by": "...",
            "payload": {...}
          }
        }
    """
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_input_path)
    )


def extract_id_from_uri(uri_column):
    """
    Extract the final path segment from a Calendly URI.

    Example:
        https://api.calendly.com/scheduled_events/abc-123
        -> abc-123
    """
    return regexp_extract(uri_column.cast(StringType()), r"([^/]+)$", 1)


def build_event_type_channel_map():
    """
    Build a Spark map expression from Calendly event type URI to marketing channel.
    """
    return create_map(
        lit(FACEBOOK_EVENT_TYPE_URI),
        lit("facebook_paid_ads"),
        lit(YOUTUBE_EVENT_TYPE_URI),
        lit("youtube_paid_ads"),
        lit(TIKTOK_EVENT_TYPE_URI),
        lit("tiktok_paid_ads"),
    )


def transform_bronze_calendly_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze Calendly webhook records into the Silver Calendly bookings table.

    Bronze grain:
        one raw Calendly webhook event per Bronze JSON record

    Silver grain:
        one booking / invitee-created event per row
    """
    flattened_df = bronze_df.select(
        col("source_system").alias("bronze_source_system"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("raw_s3_key").alias("bronze_raw_s3_key"),
        input_file_name().alias("bronze_source_file_path"),
        col("raw_event.event").alias("webhook_event"),
        col("raw_event.created_at").alias("webhook_created_at"),
        col("raw_event.created_by").alias("webhook_created_by"),
        col("raw_event.payload").alias("payload"),
    )

    event_type_channel_map = build_event_type_channel_map()

    selected_df = flattened_df.select(
        col("webhook_event"),
        col("webhook_created_at"),
        col("webhook_created_by"),
        col("payload.uri").alias("invitee_uri"),
        extract_id_from_uri(col("payload.uri")).alias("booking_id"),
        col("payload.created_at").alias("booking_created_at"),
        col("payload.updated_at").alias("booking_updated_at"),
        col("payload.email").alias("invitee_email"),
        col("payload.name").alias("invitee_name"),
        col("payload.status").alias("invitee_status"),
        col("payload.canceled").alias("invitee_canceled"),
        col("payload.cancel_reason").alias("invitee_cancel_reason"),
        col("payload.scheduled_event.uri").alias("scheduled_event_uri"),
        extract_id_from_uri(col("payload.scheduled_event.uri")).alias(
            "scheduled_event_id"
        ),
        col("payload.scheduled_event.name").alias("scheduled_event_name"),
        col("payload.scheduled_event.status").alias("scheduled_event_status"),
        col("payload.scheduled_event.event_type").alias("event_type_uri"),
        element_at(
            event_type_channel_map,
            col("payload.scheduled_event.event_type"),
        ).alias("channel"),
        col("payload.scheduled_event.start_time").alias("meeting_start_time"),
        col("payload.scheduled_event.end_time").alias("meeting_end_time"),
        col("payload.scheduled_event.location.type").alias("meeting_location_type"),
        col("payload.scheduled_event.location.location").alias("meeting_location"),
        col("payload.tracking.utm_source").alias("utm_source"),
        col("payload.tracking.utm_medium").alias("utm_medium"),
        col("payload.tracking.utm_campaign").alias("utm_campaign"),
        col("payload.tracking.utm_content").alias("utm_content"),
        col("payload.tracking.utm_term").alias("utm_term"),
        col("payload.tracking.salesforce_uuid").alias("salesforce_uuid"),
        (
            col("payload.scheduled_event.event_memberships")
            .getItem(0)
            .getField("user")
            .alias("employee_uri")
        ),
        (
            extract_id_from_uri(
                col("payload.scheduled_event.event_memberships")
                .getItem(0)
                .getField("user")
            ).alias("employee_id")
        ),
        (
            col("payload.scheduled_event.event_memberships")
            .getItem(0)
            .getField("user_name")
            .alias("employee_name")
        ),
        (
            col("payload.scheduled_event.event_memberships")
            .getItem(0)
            .getField("user_email")
            .alias("employee_email")
        ),
        col("bronze_source_system"),
        col("bronze_ingestion_timestamp"),
        col("bronze_raw_s3_key"),
        col("bronze_source_file_path"),
    )

    enriched_df = selected_df.select(
        col("webhook_event").cast(StringType()).alias("webhook_event"),
        col("webhook_created_at").cast(StringType()).alias("webhook_created_at"),
        col("webhook_created_by").cast(StringType()).alias("webhook_created_by"),
        col("booking_id").cast(StringType()).alias("booking_id"),
        col("invitee_uri").cast(StringType()).alias("invitee_uri"),
        col("booking_created_at").cast(StringType()).alias("booking_created_at"),
        to_date(to_timestamp(col("booking_created_at"))).alias("booking_date"),
        col("booking_updated_at").cast(StringType()).alias("booking_updated_at"),
        col("invitee_email").cast(StringType()).alias("invitee_email"),
        col("invitee_name").cast(StringType()).alias("invitee_name"),
        col("invitee_status").cast(StringType()).alias("invitee_status"),
        col("invitee_canceled").alias("invitee_canceled"),
        col("invitee_cancel_reason").cast(StringType()).alias("invitee_cancel_reason"),
        col("scheduled_event_id").cast(StringType()).alias("scheduled_event_id"),
        col("scheduled_event_uri").cast(StringType()).alias("scheduled_event_uri"),
        col("scheduled_event_name").cast(StringType()).alias("scheduled_event_name"),
        col("scheduled_event_status").cast(StringType()).alias(
            "scheduled_event_status"
        ),
        col("event_type_uri").cast(StringType()).alias("event_type_uri"),
        col("channel").cast(StringType()).alias("channel"),
        col("meeting_start_time").cast(StringType()).alias("meeting_start_time"),
        col("meeting_end_time").cast(StringType()).alias("meeting_end_time"),
        to_date(to_timestamp(col("meeting_start_time"))).alias("meeting_date"),
        date_format(to_timestamp(col("meeting_start_time")), "EEEE").alias(
            "meeting_day_of_week"
        ),
        date_format(to_timestamp(col("meeting_start_time")), "H")
        .cast(IntegerType())
        .alias("meeting_hour"),
        date_sub(
            to_date(to_timestamp(col("meeting_start_time"))),
            pmod(dayofweek(to_date(to_timestamp(col("meeting_start_time")))) + lit(5), lit(7)),
        ).alias("meeting_week_start_date"),
        col("meeting_location_type").cast(StringType()).alias("meeting_location_type"),
        col("meeting_location").cast(StringType()).alias("meeting_location"),
        col("utm_source").cast(StringType()).alias("utm_source"),
        col("utm_medium").cast(StringType()).alias("utm_medium"),
        col("utm_campaign").cast(StringType()).alias("utm_campaign"),
        col("utm_content").cast(StringType()).alias("utm_content"),
        col("utm_term").cast(StringType()).alias("utm_term"),
        col("salesforce_uuid").cast(StringType()).alias("salesforce_uuid"),
        col("employee_uri").cast(StringType()).alias("employee_uri"),
        col("employee_id").cast(StringType()).alias("employee_id"),
        col("employee_name").cast(StringType()).alias("employee_name"),
        col("employee_email").cast(StringType()).alias("employee_email"),
        col("bronze_source_system").cast(StringType()).alias("bronze_source_system"),
        col("bronze_ingestion_timestamp").cast(StringType()).alias(
            "bronze_ingestion_timestamp"
        ),
        col("bronze_raw_s3_key").cast(StringType()).alias("bronze_raw_s3_key"),
        col("bronze_source_file_path").cast(StringType()).alias(
            "bronze_source_file_path"
        ),
        current_timestamp().alias("silver_processed_at"),
    )

    valid_silver_df = enriched_df.where(
        (col("webhook_event") == lit("invitee.created"))
        & col("booking_id").isNotNull()
        & col("scheduled_event_id").isNotNull()
        & col("event_type_uri").isNotNull()
        & col("channel").isNotNull()
        & col("meeting_start_time").isNotNull()
        & col("meeting_date").isNotNull()
    )

    return valid_silver_df


def write_silver_calendly_delta(
    silver_df: DataFrame,
    silver_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Write the Silver Calendly bookings table in Delta format.

    Default mode is overwrite because this job is currently designed to be
    rerunnable from Bronze.
    """
    (
        silver_df.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .partitionBy("meeting_date")
        .save(silver_output_path)
    )


def run_job(
    bronze_input_path: str,
    silver_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Run the Bronze to Silver Calendly Glue job.
    """
    spark = create_spark_session()

    bronze_df = read_bronze_calendly_records(
        spark=spark,
        bronze_input_path=bronze_input_path,
    )

    silver_df = transform_bronze_calendly_to_silver(bronze_df)

    write_silver_calendly_delta(
        silver_df=silver_df,
        silver_output_path=silver_output_path,
        write_mode=write_mode,
    )


def main() -> None:
    args = parse_job_args(sys.argv[1:])

    run_job(
        bronze_input_path=args["bronze_input_path"],
        silver_output_path=args["silver_output_path"],
        write_mode=args["write_mode"],
    )


if __name__ == "__main__":
    main()