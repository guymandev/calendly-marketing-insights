import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    current_timestamp,
    lit,
)
from pyspark.sql.types import IntegerType, StringType


def parse_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Parse Glue-style command line arguments without requiring awsglue locally.

    Expected arguments:
        --silver_bookings_input_path s3://.../silver/calendly_bookings/
        --gold_daily_calls_output_path s3://.../gold/daily_calls_by_source/
        --gold_booking_trends_output_path s3://.../gold/booking_trends/
        --gold_channel_attribution_output_path s3://.../gold/channel_attribution/
        --gold_time_slot_output_path s3://.../gold/booking_volume_by_time_slot/
        --gold_employee_load_output_path s3://.../gold/employee_meeting_load/
        --gold_booking_kpis_output_path s3://.../gold/booking_dashboard_kpis/

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
        "silver_bookings_input_path",
        "gold_daily_calls_output_path",
        "gold_booking_trends_output_path",
        "gold_channel_attribution_output_path",
        "gold_time_slot_output_path",
        "gold_employee_load_output_path",
        "gold_booking_kpis_output_path",
    ]

    missing_args = [name for name in required_args if not args.get(name)]

    if missing_args:
        raise ValueError(f"Missing required Glue job arguments: {missing_args}")

    args.setdefault("write_mode", "overwrite")

    return args


def create_spark_session(app_name: str = "silver-to-gold-bookings") -> SparkSession:
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


def read_silver_booking_records(
    spark: SparkSession,
    silver_bookings_input_path: str,
) -> DataFrame:
    """
    Read the Silver Calendly bookings Delta table.

    Expected Silver grain:
        one row per invitee.created booking event
    """
    return spark.read.format("delta").load(silver_bookings_input_path)


def validate_silver_booking_columns(silver_bookings_df: DataFrame) -> None:
    """
    Validate that the Silver bookings DataFrame contains the minimum columns needed
    to produce booking-only Gold tables.
    """
    required_columns = {
        "booking_id",
        "channel",
        "booking_date",
        "meeting_date",
        "meeting_day_of_week",
        "meeting_hour",
    }

    available_columns = set(silver_bookings_df.columns)
    missing_columns = sorted(required_columns - available_columns)

    if missing_columns:
        raise ValueError(f"Silver bookings table is missing columns: {missing_columns}")


def prepare_valid_silver_booking_records(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Keep only valid Silver booking rows needed for Gold aggregation.
    """
    validate_silver_booking_columns(silver_bookings_df)

    optional_columns = set(silver_bookings_df.columns)

    selected_columns = [
        col("booking_id").cast(StringType()).alias("booking_id"),
        col("channel").cast(StringType()).alias("channel"),
        col("booking_date"),
        col("meeting_date"),
        col("meeting_day_of_week").cast(StringType()).alias("meeting_day_of_week"),
        col("meeting_hour").cast(IntegerType()).alias("meeting_hour"),
    ]

    if "utm_campaign" in optional_columns:
        selected_columns.append(
            coalesce(col("utm_campaign"), lit("unknown_campaign")).alias("utm_campaign")
        )
    else:
        selected_columns.append(lit("unknown_campaign").alias("utm_campaign"))

    if "employee_id" in optional_columns:
        selected_columns.append(
            coalesce(col("employee_id"), lit("unknown_employee")).alias("employee_id")
        )
    else:
        selected_columns.append(lit("unknown_employee").alias("employee_id"))

    if "employee_name" in optional_columns:
        selected_columns.append(
            coalesce(col("employee_name"), lit("Unknown Employee")).alias(
                "employee_name"
            )
        )
    else:
        selected_columns.append(lit("Unknown Employee").alias("employee_name"))

    if "employee_email" in optional_columns:
        selected_columns.append(
            coalesce(col("employee_email"), lit("unknown_email")).alias(
                "employee_email"
            )
        )
    else:
        selected_columns.append(lit("unknown_email").alias("employee_email"))

    return (
        silver_bookings_df.select(*selected_columns)
        .where(
            col("booking_id").isNotNull()
            & col("channel").isNotNull()
            & col("booking_date").isNotNull()
            & col("meeting_date").isNotNull()
            & col("meeting_day_of_week").isNotNull()
            & col("meeting_hour").isNotNull()
            & (col("meeting_hour") >= 0)
            & (col("meeting_hour") <= 23)
        )
    )


def build_gold_daily_calls_by_source(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build Gold daily calls booked by source/channel.

    Output grain:
        one row per booking_date + channel
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return (
        valid_bookings_df.groupBy(
            "booking_date",
            "channel",
        )
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .select(
            col("booking_date"),
            col("channel"),
            col("booked_call_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_booking_trends(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build Gold booking trend over time.

    Output grain:
        one row per booking_date
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return (
        valid_bookings_df.groupBy("booking_date")
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .select(
            col("booking_date"),
            col("booked_call_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_channel_attribution(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build Gold channel attribution.

    Output grain:
        one row per channel + utm_campaign
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return (
        valid_bookings_df.groupBy(
            "channel",
            "utm_campaign",
        )
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .select(
            col("channel"),
            col("utm_campaign"),
            col("booked_call_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_booking_volume_by_time_slot(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build Gold booking volume by day of week and meeting hour.

    Output grain:
        one row per meeting_day_of_week + meeting_hour
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return (
        valid_bookings_df.groupBy(
            "meeting_day_of_week",
            "meeting_hour",
        )
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .select(
            col("meeting_day_of_week"),
            col("meeting_hour"),
            col("booked_call_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_employee_meeting_load(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build Gold meeting load by employee.

    Output grain:
        one row per employee
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return (
        valid_bookings_df.groupBy(
            "employee_id",
            "employee_name",
            "employee_email",
        )
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .select(
            col("employee_id"),
            col("employee_name"),
            col("employee_email"),
            col("booked_call_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_booking_dashboard_kpis(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Build booking-only dashboard KPI values.

    Output grain:
        one-row KPI table
    """
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    return valid_bookings_df.agg(
        countDistinct("booking_id").alias("total_bookings"),
        countDistinct("channel").alias("channel_count"),
        countDistinct("booking_date").alias("booking_date_count"),
        countDistinct("meeting_date").alias("meeting_date_count"),
    ).select(
        col("total_bookings"),
        col("channel_count"),
        col("booking_date_count"),
        col("meeting_date_count"),
        current_timestamp().alias("gold_processed_at"),
    )


def write_delta_table(
    dataframe: DataFrame,
    output_path: str,
    write_mode: str = "overwrite",
    partition_columns: List[str] | None = None,
) -> None:
    """
    Write a DataFrame as a Delta table.
    """
    writer = (
        dataframe.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(output_path)


def run_job(
    silver_bookings_input_path: str,
    gold_daily_calls_output_path: str,
    gold_booking_trends_output_path: str,
    gold_channel_attribution_output_path: str,
    gold_time_slot_output_path: str,
    gold_employee_load_output_path: str,
    gold_booking_kpis_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Run the Silver to Gold Calendly bookings Glue job.
    """
    spark = create_spark_session()

    silver_bookings_df = read_silver_booking_records(
        spark=spark,
        silver_bookings_input_path=silver_bookings_input_path,
    )

    gold_daily_calls_df = build_gold_daily_calls_by_source(silver_bookings_df)
    gold_booking_trends_df = build_gold_booking_trends(silver_bookings_df)
    gold_channel_attribution_df = build_gold_channel_attribution(silver_bookings_df)
    gold_time_slot_df = build_gold_booking_volume_by_time_slot(silver_bookings_df)
    gold_employee_load_df = build_gold_employee_meeting_load(silver_bookings_df)
    gold_booking_kpis_df = build_gold_booking_dashboard_kpis(silver_bookings_df)

    write_delta_table(
        dataframe=gold_daily_calls_df,
        output_path=gold_daily_calls_output_path,
        write_mode=write_mode,
        partition_columns=["booking_date"],
    )

    write_delta_table(
        dataframe=gold_booking_trends_df,
        output_path=gold_booking_trends_output_path,
        write_mode=write_mode,
        partition_columns=["booking_date"],
    )

    write_delta_table(
        dataframe=gold_channel_attribution_df,
        output_path=gold_channel_attribution_output_path,
        write_mode=write_mode,
    )

    write_delta_table(
        dataframe=gold_time_slot_df,
        output_path=gold_time_slot_output_path,
        write_mode=write_mode,
    )

    write_delta_table(
        dataframe=gold_employee_load_df,
        output_path=gold_employee_load_output_path,
        write_mode=write_mode,
    )

    write_delta_table(
        dataframe=gold_booking_kpis_df,
        output_path=gold_booking_kpis_output_path,
        write_mode=write_mode,
    )


def main() -> None:
    args = parse_job_args(sys.argv[1:])

    run_job(
        silver_bookings_input_path=args["silver_bookings_input_path"],
        gold_daily_calls_output_path=args["gold_daily_calls_output_path"],
        gold_booking_trends_output_path=args["gold_booking_trends_output_path"],
        gold_channel_attribution_output_path=args[
            "gold_channel_attribution_output_path"
        ],
        gold_time_slot_output_path=args["gold_time_slot_output_path"],
        gold_employee_load_output_path=args["gold_employee_load_output_path"],
        gold_booking_kpis_output_path=args["gold_booking_kpis_output_path"],
        write_mode=args["write_mode"],
    )


if __name__ == "__main__":
    main()