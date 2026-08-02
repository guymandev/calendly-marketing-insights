import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    current_timestamp,
    lit,
    round as spark_round,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import DecimalType, DoubleType, StringType


def parse_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Parse Glue-style command line arguments without requiring awsglue locally.

    Expected arguments:
        --silver_spend_input_path s3://.../silver/marketing_spend/
        --silver_bookings_input_path s3://.../silver/calendly_bookings/
        --gold_cpb_by_channel_output_path s3://.../gold/cpb_by_channel/
        --gold_daily_cpb_by_channel_output_path s3://.../gold/daily_cpb_by_channel/
        --gold_combined_kpis_output_path s3://.../gold/combined_dashboard_kpis/

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
        "silver_spend_input_path",
        "silver_bookings_input_path",
        "gold_cpb_by_channel_output_path",
        "gold_daily_cpb_by_channel_output_path",
        "gold_combined_kpis_output_path",
    ]

    missing_args = [name for name in required_args if not args.get(name)]

    if missing_args:
        raise ValueError(f"Missing required Glue job arguments: {missing_args}")

    args.setdefault("write_mode", "overwrite")

    return args


def create_spark_session(app_name: str = "silver-to-gold-combined") -> SparkSession:
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


def read_silver_spend_records(
    spark: SparkSession,
    silver_spend_input_path: str,
) -> DataFrame:
    """
    Read the Silver marketing spend Delta table.
    """
    return spark.read.format("delta").load(silver_spend_input_path)


def read_silver_booking_records(
    spark: SparkSession,
    silver_bookings_input_path: str,
) -> DataFrame:
    """
    Read the Silver Calendly bookings Delta table.
    """
    return spark.read.format("delta").load(silver_bookings_input_path)


def validate_silver_spend_columns(silver_spend_df: DataFrame) -> None:
    """
    Validate that the Silver spend DataFrame contains the minimum columns needed
    for combined Gold metrics.
    """
    required_columns = {
        "spend_date",
        "channel",
        "spend_usd",
    }

    available_columns = set(silver_spend_df.columns)
    missing_columns = sorted(required_columns - available_columns)

    if missing_columns:
        raise ValueError(f"Silver spend table is missing columns: {missing_columns}")


def validate_silver_booking_columns(silver_bookings_df: DataFrame) -> None:
    """
    Validate that the Silver bookings DataFrame contains the minimum columns needed
    for combined Gold metrics.
    """
    required_columns = {
        "booking_id",
        "booking_date",
        "channel",
    }

    available_columns = set(silver_bookings_df.columns)
    missing_columns = sorted(required_columns - available_columns)

    if missing_columns:
        raise ValueError(
            f"Silver bookings table is missing columns: {missing_columns}"
        )


def prepare_valid_silver_spend_records(silver_spend_df: DataFrame) -> DataFrame:
    """
    Keep only valid Silver spend rows needed for combined Gold metrics.
    """
    validate_silver_spend_columns(silver_spend_df)

    return (
        silver_spend_df.select(
            col("spend_date"),
            col("channel").cast(StringType()).alias("channel"),
            col("spend_usd").cast(DecimalType(18, 2)).alias("spend_usd"),
        )
        .where(
            col("spend_date").isNotNull()
            & col("channel").isNotNull()
            & col("spend_usd").isNotNull()
            & (col("spend_usd") >= 0)
        )
    )


def prepare_valid_silver_booking_records(silver_bookings_df: DataFrame) -> DataFrame:
    """
    Keep only valid Silver booking rows needed for combined Gold metrics.
    """
    validate_silver_booking_columns(silver_bookings_df)

    return (
        silver_bookings_df.select(
            col("booking_id").cast(StringType()).alias("booking_id"),
            col("booking_date"),
            col("channel").cast(StringType()).alias("channel"),
        )
        .where(
            col("booking_id").isNotNull()
            & col("booking_date").isNotNull()
            & col("channel").isNotNull()
        )
    )


def calculate_cost_per_booking_column(
    total_spend_column,
    booking_count_column,
):
    """
    Calculate cost per booking with divide-by-zero protection.

    Returns null when booking count is zero.
    """
    return (
        when(
            booking_count_column == 0,
            lit(None).cast(DoubleType()),
        )
        .otherwise(
            spark_round(
                total_spend_column / booking_count_column,
                2,
            ).cast(DoubleType())
        )
    )


def build_gold_cpb_by_channel(
    silver_spend_df: DataFrame,
    silver_bookings_df: DataFrame,
) -> DataFrame:
    """
    Build Gold cost-per-booking by channel.

    Output grain:
        one row per channel

    Join behavior:
        full outer join on channel, so channels with spend but no bookings
        and channels with bookings but no spend are retained.
    """
    valid_spend_df = prepare_valid_silver_spend_records(silver_spend_df)
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    spend_by_channel_df = (
        valid_spend_df.groupBy("channel")
        .agg(
            spark_sum("spend_usd").alias("total_spend_usd"),
        )
        .alias("spend")
    )

    bookings_by_channel_df = (
        valid_bookings_df.groupBy("channel")
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .alias("bookings")
    )

    joined_df = spend_by_channel_df.join(
        bookings_by_channel_df,
        on="channel",
        how="full_outer",
    )

    total_spend = coalesce(
        col("total_spend_usd"),
        lit(0).cast(DecimalType(18, 2)),
    )

    booking_count = coalesce(
        col("booked_call_count"),
        lit(0),
    )

    return joined_df.select(
        col("channel"),
        spark_round(total_spend, 2).cast(DoubleType()).alias("total_spend_usd"),
        booking_count.alias("booked_call_count"),
        calculate_cost_per_booking_column(
            total_spend_column=total_spend,
            booking_count_column=booking_count,
        ).alias("cost_per_booking_usd"),
        current_timestamp().alias("gold_processed_at"),
    )


def build_gold_daily_cpb_by_channel(
    silver_spend_df: DataFrame,
    silver_bookings_df: DataFrame,
) -> DataFrame:
    """
    Build Gold daily cost-per-booking by channel.

    Output grain:
        one row per metric_date + channel

    Join behavior:
        full outer join on metric_date + channel, where:
            spend.spend_date aligns with bookings.booking_date
    """
    valid_spend_df = prepare_valid_silver_spend_records(silver_spend_df)
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    spend_by_date_channel_df = (
        valid_spend_df.groupBy(
            col("spend_date").alias("metric_date"),
            col("channel"),
        )
        .agg(
            spark_sum("spend_usd").alias("total_spend_usd"),
        )
        .alias("spend")
    )

    bookings_by_date_channel_df = (
        valid_bookings_df.groupBy(
            col("booking_date").alias("metric_date"),
            col("channel"),
        )
        .agg(
            countDistinct("booking_id").alias("booked_call_count"),
        )
        .alias("bookings")
    )

    joined_df = spend_by_date_channel_df.join(
        bookings_by_date_channel_df,
        on=["metric_date", "channel"],
        how="full_outer",
    )

    total_spend = coalesce(
        col("total_spend_usd"),
        lit(0).cast(DecimalType(18, 2)),
    )

    booking_count = coalesce(
        col("booked_call_count"),
        lit(0),
    )

    return joined_df.select(
        col("metric_date"),
        col("channel"),
        spark_round(total_spend, 2).cast(DoubleType()).alias("total_spend_usd"),
        booking_count.alias("booked_call_count"),
        calculate_cost_per_booking_column(
            total_spend_column=total_spend,
            booking_count_column=booking_count,
        ).alias("cost_per_booking_usd"),
        current_timestamp().alias("gold_processed_at"),
    )


def build_gold_combined_dashboard_kpis(
    silver_spend_df: DataFrame,
    silver_bookings_df: DataFrame,
) -> DataFrame:
    """
    Build combined spend + booking dashboard KPI values.

    Output grain:
        one-row KPI table
    """
    valid_spend_df = prepare_valid_silver_spend_records(silver_spend_df)
    valid_bookings_df = prepare_valid_silver_booking_records(silver_bookings_df)

    spend_kpis_df = valid_spend_df.agg(
        spark_sum("spend_usd").alias("total_spend_usd"),
        countDistinct("spend_date").alias("spend_date_count"),
    )

    booking_kpis_df = valid_bookings_df.agg(
        countDistinct("booking_id").alias("total_bookings"),
        countDistinct("booking_date").alias("booking_date_count"),
    )

    channel_count_df = (
        valid_spend_df.select("channel")
        .union(valid_bookings_df.select("channel"))
        .distinct()
        .agg(countDistinct("channel").alias("channel_count"))
    )

    combined_df = spend_kpis_df.crossJoin(booking_kpis_df).crossJoin(channel_count_df)

    total_spend = coalesce(
        col("total_spend_usd"),
        lit(0).cast(DecimalType(18, 2)),
    )

    total_bookings = coalesce(
        col("total_bookings"),
        lit(0),
    )

    return combined_df.select(
        spark_round(total_spend, 2).cast(DoubleType()).alias("total_spend_usd"),
        total_bookings.alias("total_bookings"),
        calculate_cost_per_booking_column(
            total_spend_column=total_spend,
            booking_count_column=total_bookings,
        ).alias("average_cost_per_booking_usd"),
        coalesce(col("channel_count"), lit(0)).alias("channel_count"),
        coalesce(col("spend_date_count"), lit(0)).alias("spend_date_count"),
        coalesce(col("booking_date_count"), lit(0)).alias("booking_date_count"),
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
    silver_spend_input_path: str,
    silver_bookings_input_path: str,
    gold_cpb_by_channel_output_path: str,
    gold_daily_cpb_by_channel_output_path: str,
    gold_combined_kpis_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Run the combined Silver to Gold spend + bookings Glue job.
    """
    spark = create_spark_session()

    silver_spend_df = read_silver_spend_records(
        spark=spark,
        silver_spend_input_path=silver_spend_input_path,
    )

    silver_bookings_df = read_silver_booking_records(
        spark=spark,
        silver_bookings_input_path=silver_bookings_input_path,
    )

    gold_cpb_by_channel_df = build_gold_cpb_by_channel(
        silver_spend_df=silver_spend_df,
        silver_bookings_df=silver_bookings_df,
    )

    gold_daily_cpb_by_channel_df = build_gold_daily_cpb_by_channel(
        silver_spend_df=silver_spend_df,
        silver_bookings_df=silver_bookings_df,
    )

    gold_combined_kpis_df = build_gold_combined_dashboard_kpis(
        silver_spend_df=silver_spend_df,
        silver_bookings_df=silver_bookings_df,
    )

    write_delta_table(
        dataframe=gold_cpb_by_channel_df,
        output_path=gold_cpb_by_channel_output_path,
        write_mode=write_mode,
    )

    write_delta_table(
        dataframe=gold_daily_cpb_by_channel_df,
        output_path=gold_daily_cpb_by_channel_output_path,
        write_mode=write_mode,
        partition_columns=["metric_date"],
    )

    write_delta_table(
        dataframe=gold_combined_kpis_df,
        output_path=gold_combined_kpis_output_path,
        write_mode=write_mode,
    )


def main() -> None:
    args = parse_job_args(sys.argv[1:])

    run_job(
        silver_spend_input_path=args["silver_spend_input_path"],
        silver_bookings_input_path=args["silver_bookings_input_path"],
        gold_cpb_by_channel_output_path=args["gold_cpb_by_channel_output_path"],
        gold_daily_cpb_by_channel_output_path=args[
            "gold_daily_cpb_by_channel_output_path"
        ],
        gold_combined_kpis_output_path=args["gold_combined_kpis_output_path"],
        write_mode=args["write_mode"],
    )


if __name__ == "__main__":
    main()