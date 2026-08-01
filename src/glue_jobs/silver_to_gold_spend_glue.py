import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.types import DecimalType, DoubleType, StringType


def parse_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Parse Glue-style command line arguments without requiring awsglue locally.

    Expected arguments:
        --silver_spend_input_path s3://.../silver/marketing_spend/
        --gold_daily_spend_output_path s3://.../gold/daily_spend_by_channel/
        --gold_channel_summary_output_path s3://.../gold/channel_spend_summary/

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
        "gold_daily_spend_output_path",
        "gold_channel_summary_output_path",
    ]

    missing_args = [name for name in required_args if not args.get(name)]

    if missing_args:
        raise ValueError(f"Missing required Glue job arguments: {missing_args}")

    args.setdefault("write_mode", "overwrite")

    return args


def create_spark_session(app_name: str = "silver-to-gold-spend") -> SparkSession:
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

    Expected Silver grain:
        one row per spend_date + channel spend record
    """
    return spark.read.format("delta").load(silver_spend_input_path)


def validate_silver_spend_columns(silver_spend_df: DataFrame) -> None:
    """
    Validate that the Silver spend DataFrame contains the minimum columns needed
    to produce spend-only Gold tables.
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


def prepare_valid_silver_spend_records(silver_spend_df: DataFrame) -> DataFrame:
    """
    Keep only valid Silver spend rows needed for Gold aggregation.
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


def build_gold_daily_spend_by_channel(silver_spend_df: DataFrame) -> DataFrame:
    """
    Build Gold daily spend by channel.

    Output grain:
        one row per spend_date + channel
    """
    valid_spend_df = prepare_valid_silver_spend_records(silver_spend_df)

    return (
        valid_spend_df.groupBy(
            "spend_date",
            "channel",
        )
        .agg(
            spark_round(spark_sum("spend_usd"), 2).alias("total_spend_usd"),
        )
        .select(
            col("spend_date"),
            col("channel"),
            col("total_spend_usd").cast(DoubleType()).alias("total_spend_usd"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_gold_channel_spend_summary(silver_spend_df: DataFrame) -> DataFrame:
    """
    Build Gold channel-level spend summary.

    Output grain:
        one row per channel
    """
    valid_spend_df = prepare_valid_silver_spend_records(silver_spend_df)

    daily_spend_df = (
        valid_spend_df.groupBy(
            "spend_date",
            "channel",
        )
        .agg(
            spark_sum("spend_usd").alias("daily_spend_usd"),
        )
    )

    channel_summary_df = (
        daily_spend_df.groupBy("channel")
        .agg(
            spark_sum("daily_spend_usd").alias("total_spend_usd"),
            countDistinct("spend_date").alias("spend_day_count"),
        )
    )

    return channel_summary_df.select(
        col("channel"),
        spark_round(col("total_spend_usd"), 2)
        .cast(DoubleType())
        .alias("total_spend_usd"),
        col("spend_day_count"),
        spark_round(
            col("total_spend_usd") / col("spend_day_count"),
            2,
        )
        .cast(DoubleType())
        .alias("average_daily_spend_usd"),
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
    gold_daily_spend_output_path: str,
    gold_channel_summary_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Run the Silver to Gold marketing spend Glue job.
    """
    spark = create_spark_session()

    silver_spend_df = read_silver_spend_records(
        spark=spark,
        silver_spend_input_path=silver_spend_input_path,
    )

    gold_daily_spend_df = build_gold_daily_spend_by_channel(silver_spend_df)
    gold_channel_summary_df = build_gold_channel_spend_summary(silver_spend_df)

    write_delta_table(
        dataframe=gold_daily_spend_df,
        output_path=gold_daily_spend_output_path,
        write_mode=write_mode,
        partition_columns=["spend_date"],
    )

    write_delta_table(
        dataframe=gold_channel_summary_df,
        output_path=gold_channel_summary_output_path,
        write_mode=write_mode,
    )


def main() -> None:
    args = parse_job_args(sys.argv[1:])

    run_job(
        silver_spend_input_path=args["silver_spend_input_path"],
        gold_daily_spend_output_path=args["gold_daily_spend_output_path"],
        gold_channel_summary_output_path=args["gold_channel_summary_output_path"],
        write_mode=args["write_mode"],
    )


if __name__ == "__main__":
    main()