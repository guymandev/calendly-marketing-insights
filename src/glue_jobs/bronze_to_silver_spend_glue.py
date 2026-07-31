import sys
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    input_file_name,
    lit,
    round as spark_round,
    to_date,
)
from pyspark.sql.types import DoubleType, StringType


VALID_CHANNELS = [
    "facebook_paid_ads",
    "youtube_paid_ads",
    "tiktok_paid_ads",
]


def parse_job_args(argv: List[str]) -> Dict[str, str]:
    """
    Parse Glue-style command line arguments without requiring awsglue locally.

    Expected arguments:
        --bronze_input_path s3://.../bronze/marketing_spend/
        --silver_output_path s3://.../silver/marketing_spend/

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


def create_spark_session(app_name: str = "bronze-to-silver-spend") -> SparkSession:
    """
    Create a SparkSession configured for Delta Lake.

    In AWS Glue, the job should also be configured with Delta Lake support.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_bronze_spend_records(
    spark: SparkSession,
    bronze_input_path: str,
) -> DataFrame:
    """
    Read Bronze marketing spend JSON files from S3.

    The Bronze records were written by the marketing-spend-ingest Lambda and have
    this shape:

        {
          "source_system": "marketing_spend_public_s3",
          "ingestion_timestamp": "...",
          "source_url": "...",
          "source_file_name": "spend_data_YYYY-MM-DD.json",
          "raw_s3_key": "bronze/marketing_spend/...",
          "raw_payload": [
            {"date": "YYYY-MM-DD", "channel": "...", "spend": 123.45}
          ]
        }
    """
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_input_path)
    )


def transform_bronze_spend_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze marketing spend records into the Silver marketing spend table.

    Bronze grain:
        one ingested source spend file per Bronze JSON record

    Silver grain:
        one spend_date + channel record per row
    """
    exploded_df = bronze_df.select(
        explode(col("raw_payload")).alias("spend_record"),
        col("source_system").alias("bronze_source_system"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("source_url").alias("bronze_source_url"),
        col("source_file_name").alias("source_file"),
        col("raw_s3_key").alias("bronze_raw_s3_key"),
        input_file_name().alias("bronze_source_file_path"),
    )

    silver_df = exploded_df.select(
        to_date(col("spend_record.date")).alias("spend_date"),
        col("spend_record.channel").cast(StringType()).alias("channel"),
        spark_round(
            col("spend_record.spend").cast(DoubleType()),
            2,
        ).alias("spend_usd"),
        col("source_file").cast(StringType()).alias("source_file"),
        col("bronze_source_system").cast(StringType()).alias("bronze_source_system"),
        col("bronze_ingestion_timestamp").cast(StringType()).alias(
            "bronze_ingestion_timestamp"
        ),
        col("bronze_source_url").cast(StringType()).alias("bronze_source_url"),
        col("bronze_raw_s3_key").cast(StringType()).alias("bronze_raw_s3_key"),
        col("bronze_source_file_path").cast(StringType()).alias(
            "bronze_source_file_path"
        ),
        current_timestamp().alias("silver_processed_at"),
    )

    valid_silver_df = silver_df.where(
        col("spend_date").isNotNull()
        & col("channel").isin(VALID_CHANNELS)
        & col("spend_usd").isNotNull()
        & (col("spend_usd") >= lit(0.0))
    )

    return valid_silver_df


def write_silver_spend_delta(
    silver_df: DataFrame,
    silver_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Write the Silver marketing spend table in Delta format.

    Default mode is overwrite because this job is currently designed to be
    rerunnable from Bronze.
    """
    (
        silver_df.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .partitionBy("spend_date")
        .save(silver_output_path)
    )


def run_job(
    bronze_input_path: str,
    silver_output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Run the Bronze to Silver marketing spend Glue job.
    """
    spark = create_spark_session()

    bronze_df = read_bronze_spend_records(
        spark=spark,
        bronze_input_path=bronze_input_path,
    )

    silver_df = transform_bronze_spend_to_silver(bronze_df)

    write_silver_spend_delta(
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