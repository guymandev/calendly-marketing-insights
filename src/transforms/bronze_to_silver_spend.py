import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.parsers.marketing_spend_parser import parse_spend_records


def load_json_file(path: str | Path) -> Any:
    """
    Load a JSON file from disk.
    """
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def write_json_file(data: Any, path: str | Path) -> None:
    """
    Write JSON data to disk with stable formatting.
    """
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, sort_keys=True)


def extract_raw_spend_payload(bronze_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract the raw marketing spend payload from a Bronze record.

    The marketing spend Lambda writes Bronze records in this shape:

        {
            "source_system": "marketing_spend_public_s3",
            "ingestion_timestamp": "...",
            "source_url": "...",
            "source_file_name": "spend_data_YYYY-MM-DD.json",
            "raw_s3_key": "...",
            "raw_payload": [
                {"date": "...", "channel": "...", "spend": ...}
            ]
        }

    This function also supports raw spend arrays directly, which makes local
    testing and future replay utilities easier.
    """
    if isinstance(bronze_record, list):
        return bronze_record

    if not isinstance(bronze_record, dict):
        raise ValueError("Bronze marketing spend record must be a JSON object.")

    raw_payload = bronze_record.get("raw_payload")

    if not isinstance(raw_payload, list):
        raise ValueError("Bronze marketing spend record is missing raw_payload list.")

    return raw_payload


def build_silver_spend_records(
    bronze_record: Dict[str, Any],
    source_file_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convert one Bronze marketing spend record into Silver spend records.

    One Bronze file may contain multiple spend rows, so this function returns
    a list of Silver records.
    """
    raw_payload = extract_raw_spend_payload(bronze_record)

    source_file_name = bronze_record.get("source_file_name")
    parsed_records = parse_spend_records(
        raw_payload,
        source_file=source_file_name,
    )

    silver_records: List[Dict[str, Any]] = []

    for parsed_record in parsed_records:
        silver_records.append(
            {
                **parsed_record,
                "bronze_source_system": bronze_record.get(
                    "source_system",
                    "marketing_spend_public_s3",
                ),
                "bronze_ingestion_timestamp": bronze_record.get("ingestion_timestamp"),
                "bronze_source_url": bronze_record.get("source_url"),
                "bronze_raw_s3_key": bronze_record.get("raw_s3_key"),
                "bronze_source_file_path": source_file_path,
            }
        )

    return silver_records


def transform_bronze_spend_records(
    bronze_records: List[Dict[str, Any]],
    skip_invalid: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transform a list of Bronze marketing spend records into Silver spend records.

    If skip_invalid is False, the first invalid record raises an error.
    If skip_invalid is True, invalid records are skipped.
    """
    if not isinstance(bronze_records, list):
        raise ValueError("Bronze marketing spend input must be a list of records.")

    silver_records: List[Dict[str, Any]] = []

    for bronze_record in bronze_records:
        try:
            silver_records.extend(build_silver_spend_records(bronze_record))
        except ValueError:
            if not skip_invalid:
                raise

    return silver_records


def load_bronze_records_from_directory(path: str | Path) -> List[Dict[str, Any]]:
    """
    Load all JSON Bronze records from a local directory.

    This is mainly for local development/testing. In AWS Glue, the equivalent
    input will be S3 Bronze JSON files.
    """
    input_path = Path(path)

    if not input_path.exists():
        raise FileNotFoundError(f"Bronze input path does not exist: {input_path}")

    if not input_path.is_dir():
        raise ValueError(f"Bronze input path must be a directory: {input_path}")

    records: List[Dict[str, Any]] = []

    for json_path in sorted(input_path.rglob("*.json")):
        record = load_json_file(json_path)

        if not isinstance(record, dict):
            raise ValueError(f"Bronze file must contain a JSON object: {json_path}")

        records.append(record)

    return records


def transform_bronze_directory_to_silver_file(
    bronze_input_path: str | Path,
    silver_output_path: str | Path,
    skip_invalid: bool = False,
) -> List[Dict[str, Any]]:
    """
    Local utility to transform a directory of Bronze marketing spend JSON files
    into a Silver JSON file.

    This is not the final Glue/Delta implementation. It is a local, testable
    version of the same transformation logic.
    """
    bronze_records = load_bronze_records_from_directory(bronze_input_path)
    silver_records = transform_bronze_spend_records(
        bronze_records=bronze_records,
        skip_invalid=skip_invalid,
    )

    write_json_file(silver_records, silver_output_path)

    return silver_records