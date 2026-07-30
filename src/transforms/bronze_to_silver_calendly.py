import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.parsers.calendly_webhook_parser import parse_invitee_created_webhook


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


def extract_raw_calendly_event(bronze_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the raw Calendly webhook event from a Bronze record.

    The Lambda writes Bronze records in this shape:

        {
            "source_system": "calendly",
            "ingestion_timestamp": "...",
            "raw_s3_key": "...",
            "raw_event": { original Calendly webhook payload }
        }

    This function also supports raw Calendly webhook JSON directly, which makes
    local testing and future replay utilities easier.
    """
    if not isinstance(bronze_record, dict):
        raise ValueError("Bronze Calendly record must be a JSON object.")

    raw_event = bronze_record.get("raw_event")

    if isinstance(raw_event, dict):
        return raw_event

    # Fallback: allow direct raw Calendly webhook events.
    if bronze_record.get("event") == "invitee.created" and "payload" in bronze_record:
        return bronze_record

    raise ValueError("Bronze Calendly record is missing raw_event payload.")


def build_silver_calendly_record(
    bronze_record: Dict[str, Any],
    source_file_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert one Bronze Calendly webhook record into one Silver Calendly booking record.

    The parser handles Calendly-specific flattening. This transform adds Bronze
    lineage metadata needed for traceability and reload support.
    """
    raw_event = extract_raw_calendly_event(bronze_record)
    parsed_booking = parse_invitee_created_webhook(raw_event)

    silver_record = {
        **parsed_booking,
        "bronze_source_system": bronze_record.get("source_system", "calendly"),
        "bronze_ingestion_timestamp": bronze_record.get("ingestion_timestamp"),
        "bronze_raw_s3_key": bronze_record.get("raw_s3_key"),
        "bronze_source_file_path": source_file_path,
    }

    return silver_record


def transform_bronze_calendly_records(
    bronze_records: List[Dict[str, Any]],
    skip_invalid: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transform a list of Bronze Calendly records into Silver Calendly booking records.

    If skip_invalid is False, the first invalid record raises an error.
    If skip_invalid is True, invalid records are skipped.
    """
    if not isinstance(bronze_records, list):
        raise ValueError("Bronze Calendly input must be a list of records.")

    silver_records: List[Dict[str, Any]] = []

    for bronze_record in bronze_records:
        try:
            silver_records.append(build_silver_calendly_record(bronze_record))
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
    Local utility to transform a directory of Bronze Calendly JSON files into
    a Silver JSON file.

    This is not the final Glue/Delta implementation. It is a local, testable
    version of the same transformation logic.
    """
    bronze_records = load_bronze_records_from_directory(bronze_input_path)
    silver_records = transform_bronze_calendly_records(
        bronze_records=bronze_records,
        skip_invalid=skip_invalid,
    )

    write_json_file(silver_records, silver_output_path)

    return silver_records