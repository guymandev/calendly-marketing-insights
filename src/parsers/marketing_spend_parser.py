import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


VALID_CHANNELS = {
    "facebook_paid_ads",
    "youtube_paid_ads",
    "tiktok_paid_ads",
}


def load_json_file(path: str | Path) -> Any:
    """
    Load a JSON file from disk.
    """
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def validate_date_string(date_value: Optional[str]) -> str:
    """
    Validate that a date string is present and formatted as YYYY-MM-DD.
    """
    if not date_value:
        raise ValueError("Spend record is missing date.")

    try:
        datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid spend date format: {date_value}. Expected YYYY-MM-DD."
        ) from exc

    return date_value


def validate_channel(channel: Optional[str]) -> str:
    """
    Validate that the spend channel is one of the supported marketing channels.
    """
    if not channel:
        raise ValueError("Spend record is missing channel.")

    if channel not in VALID_CHANNELS:
        raise ValueError(f"Unsupported marketing channel: {channel}")

    return channel


def validate_spend(spend: Any) -> float:
    """
    Validate and normalize spend as a non-negative float.
    """
    if spend is None:
        raise ValueError("Spend record is missing spend.")

    try:
        spend_float = float(spend)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid spend value: {spend}") from exc

    if spend_float < 0:
        raise ValueError(f"Spend cannot be negative: {spend_float}")

    return round(spend_float, 2)


def parse_spend_record(record: Dict[str, Any], source_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse and validate one marketing spend record into a Silver-friendly shape.
    """
    if not isinstance(record, dict):
        raise ValueError("Spend record must be a JSON object.")

    spend_date = validate_date_string(record.get("date"))
    channel = validate_channel(record.get("channel"))
    spend_usd = validate_spend(record.get("spend"))

    return {
        "spend_date": spend_date,
        "channel": channel,
        "spend_usd": spend_usd,
        "source_file": source_file,
    }


def parse_spend_records(records: List[Dict[str, Any]], source_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Parse a list of marketing spend records.
    """
    if not isinstance(records, list):
        raise ValueError("Marketing spend payload must be a JSON array.")

    if not records:
        raise ValueError("Marketing spend payload is empty.")

    return [
        parse_spend_record(record, source_file=source_file)
        for record in records
    ]


if __name__ == "__main__":
    sample_path = Path("sample_data/spend_data_sample.json")
    raw_records = load_json_file(sample_path)

    parsed_records = parse_spend_records(
        raw_records,
        source_file=str(sample_path),
    )

    print(json.dumps(parsed_records, indent=2))