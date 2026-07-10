from pathlib import Path
from typing import Any
import pytest

from src.parsers.marketing_spend_parser import (
    load_json_file,
    parse_spend_record,
    parse_spend_records,
)

SAMPLE_PATH = Path("sample_data/spend_data_sample.json")


def test_parse_spend_records_extracts_channels_from_sample_file():
    raw_records = load_json_file(SAMPLE_PATH)

    parsed = parse_spend_records(raw_records, source_file=str(SAMPLE_PATH))

    assert len(parsed) == 3

    channels = {record["channel"] for record in parsed}

    assert channels == {
        "facebook_paid_ads",
        "youtube_paid_ads",
        "tiktok_paid_ads",
    }


def test_parse_spend_records_allows_partial_channel_payload():
    raw_records = [
        {
            "date": "2025-06-24",
            "channel": "facebook_paid_ads",
            "spend": 653.28,
        }
    ]

    parsed = parse_spend_records(raw_records)

    assert len(parsed) == 1
    assert parsed[0]["channel"] == "facebook_paid_ads"
    assert parsed[0]["spend_usd"] == 653.28


def test_parse_spend_record_extracts_core_fields():
    raw_record = {
        "date": "2025-06-24",
        "channel": "facebook_paid_ads",
        "spend": 653.28,
    }

    parsed = parse_spend_record(raw_record, source_file="test_file.json")

    assert parsed["spend_date"] == "2025-06-24"
    assert parsed["channel"] == "facebook_paid_ads"
    assert parsed["spend_usd"] == 653.28
    assert parsed["source_file"] == "test_file.json"


def test_parse_spend_records_rejects_empty_payload():
    with pytest.raises(ValueError, match="payload is empty"):
        parse_spend_records([])


def test_parse_spend_records_rejects_non_array_payload():
    # Intentionally sending an invalid data type (i.e. not a list of dictionaries)
    bad_payload: Any = {"date": "2025-06-24"}

    with pytest.raises(ValueError, match="must be a JSON array"):
        parse_spend_records(bad_payload)


def test_parse_spend_record_rejects_invalid_channel():
    raw_record = {
        "date": "2025-06-24",
        "channel": "linkedin_paid_ads",
        "spend": 100.00,
    }

    with pytest.raises(ValueError, match="Unsupported marketing channel"):
        parse_spend_record(raw_record)


def test_parse_spend_record_rejects_negative_spend():
    raw_record = {
        "date": "2025-06-24",
        "channel": "facebook_paid_ads",
        "spend": -1.00,
    }

    with pytest.raises(ValueError, match="Spend cannot be negative"):
        parse_spend_record(raw_record)


def test_parse_spend_record_rejects_invalid_date_format():
    raw_record = {
        "date": "06/24/2025",
        "channel": "facebook_paid_ads",
        "spend": 653.28,
    }

    with pytest.raises(ValueError, match="Expected YYYY-MM-DD"):
        parse_spend_record(raw_record)