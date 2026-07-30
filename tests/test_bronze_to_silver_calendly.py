import json
from pathlib import Path
from typing import Any, Dict

import pytest

from src.transforms.bronze_to_silver_calendly import (
    build_silver_calendly_record,
    extract_raw_calendly_event,
    load_bronze_records_from_directory,
    transform_bronze_calendly_records,
    transform_bronze_directory_to_silver_file,
)


SAMPLE_PATH = Path("sample_data/calendly_invitee_created_sample.json")


def load_sample_raw_calendly_event() -> Dict[str, Any]:
    with open(SAMPLE_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def build_sample_bronze_record() -> Dict[str, Any]:
    raw_event = load_sample_raw_calendly_event()

    return {
        "source_system": "calendly",
        "ingestion_timestamp": "2026-07-30T17:00:00+00:00",
        "webhook_event": "invitee.created",
        "booking_id": "22a0f2d6-1bde-4fc1-95c1-d969df1da21d",
        "event_type_uri": (
            "https://api.calendly.com/event_types/"
            "d639ecd3-8718-4068-955a-436b10d72c78"
        ),
        "channel": "facebook_paid_ads",
        "raw_s3_key": (
            "bronze/calendly_webhooks/"
            "event_type=invitee.created/"
            "ingest_date=2026-07-30/"
            "22a0f2d6-1bde-4fc1-95c1-d969df1da21d.json"
        ),
        "raw_event": raw_event,
    }


def test_extract_raw_calendly_event_from_bronze_record():
    bronze_record = build_sample_bronze_record()

    raw_event = extract_raw_calendly_event(bronze_record)

    assert raw_event["event"] == "invitee.created"
    assert "payload" in raw_event


def test_extract_raw_calendly_event_accepts_direct_raw_event():
    raw_event = load_sample_raw_calendly_event()

    extracted_event = extract_raw_calendly_event(raw_event)

    assert extracted_event == raw_event


def test_extract_raw_calendly_event_rejects_missing_raw_event():
    bronze_record = {
        "source_system": "calendly",
        "ingestion_timestamp": "2026-07-30T17:00:00+00:00",
    }

    with pytest.raises(ValueError, match="missing raw_event"):
        extract_raw_calendly_event(bronze_record)


def test_build_silver_calendly_record_extracts_booking_fields():
    bronze_record = build_sample_bronze_record()

    silver_record = build_silver_calendly_record(
        bronze_record,
        source_file_path="local/test/path.json",
    )

    assert silver_record["webhook_event"] == "invitee.created"
    assert silver_record["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert silver_record["scheduled_event_id"] == "1ac9e88e-eae3-4e4b-b979-d770cff02d72"
    assert silver_record["channel"] == "facebook_paid_ads"

    assert silver_record["meeting_date"] == "2025-07-09"
    assert silver_record["meeting_hour"] == 17
    assert silver_record["meeting_day_of_week"] == "Wednesday"
    assert silver_record["meeting_week_start_date"] == "2025-07-07"

    assert silver_record["bronze_source_system"] == "calendly"
    assert silver_record["bronze_ingestion_timestamp"] == "2026-07-30T17:00:00+00:00"
    assert silver_record["bronze_raw_s3_key"].startswith("bronze/calendly_webhooks/")
    assert silver_record["bronze_source_file_path"] == "local/test/path.json"


def test_transform_bronze_calendly_records_transforms_multiple_records():
    bronze_record = build_sample_bronze_record()

    silver_records = transform_bronze_calendly_records(
        [bronze_record, bronze_record],
    )

    assert len(silver_records) == 2
    assert silver_records[0]["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert silver_records[1]["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"


def test_transform_bronze_calendly_records_rejects_non_list_input():
    with pytest.raises(ValueError, match="must be a list"):
        transform_bronze_calendly_records({"not": "a list"})  # type: ignore[arg-type]


def test_transform_bronze_calendly_records_raises_for_invalid_record_by_default():
    invalid_record = {
        "source_system": "calendly",
        "raw_event": {
            "event": "invitee.created",
            "payload": {},
        },
    }

    with pytest.raises(ValueError, match="payload is missing or empty"):
        transform_bronze_calendly_records([invalid_record])


def test_transform_bronze_calendly_records_can_skip_invalid_records():
    valid_record = build_sample_bronze_record()
    invalid_record = {
        "source_system": "calendly",
        "raw_event": {
            "event": "invitee.created",
            "payload": {},
        },
    }

    silver_records = transform_bronze_calendly_records(
        [valid_record, invalid_record],
        skip_invalid=True,
    )

    assert len(silver_records) == 1
    assert silver_records[0]["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"


def test_load_bronze_records_from_directory_loads_json_files(tmp_path):
    bronze_record = build_sample_bronze_record()

    input_dir = tmp_path / "bronze"
    input_dir.mkdir()

    file_path = input_dir / "record.json"
    file_path.write_text(json.dumps(bronze_record), encoding="utf-8")

    loaded_records = load_bronze_records_from_directory(input_dir)

    assert loaded_records == [bronze_record]


def test_transform_bronze_directory_to_silver_file_writes_output(tmp_path):
    bronze_record = build_sample_bronze_record()

    input_dir = tmp_path / "bronze"
    output_path = tmp_path / "silver" / "silver_calendly_bookings.json"

    input_dir.mkdir()
    (input_dir / "record.json").write_text(json.dumps(bronze_record), encoding="utf-8")

    silver_records = transform_bronze_directory_to_silver_file(
        bronze_input_path=input_dir,
        silver_output_path=output_path,
    )

    assert len(silver_records) == 1
    assert output_path.exists()

    written_output = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(written_output) == 1
    assert written_output[0]["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert written_output[0]["channel"] == "facebook_paid_ads"