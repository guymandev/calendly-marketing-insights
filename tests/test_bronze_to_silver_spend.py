import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.transforms.bronze_to_silver_spend import (
    build_silver_spend_records,
    extract_raw_spend_payload,
    load_bronze_records_from_directory,
    transform_bronze_directory_to_silver_file,
    transform_bronze_spend_records,
)


def build_sample_raw_spend_payload() -> List[Dict[str, Any]]:
    return [
        {
            "date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend": 653.28,
        },
        {
            "date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "spend": 487.59,
        },
        {
            "date": "2026-07-29",
            "channel": "tiktok_paid_ads",
            "spend": 345.12,
        },
    ]


def build_sample_bronze_record() -> Dict[str, Any]:
    raw_payload = build_sample_raw_spend_payload()

    return {
        "source_system": "marketing_spend_public_s3",
        "ingestion_timestamp": "2026-07-30T18:44:30+00:00",
        "source_url": (
            "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
            "calendly_spend_data/spend_data_2026-07-29.json"
        ),
        "source_file_name": "spend_data_2026-07-29.json",
        "raw_s3_key": (
            "bronze/marketing_spend/"
            "ingest_date=2026-07-30/"
            "spend_data_2026-07-29.json"
        ),
        "raw_payload": raw_payload,
    }


def test_extract_raw_spend_payload_from_bronze_record():
    bronze_record = build_sample_bronze_record()

    raw_payload = extract_raw_spend_payload(bronze_record)

    assert raw_payload == build_sample_raw_spend_payload()


def test_extract_raw_spend_payload_accepts_direct_raw_spend_array():
    raw_payload = build_sample_raw_spend_payload()

    extracted_payload = extract_raw_spend_payload(raw_payload)  # type: ignore[arg-type]

    assert extracted_payload == raw_payload


def test_extract_raw_spend_payload_rejects_missing_raw_payload():
    bronze_record = {
        "source_system": "marketing_spend_public_s3",
        "ingestion_timestamp": "2026-07-30T18:44:30+00:00",
    }

    with pytest.raises(ValueError, match="missing raw_payload"):
        extract_raw_spend_payload(bronze_record)


def test_extract_raw_spend_payload_rejects_non_list_raw_payload():
    bronze_record = {
        "source_system": "marketing_spend_public_s3",
        "raw_payload": {
            "date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend": 653.28,
        },
    }

    with pytest.raises(ValueError, match="missing raw_payload list"):
        extract_raw_spend_payload(bronze_record)


def test_build_silver_spend_records_extracts_core_fields_and_lineage():
    bronze_record = build_sample_bronze_record()

    silver_records = build_silver_spend_records(
        bronze_record,
        source_file_path="local/test/spend_data_2026-07-29.json",
    )

    assert len(silver_records) == 3

    facebook_record = silver_records[0]

    assert facebook_record["spend_date"] == "2026-07-29"
    assert facebook_record["channel"] == "facebook_paid_ads"
    assert facebook_record["spend_usd"] == 653.28
    assert facebook_record["source_file"] == "spend_data_2026-07-29.json"

    assert facebook_record["bronze_source_system"] == "marketing_spend_public_s3"
    assert facebook_record["bronze_ingestion_timestamp"] == "2026-07-30T18:44:30+00:00"
    assert facebook_record["bronze_source_url"].endswith(
        "calendly_spend_data/spend_data_2026-07-29.json"
    )
    assert facebook_record["bronze_raw_s3_key"].startswith("bronze/marketing_spend/")
    assert (
        facebook_record["bronze_source_file_path"]
        == "local/test/spend_data_2026-07-29.json"
    )


def test_transform_bronze_spend_records_transforms_multiple_bronze_records():
    bronze_record = build_sample_bronze_record()

    silver_records = transform_bronze_spend_records(
        [bronze_record, bronze_record],
    )

    assert len(silver_records) == 6
    assert silver_records[0]["spend_date"] == "2026-07-29"
    assert silver_records[0]["channel"] == "facebook_paid_ads"
    assert silver_records[3]["spend_date"] == "2026-07-29"
    assert silver_records[3]["channel"] == "facebook_paid_ads"


def test_transform_bronze_spend_records_rejects_non_list_input():
    with pytest.raises(ValueError, match="must be a list"):
        transform_bronze_spend_records({"not": "a list"})  # type: ignore[arg-type]


def test_transform_bronze_spend_records_raises_for_invalid_record_by_default():
    invalid_record = {
        "source_system": "marketing_spend_public_s3",
        "raw_payload": [
            {
                "date": "2026-07-29",
                "channel": "facebook_paid_ads",
                "spend": -10.00,
            }
        ],
    }

    with pytest.raises(ValueError, match="Spend cannot be negative"):
        transform_bronze_spend_records([invalid_record])


def test_transform_bronze_spend_records_can_skip_invalid_records():
    valid_record = build_sample_bronze_record()
    invalid_record = {
        "source_system": "marketing_spend_public_s3",
        "raw_payload": [
            {
                "date": "2026-07-29",
                "channel": "facebook_paid_ads",
                "spend": -10.00,
            }
        ],
    }

    silver_records = transform_bronze_spend_records(
        [valid_record, invalid_record],
        skip_invalid=True,
    )

    assert len(silver_records) == 3
    assert silver_records[0]["channel"] == "facebook_paid_ads"


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
    output_path = tmp_path / "silver" / "silver_marketing_spend.json"

    input_dir.mkdir()
    (input_dir / "record.json").write_text(json.dumps(bronze_record), encoding="utf-8")

    silver_records = transform_bronze_directory_to_silver_file(
        bronze_input_path=input_dir,
        silver_output_path=output_path,
    )

    assert len(silver_records) == 3
    assert output_path.exists()

    written_output = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(written_output) == 3
    assert written_output[0]["spend_date"] == "2026-07-29"
    assert written_output[0]["channel"] == "facebook_paid_ads"
    assert written_output[0]["spend_usd"] == 653.28