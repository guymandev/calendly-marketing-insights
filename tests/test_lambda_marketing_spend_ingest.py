import json
from typing import Any, Dict, List

import pytest

from src.lambda_marketing_spend_ingest import app


class FakeS3Client:
    """
    Simple fake S3 client for unit tests.
    """

    def __init__(self):
        self.put_object_calls: List[Dict[str, Any]] = []

    def put_object(self, **kwargs):
        self.put_object_calls.append(kwargs)

        return {
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            }
        }


def test_normalize_file_index_accepts_list_of_file_names():

    payload = [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
    ]

    result = app.normalize_file_index(payload)

    assert result == [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
    ]


def test_normalize_file_index_accepts_dict_with_files():

    payload = {
        "files": [
            "spend_data_2025-07-09.json",
            "spend_data_2025-07-10.json",
        ]
    }

    result = app.normalize_file_index(payload)

    assert result == [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
    ]


def test_normalize_file_index_accepts_list_of_objects_with_keys():

    payload = [
        {"key": "calendly_spend_data/spend_data_2025-07-09.json"},
        {"key": "calendly_spend_data/spend_data_2025-07-10.json"},
    ]

    result = app.normalize_file_index(payload)

    assert result == [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
    ]


def test_normalize_file_index_rejects_missing_file_list():

    payload = {
        "unexpected": []
    }

    with pytest.raises(ValueError, match="must contain a list"):
        app.normalize_file_index(payload)


def test_normalize_file_index_rejects_when_no_spend_files_found():

    payload = [
        "not_a_spend_file.json",
        "notes.txt",
    ]

    with pytest.raises(ValueError, match="No spend_data_YYYY-MM-DD"):
        app.normalize_file_index(payload)


def test_filter_file_names_from_event_filters_single_file():

    file_names = [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
    ]

    result = app.filter_file_names_from_event(
        file_names,
        {"file_name": "spend_data_2025-07-10.json"},
    )

    assert result == ["spend_data_2025-07-10.json"]


def test_filter_file_names_from_event_filters_multiple_files():

    file_names = [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
        "spend_data_2025-07-11.json",
    ]

    result = app.filter_file_names_from_event(
        file_names,
        {
            "file_names": [
                "spend_data_2025-07-09.json",
                "spend_data_2025-07-11.json",
            ]
        },
    )

    assert result == [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-11.json",
    ]


def test_filter_file_names_from_event_uses_max_files():

    file_names = [
        "spend_data_2025-07-09.json",
        "spend_data_2025-07-10.json",
        "spend_data_2025-07-11.json",
    ]

    result = app.filter_file_names_from_event(
        file_names,
        {"max_files": 2},
    )

    assert result == [
        "spend_data_2025-07-10.json",
        "spend_data_2025-07-11.json",
    ]


def test_build_source_url_handles_trailing_slash():

    result = app.build_source_url(
        "https://example.com/calendly_spend_data/",
        "spend_data_2025-07-09.json",
    )

    assert result == "https://example.com/calendly_spend_data/spend_data_2025-07-09.json"


def test_build_bronze_s3_key_uses_ingest_date_and_file_name():

    result = app.build_bronze_s3_key(
        file_name="spend_data_2025-07-09.json",
        ingestion_timestamp="2026-07-30T17:45:00+00:00",
    )

    assert result == (
        "bronze/marketing_spend/"
        "ingest_date=2026-07-30/"
        "spend_data_2025-07-09.json"
    )


def test_lambda_handler_ingests_selected_spend_file(monkeypatch):

    fake_s3 = FakeS3Client()

    file_index_payload = {
        "files": [
            "spend_data_2025-07-09.json",
            "spend_data_2025-07-10.json",
        ]
    }

    spend_payload = [
        {
            "date": "2025-07-10",
            "channel": "facebook_paid_ads",
            "spend": 653.28,
        },
        {
            "date": "2025-07-10",
            "channel": "youtube_paid_ads",
            "spend": 487.59,
        },
    ]

    def fake_fetch_json_from_url(url: str) -> Any:

        if url.endswith("file_index.json"):
            return file_index_payload

        if url.endswith("spend_data_2025-07-10.json"):
            return spend_payload

        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")
    monkeypatch.setenv("SOURCE_BASE_URL", "https://example.com/calendly_spend_data")
    monkeypatch.setattr(app, "get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(app, "fetch_json_from_url", fake_fetch_json_from_url)

    response = app.lambda_handler(
        {"file_name": "spend_data_2025-07-10.json"},
        context=None,
    )
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert response_body["message"] == "Marketing spend files ingested successfully."
    assert response_body["s3_bucket"] == "test-raw-bucket"
    assert response_body["available_file_count"] == 2
    assert response_body["ingested_file_count"] == 1
    assert response_body["ingested_files"][0]["source_file_name"] == (
        "spend_data_2025-07-10.json"
    )
    assert response_body["ingested_files"][0]["record_count"] == 2

    assert len(fake_s3.put_object_calls) == 1

    put_call = fake_s3.put_object_calls[0]

    assert put_call["Bucket"] == "test-raw-bucket"
    assert put_call["Key"].startswith("bronze/marketing_spend/ingest_date=")
    assert put_call["Key"].endswith("spend_data_2025-07-10.json")
    assert put_call["ContentType"] == "application/json"

    written_record = json.loads(put_call["Body"].decode("utf-8"))

    assert written_record["source_system"] == "marketing_spend_public_s3"
    assert written_record["source_file_name"] == "spend_data_2025-07-10.json"
    assert written_record["raw_payload"] == spend_payload


def test_lambda_handler_returns_200_when_no_requested_file_matches(monkeypatch):

    fake_s3 = FakeS3Client()

    file_index_payload = {
        "files": [
            "spend_data_2025-07-09.json",
        ]
    }

    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")
    monkeypatch.setattr(app, "get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(app, "fetch_json_from_url", lambda url: file_index_payload)

    response = app.lambda_handler(
        {"file_name": "spend_data_2099-01-01.json"},
        context=None,
    )
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert response_body["message"] == "No matching marketing spend files selected for ingestion."
    assert response_body["ingested_file_count"] == 0
    assert fake_s3.put_object_calls == []


def test_lambda_handler_returns_500_when_bucket_env_var_missing(monkeypatch):

    monkeypatch.delenv("RAW_BUCKET_NAME", raising=False)

    response = app.lambda_handler({}, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 500
    assert response_body["message"] == "Marketing spend ingestion configuration or runtime error."
    assert "RAW_BUCKET_NAME" in response_body["error"]


def test_lambda_handler_returns_400_for_bad_file_index(monkeypatch):
    
    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")
    monkeypatch.setattr(app, "fetch_json_from_url", lambda url: {"unexpected": []})

    response = app.lambda_handler({}, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert response_body["message"] == "Invalid marketing spend ingestion request or source data."
    assert "must contain a list" in response_body["error"]