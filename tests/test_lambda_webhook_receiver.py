import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.lambda_webhook_receiver import app


SAMPLE_PATH = Path("sample_data/calendly_invitee_created_sample.json")


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


def build_api_gateway_event(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a minimal API Gateway-style event for Lambda tests.
    """
    return {
        "version": "2.0",
        "routeKey": "POST /calendly",
        "rawPath": "/calendly",
        "headers": {
            "content-type": "application/json",
        },
        "body": json.dumps(body),
        "isBase64Encoded": False,
    }


def load_sample_webhook_event() -> Dict[str, Any]:
    with open(SAMPLE_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def test_lambda_handler_writes_valid_webhook_to_s3(monkeypatch):
    fake_s3 = FakeS3Client()

    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")
    monkeypatch.setattr(app, "get_s3_client", lambda: fake_s3)

    raw_webhook_event = load_sample_webhook_event()
    api_gateway_event = build_api_gateway_event(raw_webhook_event)

    response = app.lambda_handler(api_gateway_event, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert response_body["message"] == "Calendly webhook received and stored."
    assert response_body["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert response_body["s3_bucket"] == "test-raw-bucket"
    assert response_body["s3_key"].startswith(
        "bronze/calendly_webhooks/event_type=invitee.created/"
    )

    assert len(fake_s3.put_object_calls) == 1

    put_call = fake_s3.put_object_calls[0]

    assert put_call["Bucket"] == "test-raw-bucket"
    assert put_call["ContentType"] == "application/json"

    written_record = json.loads(put_call["Body"].decode("utf-8"))

    assert written_record["source_system"] == "calendly"
    assert written_record["webhook_event"] == "invitee.created"
    assert written_record["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert written_record["channel"] == "facebook_paid_ads"
    assert written_record["raw_event"] == raw_webhook_event


def test_lambda_handler_rejects_invalid_json(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")

    api_gateway_event = {
        "body": "{not valid json}",
        "isBase64Encoded": False,
    }

    response = app.lambda_handler(api_gateway_event, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert response_body["message"] == "Invalid Calendly webhook request."
    assert "not valid JSON" in response_body["error"]


def test_lambda_handler_rejects_missing_body(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")

    api_gateway_event = {
        "isBase64Encoded": False,
    }

    response = app.lambda_handler(api_gateway_event, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert response_body["message"] == "Invalid Calendly webhook request."
    assert "Request body is missing" in response_body["error"]


def test_lambda_handler_rejects_empty_payload(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET_NAME", "test-raw-bucket")

    raw_webhook_event = {
        "created_at": "2025-07-09T06:04:34.000000Z",
        "created_by": "https://api.calendly.com/users/example",
        "event": "invitee.created",
        "payload": {},
    }

    api_gateway_event = build_api_gateway_event(raw_webhook_event)

    response = app.lambda_handler(api_gateway_event, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert response_body["message"] == "Invalid Calendly webhook request."
    assert "payload is missing or empty" in response_body["error"]


def test_lambda_handler_returns_500_when_bucket_env_var_missing(monkeypatch):
    monkeypatch.delenv("RAW_BUCKET_NAME", raising=False)

    raw_webhook_event = load_sample_webhook_event()
    api_gateway_event = build_api_gateway_event(raw_webhook_event)

    response = app.lambda_handler(api_gateway_event, context=None)
    response_body = json.loads(response["body"])

    assert response["statusCode"] == 500
    assert response_body["message"] == "Webhook receiver configuration error."
    assert "RAW_BUCKET_NAME" in response_body["error"]