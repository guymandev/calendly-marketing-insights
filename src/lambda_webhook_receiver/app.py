import base64
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3

from src.parsers.calendly_webhook_parser import parse_invitee_created_webhook


def get_s3_client():
    """
    Return an S3 client.

    This is wrapped in a function so tests can monkeypatch it.
    """
    return boto3.client("s3")


def build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build an API Gateway-compatible Lambda response.
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(body),
    }


def parse_api_gateway_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse the JSON body from an API Gateway event.

    Supports both plain text and base64-encoded API Gateway bodies.
    """
    body = event.get("body")

    if body is None:
        raise ValueError("Request body is missing.")

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    try:
        parsed_body = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError("Request body is not valid JSON.") from exc

    if not isinstance(parsed_body, dict):
        raise ValueError("Request body must be a JSON object.")

    return parsed_body


def get_required_env_var(name: str) -> str:
    """
    Get a required environment variable.
    """
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def build_bronze_s3_key(
    parsed_event: Dict[str, Any],
    ingestion_timestamp: str,
) -> str:
    """
    Build the S3 key for the raw Calendly webhook event.

    Example:
        bronze/calendly_webhooks/event_type=invitee.created/ingest_date=2025-07-09/<booking_id>.json
    """
    webhook_event = parsed_event.get("webhook_event") or "unknown_event"
    booking_id = parsed_event.get("booking_id") or str(uuid.uuid4())

    ingest_date = ingestion_timestamp[:10]

    return (
        f"bronze/calendly_webhooks/"
        f"event_type={webhook_event}/"
        f"ingest_date={ingest_date}/"
        f"{booking_id}.json"
    )


def build_bronze_record(
    raw_event: Dict[str, Any],
    parsed_event: Dict[str, Any],
    s3_key: str,
    ingestion_timestamp: str,
) -> Dict[str, Any]:
    """
    Build the Bronze record written to S3.

    The raw Calendly event is preserved, while metadata is added for traceability.
    """
    return {
        "source_system": "calendly",
        "ingestion_timestamp": ingestion_timestamp,
        "webhook_event": parsed_event.get("webhook_event"),
        "booking_id": parsed_event.get("booking_id"),
        "event_type_uri": parsed_event.get("event_type_uri"),
        "channel": parsed_event.get("channel"),
        "raw_s3_key": s3_key,
        "raw_event": raw_event,
    }


def write_record_to_s3(
    bucket_name: str,
    s3_key: str,
    record: Dict[str, Any],
) -> None:
    """
    Write the Bronze record to S3.
    """
    s3_client = get_s3_client()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    """
    Receive Calendly invitee.created webhook events and write raw events to S3 Bronze.
    """
    try:
        bucket_name = get_required_env_var("RAW_BUCKET_NAME")

        raw_event = parse_api_gateway_body(event)

        # Reuse the parser validation so the Lambda rejects malformed or unsupported events.
        parsed_event = parse_invitee_created_webhook(raw_event)

        ingestion_timestamp = datetime.now(timezone.utc).isoformat()
        s3_key = build_bronze_s3_key(parsed_event, ingestion_timestamp)

        bronze_record = build_bronze_record(
            raw_event=raw_event,
            parsed_event=parsed_event,
            s3_key=s3_key,
            ingestion_timestamp=ingestion_timestamp,
        )

        write_record_to_s3(
            bucket_name=bucket_name,
            s3_key=s3_key,
            record=bronze_record,
        )

        return build_response(
            200,
            {
                "message": "Calendly webhook received and stored.",
                "booking_id": parsed_event.get("booking_id"),
                "s3_bucket": bucket_name,
                "s3_key": s3_key,
            },
        )

    except ValueError as exc:
        return build_response(
            400,
            {
                "message": "Invalid Calendly webhook request.",
                "error": str(exc),
            },
        )

    except RuntimeError as exc:
        return build_response(
            500,
            {
                "message": "Webhook receiver configuration error.",
                "error": str(exc),
            },
        )

    except Exception as exc:
        return build_response(
            500,
            {
                "message": "Unexpected error while processing Calendly webhook.",
                "error": str(exc),
            },
        )