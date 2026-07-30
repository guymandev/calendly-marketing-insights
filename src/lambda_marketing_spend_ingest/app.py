import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3


DEFAULT_SOURCE_BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data"
)
DEFAULT_FILE_INDEX_NAME = "file_index.json"


def get_s3_client():
    """
    Return an S3 client.

    Wrapped for test monkeypatching.
    """
    return boto3.client("s3")


def get_required_env_var(name: str) -> str:
    
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def get_optional_env_var(name: str, default: str) -> str:
    return os.environ.get(name, default)


def build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(body),
    }


def fetch_json_from_url(url: str) -> Any:
    """
    Fetch JSON from a public URL.

    Uses Python stdlib so the Lambda does not need requests as a dependency.
    """
    request = Request(
        url,
        headers={
            "User-Agent": "calendly-marketing-spend-ingest/1.0",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP error while reading {url}: {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error while reading {url}: {exc.reason}") from exc

    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Response from {url} was not valid JSON.") from exc


def normalize_file_index(file_index_payload: Any) -> List[str]:
    """
    Normalize file_index.json into a list of spend file names.

    Supports a few common shapes:
      ["spend_data_2025-07-09.json"]
      {"files": ["spend_data_2025-07-09.json"]}
      {"file_names": ["spend_data_2025-07-09.json"]}
      {"spend_files": ["spend_data_2025-07-09.json"]}
      [{"file_name": "spend_data_2025-07-09.json"}]
      [{"key": "calendly_spend_data/spend_data_2025-07-09.json"}]
    """
    if isinstance(file_index_payload, list):
        raw_files = file_index_payload
    elif isinstance(file_index_payload, dict):
        raw_files = (
            file_index_payload.get("files")
            or file_index_payload.get("file_names")
            or file_index_payload.get("spend_files")
            or file_index_payload.get("objects")
        )
    else:
        raw_files = None

    if not isinstance(raw_files, list):
        raise ValueError("file_index.json must contain a list of spend files.")

    file_names: List[str] = []

    for item in raw_files:
        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            candidate = (
                item.get("file_name")
                or item.get("filename")
                or item.get("name")
                or item.get("key")
                or item.get("path")
            )
        else:
            candidate = None

        if not candidate:
            continue

        # If the index provides a full path/key, keep just the filename.
        file_name = PurePosixPath(candidate).name

        if file_name.startswith("spend_data_") and file_name.endswith(".json"):
            file_names.append(file_name)

    deduped_file_names = sorted(set(file_names))

    if not deduped_file_names:
        raise ValueError("No spend_data_YYYY-MM-DD.json files found in file_index.json.")

    return deduped_file_names


def filter_file_names_from_event(
    file_names: List[str],
    event: Optional[Dict[str, Any]],
) -> List[str]:
    """
    Optionally allow manual Lambda invocations to specify a limited set of files.

    Supported event examples:
      {"file_name": "spend_data_2025-07-09.json"}
      {"file_names": ["spend_data_2025-07-09.json", "spend_data_2025-07-10.json"]}
      {"max_files": 3}
    """
    if not event:
        return file_names

    if isinstance(event.get("file_name"), str):
        requested = [event["file_name"]]
        return [name for name in file_names if name in requested]

    if isinstance(event.get("file_names"), list):
        requested = {name for name in event["file_names"] if isinstance(name, str)}
        return [name for name in file_names if name in requested]

    max_files = event.get("max_files")

    if isinstance(max_files, int) and max_files > 0:
        return file_names[-max_files:]

    env_max_files = os.environ.get("MAX_FILES")

    if env_max_files:
        try:
            parsed_max_files = int(env_max_files)
        except ValueError as exc:
            raise ValueError("MAX_FILES must be an integer.") from exc

        if parsed_max_files > 0:
            return file_names[-parsed_max_files:]

    return file_names


def build_source_url(source_base_url: str, file_name: str) -> str:
    return f"{source_base_url.rstrip('/')}/{file_name}"


def build_bronze_s3_key(file_name: str, ingestion_timestamp: str) -> str:

    ingest_date = ingestion_timestamp[:10]

    return (
        f"bronze/marketing_spend/"
        f"ingest_date={ingest_date}/"
        f"{file_name}"
    )


def build_bronze_record(
    source_url: str,
    source_file_name: str,
    raw_payload: Any,
    s3_key: str,
    ingestion_timestamp: str,
) -> Dict[str, Any]:
    
    return {
        "source_system": "marketing_spend_public_s3",
        "ingestion_timestamp": ingestion_timestamp,
        "source_url": source_url,
        "source_file_name": source_file_name,
        "raw_s3_key": s3_key,
        "raw_payload": raw_payload,
    }


def write_record_to_s3(
    bucket_name: str,
    s3_key: str,
    record: Dict[str, Any],
) -> None:
    
    s3_client = get_s3_client()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )


def ingest_spend_file(
    bucket_name: str,
    source_base_url: str,
    file_name: str,
    ingestion_timestamp: str,
) -> Dict[str, Any]:
    
    source_url = build_source_url(source_base_url, file_name)
    raw_payload = fetch_json_from_url(source_url)

    # Add UUID fallback to avoid accidental collision if naming changes later.
    s3_key = build_bronze_s3_key(file_name, ingestion_timestamp)

    if not file_name:
        s3_key = (
            f"bronze/marketing_spend/"
            f"ingest_date={ingestion_timestamp[:10]}/"
            f"{uuid.uuid4()}.json"
        )

    bronze_record = build_bronze_record(
        source_url=source_url,
        source_file_name=file_name,
        raw_payload=raw_payload,
        s3_key=s3_key,
        ingestion_timestamp=ingestion_timestamp,
    )

    write_record_to_s3(
        bucket_name=bucket_name,
        s3_key=s3_key,
        record=bronze_record,
    )

    return {
        "source_file_name": file_name,
        "source_url": source_url,
        "s3_key": s3_key,
        "record_count": len(raw_payload) if isinstance(raw_payload, list) else None,
    }


def lambda_handler(event: Optional[Dict[str, Any]], context: Optional[Any]) -> Dict[str, Any]:
    """
    Ingest marketing spend JSON files from the public DEA S3 source into Bronze S3.
    """
    try:
        bucket_name = get_required_env_var("RAW_BUCKET_NAME")
        source_base_url = get_optional_env_var("SOURCE_BASE_URL", DEFAULT_SOURCE_BASE_URL)
        file_index_name = get_optional_env_var("FILE_INDEX_NAME", DEFAULT_FILE_INDEX_NAME)

        ingestion_timestamp = datetime.now(timezone.utc).isoformat()

        file_index_url = build_source_url(source_base_url, file_index_name)
        file_index_payload = fetch_json_from_url(file_index_url)

        file_names = normalize_file_index(file_index_payload)
        selected_file_names = filter_file_names_from_event(file_names, event)

        if not selected_file_names:
            return build_response(
                200,
                {
                    "message": "No matching marketing spend files selected for ingestion.",
                    "file_index_url": file_index_url,
                    "available_file_count": len(file_names),
                    "ingested_file_count": 0,
                    "ingested_files": [],
                },
            )

        ingested_files = []

        for file_name in selected_file_names:
            ingested_files.append(
                ingest_spend_file(
                    bucket_name=bucket_name,
                    source_base_url=source_base_url,
                    file_name=file_name,
                    ingestion_timestamp=ingestion_timestamp,
                )
            )

        return build_response(
            200,
            {
                "message": "Marketing spend files ingested successfully.",
                "s3_bucket": bucket_name,
                "file_index_url": file_index_url,
                "available_file_count": len(file_names),
                "ingested_file_count": len(ingested_files),
                "ingested_files": ingested_files,
            },
        )

    except ValueError as exc:
        return build_response(
            400,
            {
                "message": "Invalid marketing spend ingestion request or source data.",
                "error": str(exc),
            },
        )

    except RuntimeError as exc:
        return build_response(
            500,
            {
                "message": "Marketing spend ingestion configuration or runtime error.",
                "error": str(exc),
            },
        )

    except Exception as exc:
        return build_response(
            500,
            {
                "message": "Unexpected error while ingesting marketing spend files.",
                "error": str(exc),
            },
        )