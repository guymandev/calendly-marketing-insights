from pathlib import Path

import pytest

from src.parsers.calendly_webhook_parser import (
    load_json_file,
    parse_invitee_created_webhook,
)


SAMPLE_PATH = Path("sample_data/calendly_invitee_created_sample.json")


def test_parse_invitee_created_webhook_extracts_core_fields():
    raw_event = load_json_file(SAMPLE_PATH)

    parsed = parse_invitee_created_webhook(raw_event)

    assert parsed["webhook_event"] == "invitee.created"
    assert parsed["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert parsed["scheduled_event_id"] == "1ac9e88e-eae3-4e4b-b979-d770cff02d72"
    assert parsed["event_type_uri"] == (
        "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78"
    )
    assert parsed["channel"] == "facebook_paid_ads"
    assert parsed["meeting_name"] == "Data Engineer Academy Info Session"
    assert parsed["meeting_start_time"] == "2025-07-09T17:45:00.000000Z"
    assert parsed["meeting_end_time"] == "2025-07-09T18:00:00.000000Z"
    assert parsed["employee_email"] == "zan@dataengineeracademy.com"
    assert parsed["employee_name"] == "Zan Strmec"
    assert parsed["booking_date"] == "2025-07-09"
    assert parsed["meeting_date"] == "2025-07-09"
    assert parsed["meeting_hour"] == 17
    assert parsed["meeting_day_of_week"] == "Wednesday"
    assert parsed["meeting_week_start_date"] == "2025-07-07"


def test_parse_invitee_created_webhook_extracts_invitee_fields():
    raw_event = load_json_file(SAMPLE_PATH)

    parsed = parse_invitee_created_webhook(raw_event)

    assert parsed["invitee_name"] == "Ninad Magdum"
    assert parsed["invitee_timezone"] == "Asia/Calcutta"
    assert parsed["invitee_status"] == "active"
    assert parsed["rescheduled"] is False


def test_parse_invitee_created_webhook_rejects_empty_payload():
    raw_event = {
        "created_at": "2025-07-09T06:04:34.000000Z",
        "created_by": "https://api.calendly.com/users/example",
        "event": "invitee.created",
        "payload": {},
    }

    with pytest.raises(ValueError, match="payload is missing or empty"):
        parse_invitee_created_webhook(raw_event)


def test_parse_invitee_created_webhook_rejects_wrong_event_type():
    raw_event = {
        "created_at": "2025-07-09T06:04:34.000000Z",
        "created_by": "https://api.calendly.com/users/example",
        "event": "invitee.canceled",
        "payload": {},
    }

    with pytest.raises(ValueError, match="Unsupported webhook event type"):
        parse_invitee_created_webhook(raw_event)