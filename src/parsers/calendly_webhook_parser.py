import json
from pathlib import Path
from typing import Any, Dict, Optional


EVENT_TYPE_TO_CHANNEL = {
    "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78": "facebook_paid_ads",
    "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098": "youtube_paid_ads",
    "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312": "tiktok_paid_ads",
}


def safe_get(data: Dict[str, Any], *keys: str) -> Optional[Any]:
    """
    Safely retrieve a nested dictionary value.

    Example:
        safe_get(data, "payload", "scheduled_event", "start_time")
    """
    current: Any = data

    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)

    return current


def extract_id_from_uri(uri: Optional[str]) -> Optional[str]:
    """
    Extract the final ID segment from a Calendly URI.
    """
    if not uri:
        return None

    return uri.rstrip("/").split("/")[-1]


def extract_phone_number(payload: Dict[str, Any]) -> Optional[str]:
    """
    Extract phone number from Calendly questions_and_answers.
    """
    questions = payload.get("questions_and_answers") or []

    for item in questions:
        question = (item.get("question") or "").lower()
        if "phone" in question:
            return item.get("answer")

    return None


def parse_invitee_created_webhook(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a Calendly invitee.created webhook event into a Silver-friendly record.
    """
    #payload = event.get("payload") or {}
    if event.get("event") != "invitee.created":
        raise ValueError(f"Unsupported webhook event type: {event.get('event')}")

    payload = event.get("payload") or {}

    if not payload:
        raise ValueError("Calendly webhook payload is missing or empty.")

    scheduled_event = payload.get("scheduled_event") or {}

    if not scheduled_event:
        raise ValueError("Calendly webhook payload is missing scheduled_event data.")

    tracking = payload.get("tracking") or {}

    event_type_uri = scheduled_event.get("event_type")
    channel = EVENT_TYPE_TO_CHANNEL.get(event_type_uri)

    event_memberships = scheduled_event.get("event_memberships") or []
    primary_membership = event_memberships[0] if event_memberships else {}

    invitee_uri = payload.get("uri")
    scheduled_event_uri = scheduled_event.get("uri")

    return {
        # Webhook metadata
        "webhook_event": event.get("event"),
        "webhook_created_at": event.get("created_at"),
        "webhook_created_by": event.get("created_by"),

        # Invitee / booking identifiers
        "booking_id": extract_id_from_uri(invitee_uri),
        "invitee_uri": invitee_uri,
        "scheduled_event_id": extract_id_from_uri(scheduled_event_uri),
        "scheduled_event_uri": scheduled_event_uri,

        # Channel / campaign attribution
        "event_type_uri": event_type_uri,
        "channel": channel,
        "utm_source": tracking.get("utm_source"),
        "utm_medium": tracking.get("utm_medium"),
        "utm_campaign": tracking.get("utm_campaign"),
        "utm_content": tracking.get("utm_content"),
        "utm_term": tracking.get("utm_term"),
        "salesforce_uuid": tracking.get("salesforce_uuid"),

        # Invitee details
        "invitee_email": payload.get("email"),
        "invitee_name": payload.get("name"),
        "invitee_first_name": payload.get("first_name"),
        "invitee_last_name": payload.get("last_name"),
        "invitee_phone": extract_phone_number(payload),
        "invitee_timezone": payload.get("timezone"),
        "invitee_status": payload.get("status"),
        "rescheduled": payload.get("rescheduled"),

        # Meeting details
        "meeting_name": scheduled_event.get("name"),
        "meeting_status": scheduled_event.get("status"),
        "meeting_created_at": scheduled_event.get("created_at"),
        "meeting_updated_at": scheduled_event.get("updated_at"),
        "meeting_start_time": scheduled_event.get("start_time"),
        "meeting_end_time": scheduled_event.get("end_time"),
        "meeting_location_type": safe_get(scheduled_event, "location", "type"),
        "meeting_location": safe_get(scheduled_event, "location", "location"),

        # Employee / host details
        "employee_user_uri": primary_membership.get("user"),
        "employee_id": extract_id_from_uri(primary_membership.get("user")),
        "employee_email": primary_membership.get("user_email"),
        "employee_name": primary_membership.get("user_name"),
    }


def load_json_file(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


if __name__ == "__main__":
    sample_path = Path("sample_data/calendly_invitee_created_sample.json")
    webhook_event = load_json_file(sample_path)
    parsed = parse_invitee_created_webhook(webhook_event)

    print(json.dumps(parsed, indent=2))