from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple


def validate_silver_booking_record(record: Dict[str, Any]) -> None:
    """
    Validate the minimum fields needed from a Silver Calendly booking record.

    Expected Silver grain:
        one invitee.created booking row
    """
    required_fields = [
        "booking_id",
        "channel",
        "booking_date",
        "meeting_date",
        "meeting_day_of_week",
        "meeting_hour",
    ]

    missing_fields = [
        field_name
        for field_name in required_fields
        if record.get(field_name) is None
    ]

    if missing_fields:
        raise ValueError(f"Silver booking record is missing fields: {missing_fields}")

    try:
        datetime.strptime(record["booking_date"], "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid booking_date format: {record['booking_date']}. "
            "Expected YYYY-MM-DD."
        ) from exc

    try:
        datetime.strptime(record["meeting_date"], "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid meeting_date format: {record['meeting_date']}. "
            "Expected YYYY-MM-DD."
        ) from exc

    try:
        meeting_hour = int(record["meeting_hour"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid meeting_hour value: {record['meeting_hour']}") from exc

    if meeting_hour < 0 or meeting_hour > 23:
        raise ValueError(f"meeting_hour must be between 0 and 23: {meeting_hour}")


def calculate_daily_calls_by_source(
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold daily calls booked by source/channel.

    Output grain:
        one row per booking_date + channel

    Metric:
        booked_call_count = distinct booking_id count
    """
    grouped_bookings: Dict[Tuple[str, str], set[str]] = defaultdict(set)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        booking_date = record["booking_date"]
        channel = record["channel"]
        booking_id = record["booking_id"]

        grouped_bookings[(booking_date, channel)].add(booking_id)

    return [
        {
            "booking_date": booking_date,
            "channel": channel,
            "booked_call_count": len(booking_ids),
        }
        for (booking_date, channel), booking_ids in sorted(grouped_bookings.items())
    ]


def calculate_booking_trends(
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold booking trend over time.

    Output grain:
        one row per booking_date

    Metric:
        booked_call_count = distinct booking_id count
    """
    grouped_bookings: Dict[str, set[str]] = defaultdict(set)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        booking_date = record["booking_date"]
        booking_id = record["booking_id"]

        grouped_bookings[booking_date].add(booking_id)

    return [
        {
            "booking_date": booking_date,
            "booked_call_count": len(booking_ids),
        }
        for booking_date, booking_ids in sorted(grouped_bookings.items())
    ]


def calculate_channel_attribution(
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold channel attribution from booking records.

    Output grain:
        one row per channel + campaign

    Metric:
        booked_call_count = distinct booking_id count
    """
    grouped_bookings: Dict[Tuple[str, str], set[str]] = defaultdict(set)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        channel = record["channel"]
        campaign = record.get("utm_campaign") or "unknown_campaign"
        booking_id = record["booking_id"]

        grouped_bookings[(channel, campaign)].add(booking_id)

    return [
        {
            "channel": channel,
            "utm_campaign": campaign,
            "booked_call_count": len(booking_ids),
        }
        for (channel, campaign), booking_ids in sorted(grouped_bookings.items())
    ]


def calculate_booking_volume_by_time_slot(
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold booking volume by day of week and hour.

    Output grain:
        one row per meeting_day_of_week + meeting_hour

    Metric:
        booked_call_count = distinct booking_id count
    """
    grouped_bookings: Dict[Tuple[str, int], set[str]] = defaultdict(set)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        meeting_day_of_week = record["meeting_day_of_week"]
        meeting_hour = int(record["meeting_hour"])
        booking_id = record["booking_id"]

        grouped_bookings[(meeting_day_of_week, meeting_hour)].add(booking_id)

    return [
        {
            "meeting_day_of_week": meeting_day_of_week,
            "meeting_hour": meeting_hour,
            "booked_call_count": len(booking_ids),
        }
        for (meeting_day_of_week, meeting_hour), booking_ids in sorted(
            grouped_bookings.items()
        )
    ]


def calculate_employee_meeting_load(
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold meeting load by employee.

    Output grain:
        one row per employee

    Metric:
        booked_call_count = distinct booking_id count
    """
    grouped_bookings: Dict[Tuple[str, str, str], set[str]] = defaultdict(set)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        employee_id = record.get("employee_id") or "unknown_employee"
        employee_name = record.get("employee_name") or "Unknown Employee"
        employee_email = record.get("employee_email") or "unknown_email"
        booking_id = record["booking_id"]

        grouped_bookings[(employee_id, employee_name, employee_email)].add(booking_id)

    return [
        {
            "employee_id": employee_id,
            "employee_name": employee_name,
            "employee_email": employee_email,
            "booked_call_count": len(booking_ids),
        }
        for (employee_id, employee_name, employee_email), booking_ids in sorted(
            grouped_bookings.items()
        )
    ]


def calculate_booking_dashboard_kpis(
    silver_booking_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build booking-only dashboard KPI values.

    Combined spend + booking KPIs will be handled in a later transform.
    """
    validated_booking_ids: set[str] = set()
    channels: set[str] = set()
    booking_dates: set[str] = set()
    meeting_dates: set[str] = set()

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        validated_booking_ids.add(record["booking_id"])
        channels.add(record["channel"])
        booking_dates.add(record["booking_date"])
        meeting_dates.add(record["meeting_date"])

    return {
        "total_bookings": len(validated_booking_ids),
        "channel_count": len(channels),
        "booking_date_count": len(booking_dates),
        "meeting_date_count": len(meeting_dates),
    }


def build_booking_gold_tables(
    silver_booking_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build all booking-only Gold outputs from Silver Calendly booking records.
    """
    return {
        "gold_daily_calls_by_source": calculate_daily_calls_by_source(
            silver_booking_records
        ),
        "gold_booking_trends": calculate_booking_trends(silver_booking_records),
        "gold_channel_attribution": calculate_channel_attribution(
            silver_booking_records
        ),
        "gold_booking_volume_by_time_slot": calculate_booking_volume_by_time_slot(
            silver_booking_records
        ),
        "gold_employee_meeting_load": calculate_employee_meeting_load(
            silver_booking_records
        ),
        "gold_booking_dashboard_kpis": calculate_booking_dashboard_kpis(
            silver_booking_records
        ),
    }