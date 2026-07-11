from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple


def calculate_daily_calls_by_source(
    bookings: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Count distinct bookings per booking_date and channel/source.
    """
    grouped: Dict[Tuple[str, str], set] = defaultdict(set)

    for booking in bookings:
        booking_date = booking.get("booking_date")
        channel = booking.get("channel")
        booking_id = booking.get("booking_id")

        if not booking_date or not channel or not booking_id:
            continue

        grouped[(booking_date, channel)].add(booking_id)

    return [
        {
            "booking_date": booking_date,
            "channel": channel,
            "booked_calls": len(booking_ids),
        }
        for (booking_date, channel), booking_ids in sorted(grouped.items())
    ]


def calculate_cpb_by_channel(
    bookings: List[Dict[str, Any]],
    spend_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Calculate Cost Per Booking by channel.

    CPB = Total Spend / Total Booked Calls
    """
    bookings_by_channel: Dict[str, set] = defaultdict(set)
    spend_by_channel: Dict[str, float] = defaultdict(float)

    for booking in bookings:
        channel = booking.get("channel")
        booking_id = booking.get("booking_id")

        if not channel or not booking_id:
            continue

        bookings_by_channel[channel].add(booking_id)

    for spend_record in spend_records:
        channel = spend_record.get("channel")
        spend_usd = spend_record.get("spend_usd")

        if not channel or spend_usd is None:
            continue

        spend_by_channel[channel] += float(spend_usd)

    all_channels = sorted(set(bookings_by_channel) | set(spend_by_channel))

    results = []

    for channel in all_channels:
        total_bookings = len(bookings_by_channel.get(channel, set()))
        total_spend = round(spend_by_channel.get(channel, 0.0), 2)

        cpb = None
        if total_bookings > 0:
            cpb = round(total_spend / total_bookings, 2)

        results.append(
            {
                "channel": channel,
                "total_spend": total_spend,
                "total_bookings": total_bookings,
                "cost_per_booking": cpb,
            }
        )

    return results


def calculate_booking_trends(
    bookings: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Track daily booking volume by channel/source.
    """
    return calculate_daily_calls_by_source(bookings)


def calculate_channel_attribution(
    bookings: List[Dict[str, Any]],
    spend_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Attribute bookings and spend to channels/campaigns.

    This currently groups by channel and utm_campaign. If utm_campaign is
    missing, it uses 'unknown_campaign'.
    """
    bookings_by_channel_campaign: Dict[Tuple[str, str], set] = defaultdict(set)
    spend_by_channel: Dict[str, float] = defaultdict(float)

    for booking in bookings:
        channel = booking.get("channel")
        booking_id = booking.get("booking_id")
        campaign = booking.get("utm_campaign") or "unknown_campaign"

        if not channel or not booking_id:
            continue

        bookings_by_channel_campaign[(channel, campaign)].add(booking_id)

    for spend_record in spend_records:
        channel = spend_record.get("channel")
        spend_usd = spend_record.get("spend_usd")

        if not channel or spend_usd is None:
            continue

        spend_by_channel[channel] += float(spend_usd)

    results = []

    for (channel, campaign), booking_ids in sorted(bookings_by_channel_campaign.items()):
        total_bookings = len(booking_ids)
        total_spend = round(spend_by_channel.get(channel, 0.0), 2)

        cpb = None
        if total_bookings > 0:
            cpb = round(total_spend / total_bookings, 2)

        results.append(
            {
                "channel": channel,
                "campaign": campaign,
                "total_bookings": total_bookings,
                "total_spend": total_spend,
                "cost_per_booking": cpb,
            }
        )

    return results


def calculate_booking_volume_by_time_slot(
    bookings: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Count bookings by meeting_day_of_week, meeting_hour, and channel.
    """
    grouped: Dict[Tuple[str, int, str], set] = defaultdict(set)

    for booking in bookings:
        day_of_week = booking.get("meeting_day_of_week")
        meeting_hour = booking.get("meeting_hour")
        channel = booking.get("channel")
        booking_id = booking.get("booking_id")

        if day_of_week is None or meeting_hour is None or not channel or not booking_id:
            continue

        grouped[(day_of_week, int(meeting_hour), channel)].add(booking_id)

    return [
        {
            "meeting_day_of_week": day_of_week,
            "meeting_hour": meeting_hour,
            "channel": channel,
            "bookings": len(booking_ids),
        }
        for (day_of_week, meeting_hour, channel), booking_ids in sorted(grouped.items())
    ]


def calculate_employee_meeting_load(
    bookings: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Calculate total meetings and average meetings per week by employee.
    """
    meetings_by_employee: Dict[str, set] = defaultdict(set)
    weeks_by_employee: Dict[str, set] = defaultdict(set)
    employee_names: Dict[str, str] = {}

    for booking in bookings:
        employee_id = booking.get("employee_id")
        employee_name = booking.get("employee_name")
        booking_id = booking.get("booking_id")
        week_start = booking.get("meeting_week_start_date")

        if not employee_id or not booking_id or not week_start:
            continue

        meetings_by_employee[employee_id].add(booking_id)
        weeks_by_employee[employee_id].add(week_start)

        if employee_name:
            employee_names[employee_id] = employee_name

    results = []

    for employee_id in sorted(meetings_by_employee):
        total_meetings = len(meetings_by_employee[employee_id])
        number_of_weeks = len(weeks_by_employee[employee_id])

        avg_meetings_per_week = None
        if number_of_weeks > 0:
            avg_meetings_per_week = round(total_meetings / number_of_weeks, 2)

        results.append(
            {
                "employee_id": employee_id,
                "employee_name": employee_names.get(employee_id),
                "total_meetings": total_meetings,
                "number_of_weeks": number_of_weeks,
                "avg_meetings_per_week": avg_meetings_per_week,
            }
        )

    return results


def calculate_dashboard_kpis(
    bookings: List[Dict[str, Any]],
    spend_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Calculate top-level dashboard KPIs.
    """
    booking_ids = {
        booking.get("booking_id")
        for booking in bookings
        if booking.get("booking_id")
    }

    total_spend = round(
        sum(float(record.get("spend_usd", 0.0)) for record in spend_records),
        2,
    )

    total_bookings = len(booking_ids)

    average_cpb = None
    if total_bookings > 0:
        average_cpb = round(total_spend / total_bookings, 2)

    return {
        "total_bookings": total_bookings,
        "total_spend": total_spend,
        "average_cpb": average_cpb,
    }