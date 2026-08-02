from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Tuple


def round_currency(value: Decimal) -> float:
    """
    Round currency values to two decimal places using standard half-up rounding.
    """
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def safe_cpb(total_spend: Decimal, booking_count: int) -> float | None:
    """
    Calculate cost per booking.

    Returns None when booking_count is zero to avoid divide-by-zero errors.
    """
    if booking_count == 0:
        return None

    return round_currency(total_spend / Decimal(booking_count))


def validate_silver_spend_record(record: Dict[str, Any]) -> None:
    """
    Validate the minimum spend fields needed for combined Gold metrics.
    """
    required_fields = ["spend_date", "channel", "spend_usd"]

    missing_fields = [
        field_name
        for field_name in required_fields
        if record.get(field_name) is None
    ]

    if missing_fields:
        raise ValueError(f"Silver spend record is missing fields: {missing_fields}")

    try:
        datetime.strptime(record["spend_date"], "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid spend_date format: {record['spend_date']}. "
            "Expected YYYY-MM-DD."
        ) from exc

    try:
        spend_usd = Decimal(str(record["spend_usd"]))
    except Exception as exc:
        raise ValueError(f"Invalid spend_usd value: {record['spend_usd']}") from exc

    if spend_usd < 0:
        raise ValueError(f"spend_usd cannot be negative: {spend_usd}")


def validate_silver_booking_record(record: Dict[str, Any]) -> None:
    """
    Validate the minimum booking fields needed for combined Gold metrics.
    """
    required_fields = ["booking_id", "booking_date", "channel"]

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


def calculate_cpb_by_channel(
    silver_spend_records: List[Dict[str, Any]],
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold cost-per-booking by channel.

    Output grain:
        one row per channel

    Metrics:
        total_spend_usd
        booked_call_count
        cost_per_booking_usd

    Notes:
        A channel can have spend but zero bookings.
        A channel can have bookings but zero spend.
    """
    spend_by_channel: Dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    bookings_by_channel: Dict[str, set[str]] = defaultdict(set)

    for record in silver_spend_records:
        validate_silver_spend_record(record)

        channel = record["channel"]
        spend_usd = Decimal(str(record["spend_usd"]))

        spend_by_channel[channel] += spend_usd

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        channel = record["channel"]
        booking_id = record["booking_id"]

        bookings_by_channel[channel].add(booking_id)

    all_channels = sorted(set(spend_by_channel.keys()) | set(bookings_by_channel.keys()))

    return [
        {
            "channel": channel,
            "total_spend_usd": round_currency(spend_by_channel[channel]),
            "booked_call_count": len(bookings_by_channel[channel]),
            "cost_per_booking_usd": safe_cpb(
                total_spend=spend_by_channel[channel],
                booking_count=len(bookings_by_channel[channel]),
            ),
        }
        for channel in all_channels
    ]


def calculate_daily_cpb_by_channel(
    silver_spend_records: List[Dict[str, Any]],
    silver_booking_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold daily cost-per-booking by channel.

    Output grain:
        one row per metric_date + channel

    Join logic:
        spend_date from spend records is aligned with booking_date from booking records.

    Notes:
        This table is useful for trend charts and daily performance monitoring.
    """
    spend_by_date_channel: Dict[Tuple[str, str], Decimal] = defaultdict(
        lambda: Decimal("0.00")
    )
    bookings_by_date_channel: Dict[Tuple[str, str], set[str]] = defaultdict(set)

    for record in silver_spend_records:
        validate_silver_spend_record(record)

        spend_date = record["spend_date"]
        channel = record["channel"]
        spend_usd = Decimal(str(record["spend_usd"]))

        spend_by_date_channel[(spend_date, channel)] += spend_usd

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        booking_date = record["booking_date"]
        channel = record["channel"]
        booking_id = record["booking_id"]

        bookings_by_date_channel[(booking_date, channel)].add(booking_id)

    all_date_channel_keys = sorted(
        set(spend_by_date_channel.keys()) | set(bookings_by_date_channel.keys())
    )

    return [
        {
            "metric_date": metric_date,
            "channel": channel,
            "total_spend_usd": round_currency(
                spend_by_date_channel[(metric_date, channel)]
            ),
            "booked_call_count": len(bookings_by_date_channel[(metric_date, channel)]),
            "cost_per_booking_usd": safe_cpb(
                total_spend=spend_by_date_channel[(metric_date, channel)],
                booking_count=len(bookings_by_date_channel[(metric_date, channel)]),
            ),
        }
        for metric_date, channel in all_date_channel_keys
    ]


def calculate_combined_dashboard_kpis(
    silver_spend_records: List[Dict[str, Any]],
    silver_booking_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build combined dashboard KPI values using both spend and booking records.

    Output grain:
        one dashboard KPI object
    """
    total_spend = Decimal("0.00")
    booking_ids: set[str] = set()
    spend_channels: set[str] = set()
    booking_channels: set[str] = set()
    spend_dates: set[str] = set()
    booking_dates: set[str] = set()

    for record in silver_spend_records:
        validate_silver_spend_record(record)

        spend_date = record["spend_date"]
        channel = record["channel"]
        spend_usd = Decimal(str(record["spend_usd"]))

        total_spend += spend_usd
        spend_channels.add(channel)
        spend_dates.add(spend_date)

    for record in silver_booking_records:
        validate_silver_booking_record(record)

        booking_id = record["booking_id"]
        booking_date = record["booking_date"]
        channel = record["channel"]

        booking_ids.add(booking_id)
        booking_channels.add(channel)
        booking_dates.add(booking_date)

    total_bookings = len(booking_ids)
    all_channels = spend_channels | booking_channels

    return {
        "total_spend_usd": round_currency(total_spend),
        "total_bookings": total_bookings,
        "average_cost_per_booking_usd": safe_cpb(
            total_spend=total_spend,
            booking_count=total_bookings,
        ),
        "channel_count": len(all_channels),
        "spend_date_count": len(spend_dates),
        "booking_date_count": len(booking_dates),
    }


def build_combined_gold_tables(
    silver_spend_records: List[Dict[str, Any]],
    silver_booking_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build all combined spend + booking Gold outputs.
    """
    return {
        "gold_cpb_by_channel": calculate_cpb_by_channel(
            silver_spend_records=silver_spend_records,
            silver_booking_records=silver_booking_records,
        ),
        "gold_daily_cpb_by_channel": calculate_daily_cpb_by_channel(
            silver_spend_records=silver_spend_records,
            silver_booking_records=silver_booking_records,
        ),
        "gold_combined_dashboard_kpis": calculate_combined_dashboard_kpis(
            silver_spend_records=silver_spend_records,
            silver_booking_records=silver_booking_records,
        ),
    }