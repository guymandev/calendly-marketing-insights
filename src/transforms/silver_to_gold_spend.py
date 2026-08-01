from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Tuple


def round_currency(value: Decimal) -> float:
    """
    Round currency values to two decimal places using standard half-up rounding.
    """
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def validate_silver_spend_record(record: Dict[str, Any]) -> None:
    """
    Validate the minimum fields needed from a Silver marketing spend record.

    Expected Silver grain:
        one spend_date + channel spend row
    """
    required_fields = [
        "spend_date",
        "channel",
        "spend_usd",
    ]

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
            f"Invalid spend_date format: {record['spend_date']}. Expected YYYY-MM-DD."
        ) from exc

    try:
        spend_amount = float(record["spend_usd"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid spend_usd value: {record['spend_usd']}") from exc

    if spend_amount < 0:
        raise ValueError(f"spend_usd cannot be negative: {spend_amount}")


def calculate_daily_spend_by_channel(
    silver_spend_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold daily spend by channel.

    Output grain:
        one row per spend_date + channel
    """
    grouped_spend: Dict[Tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0.00"))

    for record in silver_spend_records:
        validate_silver_spend_record(record)

        spend_date = record["spend_date"]
        channel = record["channel"]
        spend_usd = Decimal(str(record["spend_usd"]))

        grouped_spend[(spend_date, channel)] += spend_usd

    return [
        {
            "spend_date": spend_date,
            "channel": channel,
            "total_spend_usd": round_currency(total_spend_usd),
        }
        for (spend_date, channel), total_spend_usd in sorted(grouped_spend.items())
    ]


def calculate_channel_spend_summary(
    silver_spend_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Build Gold channel-level spend summary.

    Output grain:
        one row per channel
    """
    grouped_spend: Dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    grouped_day_counts: Dict[str, set[str]] = defaultdict(set)

    for record in silver_spend_records:
        validate_silver_spend_record(record)

        spend_date = record["spend_date"]
        channel = record["channel"]
        spend_usd = Decimal(str(record["spend_usd"]))

        grouped_spend[channel] += spend_usd
        grouped_day_counts[channel].add(spend_date)

    return [
        {
            "channel": channel,
            "total_spend_usd": round_currency(total_spend_usd),
            "spend_day_count": len(grouped_day_counts[channel]),
            "average_daily_spend_usd": round_currency(
                total_spend_usd / Decimal(len(grouped_day_counts[channel]))
            )
            if grouped_day_counts[channel]
            else 0.0,
        }
        for channel, total_spend_usd in sorted(grouped_spend.items())
    ]


def build_spend_gold_tables(
    silver_spend_records: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build all spend-only Gold outputs from Silver marketing spend records.
    """
    return {
        "gold_daily_spend_by_channel": calculate_daily_spend_by_channel(
            silver_spend_records
        ),
        "gold_channel_spend_summary": calculate_channel_spend_summary(
            silver_spend_records
        ),
    }