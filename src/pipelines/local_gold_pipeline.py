import json
from pathlib import Path
from typing import Any, Dict, List

from src.metrics.gold_metrics import (
    calculate_booking_trends,
    calculate_booking_volume_by_time_slot,
    calculate_channel_attribution,
    calculate_cpb_by_channel,
    calculate_daily_calls_by_source,
    calculate_dashboard_kpis,
    calculate_employee_meeting_load,
)
from src.parsers.calendly_webhook_parser import (
    load_json_file as load_calendly_json_file,
    parse_invitee_created_webhook,
)
from src.parsers.marketing_spend_parser import (
    load_json_file as load_spend_json_file,
    parse_spend_records,
)


CALENDLY_SAMPLE_PATH = Path("sample_data/calendly_invitee_created_sample.json")
SPEND_SAMPLE_PATH = Path("sample_data/spend_data_sample.json")
OUTPUT_PATH = Path("data/gold/local_gold_metrics.json")


def build_gold_metrics(
    bookings: List[Dict[str, Any]],
    spend_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build all local Gold-layer dashboard metrics.
    """
    return {
        "dashboard_kpis": calculate_dashboard_kpis(bookings, spend_records),
        "daily_calls_by_source": calculate_daily_calls_by_source(bookings),
        "cpb_by_channel": calculate_cpb_by_channel(bookings, spend_records),
        "booking_trends": calculate_booking_trends(bookings),
        "channel_attribution": calculate_channel_attribution(bookings, spend_records),
        "booking_volume_by_time_slot": calculate_booking_volume_by_time_slot(bookings),
        "employee_meeting_load": calculate_employee_meeting_load(bookings),
    }


def write_json_output(data: Dict[str, Any], output_path: Path) -> None:
    """
    Write JSON output to disk.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def run_local_gold_pipeline() -> Dict[str, Any]:
    """
    Run the local sample-data pipeline from raw JSON to Gold metrics.
    """
    calendly_raw_event = load_calendly_json_file(CALENDLY_SAMPLE_PATH)
    booking_record = parse_invitee_created_webhook(calendly_raw_event)

    spend_raw_records = load_spend_json_file(SPEND_SAMPLE_PATH)
    spend_records = parse_spend_records(
        spend_raw_records,
        source_file=str(SPEND_SAMPLE_PATH),
    )

    bookings = [booking_record]

    gold_metrics = build_gold_metrics(bookings, spend_records)
    write_json_output(gold_metrics, OUTPUT_PATH)

    return gold_metrics


if __name__ == "__main__":
    metrics = run_local_gold_pipeline()
    print(json.dumps(metrics, indent=2))
    print(f"\nGold metrics written to: {OUTPUT_PATH}")