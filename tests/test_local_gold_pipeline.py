from pathlib import Path

from src.pipelines.local_gold_pipeline import (
    OUTPUT_PATH,
    build_gold_metrics,
    run_local_gold_pipeline,
)


def test_build_gold_metrics_returns_expected_sections():
    bookings = [
        {
            "booking_id": "booking_1",
            "booking_date": "2025-07-09",
            "channel": "facebook_paid_ads",
            "utm_campaign": "campaign_a",
            "meeting_day_of_week": "Wednesday",
            "meeting_hour": 17,
            "meeting_week_start_date": "2025-07-07",
            "employee_id": "employee_1",
            "employee_name": "Test Employee",
        }
    ]

    spend_records = [
        {
            "spend_date": "2025-07-09",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        }
    ]

    metrics = build_gold_metrics(bookings, spend_records)

    assert set(metrics.keys()) == {
        "dashboard_kpis",
        "daily_calls_by_source",
        "cpb_by_channel",
        "booking_trends",
        "channel_attribution",
        "booking_volume_by_time_slot",
        "employee_meeting_load",
    }

    assert metrics["dashboard_kpis"]["total_bookings"] == 1
    assert metrics["dashboard_kpis"]["total_spend"] == 100.00
    assert metrics["dashboard_kpis"]["average_cpb"] == 100.00


def test_run_local_gold_pipeline_creates_output_file():
    metrics = run_local_gold_pipeline()

    assert OUTPUT_PATH.exists()
    assert Path(OUTPUT_PATH).is_file()

    assert metrics["dashboard_kpis"]["total_bookings"] == 1
    assert "cpb_by_channel" in metrics
    assert "employee_meeting_load" in metrics