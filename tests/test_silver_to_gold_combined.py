import pytest

from src.transforms.silver_to_gold_combined import (
    build_combined_gold_tables,
    calculate_combined_dashboard_kpis,
    calculate_cpb_by_channel,
    calculate_daily_cpb_by_channel,
    safe_cpb,
    validate_silver_booking_record,
    validate_silver_spend_record,
)


def build_sample_silver_spend_records():
    return [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 653.28,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "spend_usd": 487.59,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "tiktok_paid_ads",
            "spend_usd": 345.12,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "facebook_paid_ads",
            "spend_usd": 700.00,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "youtube_paid_ads",
            "spend_usd": 500.00,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
            "spend_usd": 300.00,
        },
    ]


def build_sample_silver_booking_records():
    return [
        {
            "booking_id": "booking-001",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-002",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-003",
            "booking_date": "2026-07-30",
            "channel": "youtube_paid_ads",
        },
        {
            "booking_id": "booking-004",
            "booking_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
        },
    ]


def test_safe_cpb_returns_none_when_booking_count_is_zero():
    assert safe_cpb(total_spend=100, booking_count=0) is None


def test_safe_cpb_calculates_cost_per_booking():
    assert safe_cpb(total_spend=100, booking_count=4) == 25.00


def test_validate_silver_spend_record_accepts_valid_record():
    record = {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "spend_usd": 653.28,
    }

    validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_missing_required_field():
    record = {
        "spend_date": "2026-07-29",
        "spend_usd": 653.28,
    }

    with pytest.raises(ValueError, match="missing fields"):
        validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_bad_date_format():
    record = {
        "spend_date": "07/29/2026",
        "channel": "facebook_paid_ads",
        "spend_usd": 653.28,
    }

    with pytest.raises(ValueError, match="Invalid spend_date format"):
        validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_bad_spend_value():
    record = {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "spend_usd": "not-a-number",
    }

    with pytest.raises(ValueError, match="Invalid spend_usd value"):
        validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_negative_spend():
    record = {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "spend_usd": -10.00,
    }

    with pytest.raises(ValueError, match="spend_usd cannot be negative"):
        validate_silver_spend_record(record)


def test_validate_silver_booking_record_accepts_valid_record():
    record = {
        "booking_id": "booking-001",
        "booking_date": "2026-07-29",
        "channel": "facebook_paid_ads",
    }

    validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_missing_required_field():
    record = {
        "booking_id": "booking-001",
        "booking_date": "2026-07-29",
    }

    with pytest.raises(ValueError, match="missing fields"):
        validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_bad_booking_date():
    record = {
        "booking_id": "booking-001",
        "booking_date": "07/29/2026",
        "channel": "facebook_paid_ads",
    }

    with pytest.raises(ValueError, match="Invalid booking_date format"):
        validate_silver_booking_record(record)


def test_calculate_cpb_by_channel_combines_spend_and_bookings():
    spend_records = build_sample_silver_spend_records()
    booking_records = build_sample_silver_booking_records()

    cpb_by_channel = calculate_cpb_by_channel(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert cpb_by_channel == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 1353.28,
            "booked_call_count": 2,
            "cost_per_booking_usd": 676.64,
        },
        {
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 645.12,
            "booked_call_count": 1,
            "cost_per_booking_usd": 645.12,
        },
        {
            "channel": "youtube_paid_ads",
            "total_spend_usd": 987.59,
            "booked_call_count": 1,
            "cost_per_booking_usd": 987.59,
        },
    ]


def test_calculate_cpb_by_channel_includes_spend_with_no_bookings():
    spend_records = [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "spend_usd": 50.00,
        },
    ]
    booking_records = [
        {
            "booking_id": "booking-001",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        }
    ]

    cpb_by_channel = calculate_cpb_by_channel(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert cpb_by_channel == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 100.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 100.00,
        },
        {
            "channel": "youtube_paid_ads",
            "total_spend_usd": 50.00,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
        },
    ]


def test_calculate_cpb_by_channel_includes_bookings_with_no_spend():
    spend_records = [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        }
    ]
    booking_records = [
        {
            "booking_id": "booking-001",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-002",
            "booking_date": "2026-07-29",
            "channel": "organic_referral",
        },
    ]

    cpb_by_channel = calculate_cpb_by_channel(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert cpb_by_channel == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 100.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 100.00,
        },
        {
            "channel": "organic_referral",
            "total_spend_usd": 0.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 0.00,
        },
    ]


def test_calculate_cpb_by_channel_counts_distinct_booking_ids():
    spend_records = [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        }
    ]
    booking_records = [
        {
            "booking_id": "booking-001",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        },
        {
            "booking_id": "booking-001",
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
        },
    ]

    cpb_by_channel = calculate_cpb_by_channel(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert cpb_by_channel == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 100.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 100.00,
        }
    ]


def test_calculate_daily_cpb_by_channel_combines_spend_and_bookings_by_date_channel():
    spend_records = build_sample_silver_spend_records()
    booking_records = build_sample_silver_booking_records()

    daily_cpb = calculate_daily_cpb_by_channel(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert daily_cpb == [
        {
            "metric_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "total_spend_usd": 653.28,
            "booked_call_count": 2,
            "cost_per_booking_usd": 326.64,
        },
        {
            "metric_date": "2026-07-29",
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 345.12,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
        },
        {
            "metric_date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "total_spend_usd": 487.59,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
        },
        {
            "metric_date": "2026-07-30",
            "channel": "facebook_paid_ads",
            "total_spend_usd": 700.00,
            "booked_call_count": 0,
            "cost_per_booking_usd": None,
        },
        {
            "metric_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 300.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 300.00,
        },
        {
            "metric_date": "2026-07-30",
            "channel": "youtube_paid_ads",
            "total_spend_usd": 500.00,
            "booked_call_count": 1,
            "cost_per_booking_usd": 500.00,
        },
    ]


def test_calculate_combined_dashboard_kpis_returns_expected_summary():
    spend_records = build_sample_silver_spend_records()
    booking_records = build_sample_silver_booking_records()

    kpis = calculate_combined_dashboard_kpis(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert kpis == {
        "total_spend_usd": 2985.99,
        "total_bookings": 4,
        "average_cost_per_booking_usd": 746.50,
        "channel_count": 3,
        "spend_date_count": 2,
        "booking_date_count": 2,
    }


def test_calculate_combined_dashboard_kpis_handles_zero_bookings():
    spend_records = build_sample_silver_spend_records()
    booking_records = []

    kpis = calculate_combined_dashboard_kpis(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert kpis == {
        "total_spend_usd": 2985.99,
        "total_bookings": 0,
        "average_cost_per_booking_usd": None,
        "channel_count": 3,
        "spend_date_count": 2,
        "booking_date_count": 0,
    }


def test_build_combined_gold_tables_returns_expected_outputs():
    spend_records = build_sample_silver_spend_records()
    booking_records = build_sample_silver_booking_records()

    gold_tables = build_combined_gold_tables(
        silver_spend_records=spend_records,
        silver_booking_records=booking_records,
    )

    assert set(gold_tables.keys()) == {
        "gold_cpb_by_channel",
        "gold_daily_cpb_by_channel",
        "gold_combined_dashboard_kpis",
    }

    assert len(gold_tables["gold_cpb_by_channel"]) == 3
    assert len(gold_tables["gold_daily_cpb_by_channel"]) == 6

    assert gold_tables["gold_cpb_by_channel"][0] == {
        "channel": "facebook_paid_ads",
        "total_spend_usd": 1353.28,
        "booked_call_count": 2,
        "cost_per_booking_usd": 676.64,
    }

    assert gold_tables["gold_combined_dashboard_kpis"] == {
        "total_spend_usd": 2985.99,
        "total_bookings": 4,
        "average_cost_per_booking_usd": 746.50,
        "channel_count": 3,
        "spend_date_count": 2,
        "booking_date_count": 2,
    }