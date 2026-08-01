import pytest

from src.transforms.silver_to_gold_bookings import (
    build_booking_gold_tables,
    calculate_booking_dashboard_kpis,
    calculate_booking_trends,
    calculate_booking_volume_by_time_slot,
    calculate_channel_attribution,
    calculate_daily_calls_by_source,
    calculate_employee_meeting_load,
    validate_silver_booking_record,
)


def build_sample_silver_booking_records():
    return [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": "2026-07-29",
            "meeting_date": "2026-08-01",
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-002",
            "channel": "facebook_paid_ads",
            "booking_date": "2026-07-29",
            "meeting_date": "2026-08-01",
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 18,
            "utm_campaign": "summer_campaign",
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
        },
        {
            "booking_id": "booking-003",
            "channel": "youtube_paid_ads",
            "booking_date": "2026-07-30",
            "meeting_date": "2026-08-02",
            "meeting_day_of_week": "Sunday",
            "meeting_hour": 10,
            "utm_campaign": "youtube_campaign",
            "employee_id": "employee-002",
            "employee_name": "Employee Two",
            "employee_email": "employee.two@example.com",
        },
        {
            "booking_id": "booking-004",
            "channel": "tiktok_paid_ads",
            "booking_date": "2026-07-30",
            "meeting_date": "2026-08-03",
            "meeting_day_of_week": "Monday",
            "meeting_hour": 9,
            "utm_campaign": None,
            "employee_id": None,
            "employee_name": None,
            "employee_email": None,
        },
    ]


def test_validate_silver_booking_record_accepts_valid_record():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "2026-07-29",
        "meeting_date": "2026-08-01",
        "meeting_day_of_week": "Saturday",
        "meeting_hour": 17,
    }

    validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_missing_required_field():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "2026-07-29",
        "meeting_date": "2026-08-01",
        "meeting_hour": 17,
    }

    with pytest.raises(ValueError, match="missing fields"):
        validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_bad_booking_date():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "07/29/2026",
        "meeting_date": "2026-08-01",
        "meeting_day_of_week": "Saturday",
        "meeting_hour": 17,
    }

    with pytest.raises(ValueError, match="Invalid booking_date format"):
        validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_bad_meeting_date():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "2026-07-29",
        "meeting_date": "08/01/2026",
        "meeting_day_of_week": "Saturday",
        "meeting_hour": 17,
    }

    with pytest.raises(ValueError, match="Invalid meeting_date format"):
        validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_non_numeric_meeting_hour():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "2026-07-29",
        "meeting_date": "2026-08-01",
        "meeting_day_of_week": "Saturday",
        "meeting_hour": "not-an-hour",
    }

    with pytest.raises(ValueError, match="Invalid meeting_hour value"):
        validate_silver_booking_record(record)


def test_validate_silver_booking_record_rejects_out_of_range_meeting_hour():
    record = {
        "booking_id": "booking-001",
        "channel": "facebook_paid_ads",
        "booking_date": "2026-07-29",
        "meeting_date": "2026-08-01",
        "meeting_day_of_week": "Saturday",
        "meeting_hour": 24,
    }

    with pytest.raises(ValueError, match="meeting_hour must be between 0 and 23"):
        validate_silver_booking_record(record)


def test_calculate_daily_calls_by_source_groups_by_booking_date_and_channel():
    records = build_sample_silver_booking_records()

    daily_calls = calculate_daily_calls_by_source(records)

    assert daily_calls == [
        {
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "booked_call_count": 2,
        },
        {
            "booking_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
            "booked_call_count": 1,
        },
        {
            "booking_date": "2026-07-30",
            "channel": "youtube_paid_ads",
            "booked_call_count": 1,
        },
    ]


def test_calculate_daily_calls_by_source_counts_distinct_booking_ids():
    records = [
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": "2026-07-29",
            "meeting_date": "2026-08-01",
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
        },
        {
            "booking_id": "booking-001",
            "channel": "facebook_paid_ads",
            "booking_date": "2026-07-29",
            "meeting_date": "2026-08-01",
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
        },
    ]

    daily_calls = calculate_daily_calls_by_source(records)

    assert daily_calls == [
        {
            "booking_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "booked_call_count": 1,
        }
    ]


def test_calculate_booking_trends_groups_by_booking_date():
    records = build_sample_silver_booking_records()

    booking_trends = calculate_booking_trends(records)

    assert booking_trends == [
        {
            "booking_date": "2026-07-29",
            "booked_call_count": 2,
        },
        {
            "booking_date": "2026-07-30",
            "booked_call_count": 2,
        },
    ]


def test_calculate_channel_attribution_groups_by_channel_and_campaign():
    records = build_sample_silver_booking_records()

    channel_attribution = calculate_channel_attribution(records)

    assert channel_attribution == [
        {
            "channel": "facebook_paid_ads",
            "utm_campaign": "summer_campaign",
            "booked_call_count": 2,
        },
        {
            "channel": "tiktok_paid_ads",
            "utm_campaign": "unknown_campaign",
            "booked_call_count": 1,
        },
        {
            "channel": "youtube_paid_ads",
            "utm_campaign": "youtube_campaign",
            "booked_call_count": 1,
        },
    ]


def test_calculate_booking_volume_by_time_slot_groups_by_day_and_hour():
    records = build_sample_silver_booking_records()

    volume_by_time_slot = calculate_booking_volume_by_time_slot(records)

    assert volume_by_time_slot == [
        {
            "meeting_day_of_week": "Monday",
            "meeting_hour": 9,
            "booked_call_count": 1,
        },
        {
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 17,
            "booked_call_count": 1,
        },
        {
            "meeting_day_of_week": "Saturday",
            "meeting_hour": 18,
            "booked_call_count": 1,
        },
        {
            "meeting_day_of_week": "Sunday",
            "meeting_hour": 10,
            "booked_call_count": 1,
        },
    ]


def test_calculate_employee_meeting_load_groups_by_employee():
    records = build_sample_silver_booking_records()

    employee_load = calculate_employee_meeting_load(records)

    assert employee_load == [
        {
            "employee_id": "employee-001",
            "employee_name": "Employee One",
            "employee_email": "employee.one@example.com",
            "booked_call_count": 2,
        },
        {
            "employee_id": "employee-002",
            "employee_name": "Employee Two",
            "employee_email": "employee.two@example.com",
            "booked_call_count": 1,
        },
        {
            "employee_id": "unknown_employee",
            "employee_name": "Unknown Employee",
            "employee_email": "unknown_email",
            "booked_call_count": 1,
        },
    ]


def test_calculate_booking_dashboard_kpis_returns_booking_summary():
    records = build_sample_silver_booking_records()

    kpis = calculate_booking_dashboard_kpis(records)

    assert kpis == {
        "total_bookings": 4,
        "channel_count": 3,
        "booking_date_count": 2,
        "meeting_date_count": 3,
    }


def test_build_booking_gold_tables_returns_expected_outputs():
    records = build_sample_silver_booking_records()

    gold_tables = build_booking_gold_tables(records)

    assert set(gold_tables.keys()) == {
        "gold_daily_calls_by_source",
        "gold_booking_trends",
        "gold_channel_attribution",
        "gold_booking_volume_by_time_slot",
        "gold_employee_meeting_load",
        "gold_booking_dashboard_kpis",
    }

    assert len(gold_tables["gold_daily_calls_by_source"]) == 3
    assert len(gold_tables["gold_booking_trends"]) == 2
    assert len(gold_tables["gold_channel_attribution"]) == 3
    assert len(gold_tables["gold_booking_volume_by_time_slot"]) == 4
    assert len(gold_tables["gold_employee_meeting_load"]) == 3

    assert gold_tables["gold_daily_calls_by_source"][0] == {
        "booking_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "booked_call_count": 2,
    }

    assert gold_tables["gold_booking_dashboard_kpis"] == {
        "total_bookings": 4,
        "channel_count": 3,
        "booking_date_count": 2,
        "meeting_date_count": 3,
    }