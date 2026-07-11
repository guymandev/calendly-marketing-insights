from src.metrics.gold_metrics import (
    calculate_booking_trends,
    calculate_booking_volume_by_time_slot,
    calculate_channel_attribution,
    calculate_cpb_by_channel,
    calculate_daily_calls_by_source,
    calculate_dashboard_kpis,
    calculate_employee_meeting_load,
)


BOOKINGS = [
    {
        "booking_id": "booking_1",
        "booking_date": "2025-07-09",
        "channel": "facebook_paid_ads",
        "utm_campaign": "campaign_a",
        "meeting_day_of_week": "Wednesday",
        "meeting_hour": 17,
        "meeting_week_start_date": "2025-07-07",
        "employee_id": "employee_1",
        "employee_name": "Test Employee One",
    },
    {
        "booking_id": "booking_2",
        "booking_date": "2025-07-09",
        "channel": "facebook_paid_ads",
        "utm_campaign": "campaign_a",
        "meeting_day_of_week": "Wednesday",
        "meeting_hour": 17,
        "meeting_week_start_date": "2025-07-07",
        "employee_id": "employee_1",
        "employee_name": "Test Employee One",
    },
    {
        "booking_id": "booking_3",
        "booking_date": "2025-07-10",
        "channel": "youtube_paid_ads",
        "utm_campaign": "campaign_b",
        "meeting_day_of_week": "Thursday",
        "meeting_hour": 10,
        "meeting_week_start_date": "2025-07-07",
        "employee_id": "employee_2",
        "employee_name": "Test Employee Two",
    },
    {
        "booking_id": "booking_4",
        "booking_date": "2025-07-16",
        "channel": "facebook_paid_ads",
        "utm_campaign": None,
        "meeting_day_of_week": "Wednesday",
        "meeting_hour": 12,
        "meeting_week_start_date": "2025-07-14",
        "employee_id": "employee_1",
        "employee_name": "Test Employee One",
    },
]


SPEND_RECORDS = [
    {
        "spend_date": "2025-07-09",
        "channel": "facebook_paid_ads",
        "spend_usd": 300.00,
    },
    {
        "spend_date": "2025-07-10",
        "channel": "youtube_paid_ads",
        "spend_usd": 120.00,
    },
    {
        "spend_date": "2025-07-16",
        "channel": "facebook_paid_ads",
        "spend_usd": 150.00,
    },
]


def test_calculate_daily_calls_by_source():
    results = calculate_daily_calls_by_source(BOOKINGS)

    assert {
        "booking_date": "2025-07-09",
        "channel": "facebook_paid_ads",
        "booked_calls": 2,
    } in results

    assert {
        "booking_date": "2025-07-10",
        "channel": "youtube_paid_ads",
        "booked_calls": 1,
    } in results

    assert {
        "booking_date": "2025-07-16",
        "channel": "facebook_paid_ads",
        "booked_calls": 1,
    } in results


def test_calculate_cpb_by_channel():
    results = calculate_cpb_by_channel(BOOKINGS, SPEND_RECORDS)

    facebook_result = next(
        item for item in results if item["channel"] == "facebook_paid_ads"
    )
    youtube_result = next(
        item for item in results if item["channel"] == "youtube_paid_ads"
    )

    assert facebook_result["total_spend"] == 450.00
    assert facebook_result["total_bookings"] == 3
    assert facebook_result["cost_per_booking"] == 150.00

    assert youtube_result["total_spend"] == 120.00
    assert youtube_result["total_bookings"] == 1
    assert youtube_result["cost_per_booking"] == 120.00


def test_calculate_booking_trends_matches_daily_calls_logic():
    trends = calculate_booking_trends(BOOKINGS)
    daily_calls = calculate_daily_calls_by_source(BOOKINGS)

    assert trends == daily_calls


def test_calculate_channel_attribution():
    results = calculate_channel_attribution(BOOKINGS, SPEND_RECORDS)

    facebook_campaign_a = next(
        item
        for item in results
        if item["channel"] == "facebook_paid_ads"
        and item["campaign"] == "campaign_a"
    )

    facebook_unknown_campaign = next(
        item
        for item in results
        if item["channel"] == "facebook_paid_ads"
        and item["campaign"] == "unknown_campaign"
    )

    assert facebook_campaign_a["total_bookings"] == 2
    assert facebook_campaign_a["total_spend"] == 450.00
    assert facebook_campaign_a["cost_per_booking"] == 225.00

    assert facebook_unknown_campaign["total_bookings"] == 1
    assert facebook_unknown_campaign["total_spend"] == 450.00
    assert facebook_unknown_campaign["cost_per_booking"] == 450.00


def test_calculate_booking_volume_by_time_slot():
    results = calculate_booking_volume_by_time_slot(BOOKINGS)

    assert {
        "meeting_day_of_week": "Wednesday",
        "meeting_hour": 17,
        "channel": "facebook_paid_ads",
        "bookings": 2,
    } in results

    assert {
        "meeting_day_of_week": "Thursday",
        "meeting_hour": 10,
        "channel": "youtube_paid_ads",
        "bookings": 1,
    } in results


def test_calculate_employee_meeting_load():
    results = calculate_employee_meeting_load(BOOKINGS)

    employee_1 = next(
        item for item in results if item["employee_id"] == "employee_1"
    )
    employee_2 = next(
        item for item in results if item["employee_id"] == "employee_2"
    )

    assert employee_1["employee_name"] == "Test Employee One"
    assert employee_1["total_meetings"] == 3
    assert employee_1["number_of_weeks"] == 2
    assert employee_1["avg_meetings_per_week"] == 1.5

    assert employee_2["employee_name"] == "Test Employee Two"
    assert employee_2["total_meetings"] == 1
    assert employee_2["number_of_weeks"] == 1
    assert employee_2["avg_meetings_per_week"] == 1.0


def test_calculate_dashboard_kpis():
    results = calculate_dashboard_kpis(BOOKINGS, SPEND_RECORDS)

    assert results["total_bookings"] == 4
    assert results["total_spend"] == 570.00
    assert results["average_cpb"] == 142.50