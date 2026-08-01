import pytest

from src.transforms.silver_to_gold_spend import (
    build_spend_gold_tables,
    calculate_channel_spend_summary,
    calculate_daily_spend_by_channel,
    validate_silver_spend_record,
)


def build_sample_silver_spend_records():
    return [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 653.28,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "spend_usd": 487.59,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": "2026-07-29",
            "channel": "tiktok_paid_ads",
            "spend_usd": 345.12,
            "source_file": "spend_data_2026-07-29.json",
        },
        {
            "spend_date": "2026-07-30",
            "channel": "facebook_paid_ads",
            "spend_usd": 700.00,
            "source_file": "spend_data_2026-07-30.json",
        },
        {
            "spend_date": "2026-07-30",
            "channel": "youtube_paid_ads",
            "spend_usd": 500.00,
            "source_file": "spend_data_2026-07-30.json",
        },
        {
            "spend_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
            "spend_usd": 300.00,
            "source_file": "spend_data_2026-07-30.json",
        },
    ]


def test_validate_silver_spend_record_accepts_valid_record():
    record = {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "spend_usd": 653.28,
    }

    validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_missing_spend_date():
    record = {
        "channel": "facebook_paid_ads",
        "spend_usd": 653.28,
    }

    with pytest.raises(ValueError, match="missing fields"):
        validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_missing_channel():
    record = {
        "spend_date": "2026-07-29",
        "spend_usd": 653.28,
    }

    with pytest.raises(ValueError, match="missing fields"):
        validate_silver_spend_record(record)


def test_validate_silver_spend_record_rejects_missing_spend_usd():
    record = {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
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


def test_validate_silver_spend_record_rejects_non_numeric_spend():
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


def test_calculate_daily_spend_by_channel_groups_by_date_and_channel():
    records = build_sample_silver_spend_records()

    daily_spend = calculate_daily_spend_by_channel(records)

    assert daily_spend == [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "total_spend_usd": 653.28,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 345.12,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "youtube_paid_ads",
            "total_spend_usd": 487.59,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "facebook_paid_ads",
            "total_spend_usd": 700.00,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 300.00,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "youtube_paid_ads",
            "total_spend_usd": 500.00,
        },
    ]


def test_calculate_daily_spend_by_channel_sums_duplicate_date_channel_rows():
    records = [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 50.25,
        },
    ]

    daily_spend = calculate_daily_spend_by_channel(records)

    assert daily_spend == [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "total_spend_usd": 150.25,
        }
    ]


def test_calculate_channel_spend_summary_groups_by_channel():
    records = build_sample_silver_spend_records()

    channel_summary = calculate_channel_spend_summary(records)

    assert channel_summary == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 1353.28,
            "spend_day_count": 2,
            "average_daily_spend_usd": 676.64,
        },
        {
            "channel": "tiktok_paid_ads",
            "total_spend_usd": 645.12,
            "spend_day_count": 2,
            "average_daily_spend_usd": 322.56,
        },
        {
            "channel": "youtube_paid_ads",
            "total_spend_usd": 987.59,
            "spend_day_count": 2,
            "average_daily_spend_usd": 493.8,
        },
    ]


def test_calculate_channel_spend_summary_counts_distinct_spend_days():
    records = [
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 100.00,
        },
        {
            "spend_date": "2026-07-29",
            "channel": "facebook_paid_ads",
            "spend_usd": 50.00,
        },
        {
            "spend_date": "2026-07-30",
            "channel": "facebook_paid_ads",
            "spend_usd": 200.00,
        },
    ]

    channel_summary = calculate_channel_spend_summary(records)

    assert channel_summary == [
        {
            "channel": "facebook_paid_ads",
            "total_spend_usd": 350.00,
            "spend_day_count": 2,
            "average_daily_spend_usd": 175.00,
        }
    ]


def test_build_spend_gold_tables_returns_expected_outputs():
    records = build_sample_silver_spend_records()

    gold_tables = build_spend_gold_tables(records)

    assert set(gold_tables.keys()) == {
        "gold_daily_spend_by_channel",
        "gold_channel_spend_summary",
    }

    assert len(gold_tables["gold_daily_spend_by_channel"]) == 6
    assert len(gold_tables["gold_channel_spend_summary"]) == 3

    assert gold_tables["gold_daily_spend_by_channel"][0] == {
        "spend_date": "2026-07-29",
        "channel": "facebook_paid_ads",
        "total_spend_usd": 653.28,
    }

    assert gold_tables["gold_channel_spend_summary"][0] == {
        "channel": "facebook_paid_ads",
        "total_spend_usd": 1353.28,
        "spend_day_count": 2,
        "average_daily_spend_usd": 676.64,
    }