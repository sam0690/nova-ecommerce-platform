import pytest

from ingestion.csv_ingestion.csv_loader import (
    ValidationStats,
    check_rejection_rate,
)


def test_zero_rejections_pass() -> None:
    stats = ValidationStats(
        total_rows=100,
        rejected_rows=0,
    )

    check_rejection_rate(stats)


def test_exactly_one_percent_passes() -> None:
    stats = ValidationStats(
        total_rows=100,
        rejected_rows=1,
    )

    check_rejection_rate(stats)


def test_more_than_one_percent_fails() -> None:
    stats = ValidationStats(
        total_rows=100,
        rejected_rows=2,
    )

    with pytest.raises(ValueError, match="Too many rejected rows"):
        check_rejection_rate(stats)


def test_twenty_percent_fails() -> None:
    stats = ValidationStats(
        total_rows=100,
        rejected_rows=20,
    )

    with pytest.raises(ValueError, match="Too many rejected rows"):
        check_rejection_rate(stats)
