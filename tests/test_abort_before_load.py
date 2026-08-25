import pytest

from ingestion.csv_ingestion.csv_loader import (
    ValidationStats,
    check_rejection_rate,
)


def test_twenty_percent_bad_file_aborts() -> None:
    stats = ValidationStats(
        total_rows=100,
        rejected_rows=20,
    )

    with pytest.raises(ValueError):
        check_rejection_rate(stats)

