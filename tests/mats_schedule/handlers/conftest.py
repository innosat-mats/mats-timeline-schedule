from pathlib import Path
import pytest  # type: ignore


@pytest.fixture
def csv_file() -> Path:
    return Path(__file__).parent / "files" / "20230309_timeline_schedule.csv"
