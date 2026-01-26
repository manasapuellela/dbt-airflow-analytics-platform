import pandas as pd

from src.ingest.ingest_to_s3 import add_pickup_date


def test_add_pickup_date_creates_date_column():
    frame = pd.DataFrame(
        {
            "tpep_pickup_datetime": ["2024-01-01 08:00:00", "2024-01-02 09:30:00"]
        }
    )

    result = add_pickup_date(frame)

    assert "pickup_date" in result.columns
    assert str(result.loc[0, "pickup_date"]) == "2024-01-01"
