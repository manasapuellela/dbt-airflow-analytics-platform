import os

DEFAULT_WAREHOUSE_DB_URI = "postgresql://warehouse-db:5432/warehouse"


def get_warehouse_db_uri() -> str:
    """Return the warehouse database URI for containerized runs."""
    return os.getenv("WAREHOUSE_DB_URI", DEFAULT_WAREHOUSE_DB_URI)
