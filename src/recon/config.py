import os
from decimal import Decimal
from pathlib import Path

DATA_PATH = Path(os.environ.get("DATA_PATH", "data"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "local")

WAREHOUSE_FILE = "warehouse.duckdb"

TOLERANCE = Decimal("0.01")
WINDOW_DAYS = 7
MISMATCH_RATE_ALERT = 0.05
PROCESSORS = ("paysettler",)


def warehouse_path() -> Path:
    return DATA_PATH / WAREHOUSE_FILE
