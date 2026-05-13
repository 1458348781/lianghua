from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_DIR = DATA_DIR / "database"
DATABASE_PATH = DATABASE_DIR / "market.sqlite"
WEB_DIR = PROJECT_ROOT / "web"

DEFAULT_SYMBOLS = ["000001.SZ", "600519.SH", "000300.SH"]


def ensure_directories() -> None:
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
