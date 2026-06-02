import os
from dataclasses import dataclass


@dataclass
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


def load_db_config() -> DbConfig:
    return DbConfig(
        host=os.getenv("OB_DB_HOST", "localhost"),
        port=int(os.getenv("OB_DB_PORT", "5432")),
        name=os.getenv("OB_DB_NAME", "orderbooks"),
        user=os.getenv("OB_DB_USER", "postgres"),
        password=os.getenv("OB_DB_PASS", "postgres"),
    )


@dataclass
class ApiConfig:
    host: str = os.getenv("OB_API_HOST", "0.0.0.0")
    port: int = int(os.getenv("OB_API_PORT", "8000"))


@dataclass
class EtlConfig:
    batch_size: int = int(os.getenv("OB_ETL_BATCH_SIZE", "5000"))
    # Giełda referencyjna – używana w ETL do liczenia gainów vs ref
    ref_exchange: str = os.getenv("OB_REF_EXCHANGE", "BINANCE_FUT").upper()
    # Łączne fee (source + ref + inne) jako ułamek, np. 0.002 = 0.2%
    total_fees_pct: float = float(os.getenv("OB_TOTAL_FEES_PCT", "0.0006"))
