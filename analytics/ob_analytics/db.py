from contextlib import contextmanager
from typing import Any, Dict, Generator, Iterable, List, Optional

import pg8000

from .config import load_db_config


def get_connection() -> pg8000.dbapi.Connection:
    cfg = load_db_config()
    conn = pg8000.connect(
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        database=cfg.name,
    )
    return conn


@contextmanager
def db_conn() -> Generator[pg8000.dbapi.Connection, None, None]:
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetchall_dicts(
    conn: pg8000.dbapi.Connection,
    sql: str,
    params: Optional[Iterable[Any]] = None,
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [col[0] for col in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def fetchone_dict(
    conn: pg8000.dbapi.Connection,
    sql: str,
    params: Optional[Iterable[Any]] = None,
) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(sql, params or [])
    row = cur.fetchone()
    if row is None:
        return None
    cols = [col[0] for col in cur.description]
    return dict(zip(cols, row))
