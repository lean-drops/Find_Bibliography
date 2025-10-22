# db_config.py  (optimiert)
from __future__ import annotations
import os
from typing import Any, Iterable, Sequence
from dotenv import load_dotenv
import mysql.connector as mysql
from mysql.connector.pooling import MySQLConnectionPool, CMySQLConnection

load_dotenv()

DB_CONFIG: dict[str, Any] = {
    "host":      os.getenv("DB_HOST", "127.0.0.1"),
    "port":      int(os.getenv("DB_PORT", "3306")),
    "user":      os.getenv("DB_USER", "root"),
    "password":  os.getenv("DB_PASSWORD", ""),
    "database":  os.getenv("DB_NAME", "Bibbud"),
    "autocommit": False,
    "raise_on_warnings": True,
    "use_pure": False,
    "charset": "utf8mb4",
}

_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
_POOL: MySQLConnectionPool | None = None      # ← noch nicht erstellt

# ------------------------------------------------------------------ #
# 1) interner Helper: Pool bei Bedarf initialisieren                 #
# ------------------------------------------------------------------ #
def _get_pool() -> MySQLConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = MySQLConnectionPool(
            pool_name="global_mysql_pool",
            pool_size=_POOL_SIZE,
            pool_reset_session=True,
            **DB_CONFIG,
        )
    return _POOL

# ------------------------------------------------------------------ #
# 2) Public Helper – nutzen intern _get_pool()                       #
# ------------------------------------------------------------------ #
def get_conn() -> CMySQLConnection:
    """Verbindung aus dem globalen Pool holen (lazy-init)."""
    return _get_pool().get_connection()

def fetch_all(sql: str, params: Sequence[Any] | None = None):
    with get_conn() as cnx, cnx.cursor(dictionary=True) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def fetch_one(sql: str, params: Sequence[Any] | None = None):
    with get_conn() as cnx, cnx.cursor(dictionary=True) as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()

def execute(sql: str, params: Sequence[Any] | None = None) -> int:
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(sql, params or ())
        cnx.commit()
        return cur.rowcount

def executemany(sql: str, seq_of_params: Iterable[Sequence[Any]]) -> int:
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.executemany(sql, seq_of_params)
        cnx.commit()
        return cur.rowcount

# ------------------------------------------------------------------ #
# 3) Optionaler Selbst-Test                                          #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    print("Lazy pool size:", _POOL_SIZE)
    print("Threads_connected:", fetch_one("SHOW STATUS LIKE 'Threads_connected'")["Value"])