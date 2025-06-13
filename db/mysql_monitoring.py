#!/usr/bin/env python3
"""
Streamlit-Dashboard  ·  MySQL Live Monitor & Connection Killer
──────────────────────────────────────────────────────────────
• Live-Metriken (Threads / Uptime …)
• vollständige PROCESSLIST als Tabelle
• Balkendiagramm “Connections per User”
• Kill-Button pro Thread-ID
• Bulk‑Kill schlafender Connections pro DB
"""
from __future__ import annotations
import os, time
from collections import Counter
from typing import Dict, List

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import mysql.connector as mysql

# ─────────────────────── 0) ENV & DB-Connect  ─────────────────────────────
load_dotenv()

DB_CONFIG: Dict[str, str] = {
    "host"    : os.getenv("DB_HOST", "127.0.0.1"),
    "user"    : os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "mysql"),
    "charset" : "utf8mb4",
    "autocommit": True,
}

def get_cnx():
    """Create a fresh connection with autocommit so KILL executes instantly."""
    return mysql.connect(**DB_CONFIG)

# ─────────────────────── 1) Helper: safe Streamlit rerun  ─────────────────

def st_rerun():  # pylint: disable=invalid-name
    """Compatibility wrapper for Streamlit ≤1.26 (experimental_rerun) and ≥1.27 (rerun)."""
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# ─────────────────────── 2) DB Helper Functions  ──────────────────────────
STATUS_KEYS = [
    "Threads_connected", "Threads_running", "Max_used_connections",
    "Uptime", "Connections"
]

def fetch_status() -> Dict[str, int]:
    q = (
        "SHOW STATUS WHERE Variable_name IN ("
        + ",".join(f"'{k}'" for k in STATUS_KEYS) + ")"
    )
    with get_cnx() as cnx, cnx.cursor() as cur:
        cur.execute(q)
        return {k: int(v) for k, v in cur.fetchall()}


def fetch_processlist() -> pd.DataFrame:
    with get_cnx() as cnx, cnx.cursor(dictionary=True) as cur:
        cur.execute("SHOW FULL PROCESSLIST")
        df = pd.DataFrame(cur.fetchall())
    if not df.empty:
        df = df.drop(columns=["State", "Info"], errors="ignore")
        df["Time"] = df["Time"].astype(int)
    return df


def kill_connection(thread_id: int) -> None:
    """Schließt komplette Connection (nicht nur Query)."""
    with get_cnx() as cnx, cnx.cursor() as cur:
        cur.execute(f"KILL CONNECTION {thread_id}")


def kill_idle_connections(db_name: str, only_sleep: bool = True) -> List[int]:
    """Killt alle Verbindungen zu *db_name* (optional nur 'Sleep').
    Gibt Liste der gekillten Thread‑IDs zurück."""
    cond = "AND command = 'Sleep'" if only_sleep else ""
    sql_select = (
        "SELECT id FROM information_schema.PROCESSLIST "
        "WHERE db = %s " + cond
    )
    killed: List[int] = []
    with get_cnx() as cnx, cnx.cursor() as cur:
        cur.execute(sql_select, (db_name,))
        ids = [row[0] for row in cur.fetchall()]
        for tid in ids:
            try:
                cur.execute(f"KILL CONNECTION {tid}")
                killed.append(tid)
            except mysql.Error:
                pass  # evtl. schon beendet
    return killed

# ─────────────────────── 3) Streamlit UI  ─────────────────────────────────
st.set_page_config(page_title="MySQL Monitor", layout="wide")
st.title("MySQL Live Monitor")

# ---------- 3.1  Sidebar / Steuerung --------------------------------------
with st.sidebar:
    st.header("Steuerung")
    auto = st.checkbox("Auto‑Refresh", value=True, key="auto")
    interval = st.slider("Intervall (Sek.)", 3, 60, 10, step=1, key="interval")

    st.markdown("---")
    st.subheader("Bulk‑Killer")
    db_filter = st.text_input("DB‑Name", "Bibbud", key="db_filter")
    col_k1, col_k2 = st.columns([2,1])
    with col_k1:
        only_sleep = st.checkbox("nur Sleep‑Threads", value=True, key="only_sleep")
    with col_k2:
        if st.button("🔥 Alle beenden", key="btn_kill_all"):
            killed = kill_idle_connections(db_filter.strip(), only_sleep)
            if killed:
                st.success(f"{len(killed)} Threads gekillt: {', '.join(map(str, killed))}")
                st.session_state["_force_refresh"] = True   # Flag für main‑area
            else:
                st.info("Keine passenden Threads gefunden.")

# ---------- 3.2  Auto‑Refresh ---------------------------------------------
if auto or st.session_state.pop("_force_refresh", False):
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=interval * 1000, key="autoreload")

# ─────────────────────── 4) Daten laden -----------------------------------
status  = fetch_status()
proclst = fetch_processlist()

# ---------- 4.1  KPI‑Tiles -------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5, gap="large")
k1.metric("Threads Connected", status.get("Threads_connected", 0))
k2.metric("Threads Running",   status.get("Threads_running", 0))
k3.metric("Max Used",          status.get("Max_used_connections", 0))
k4.metric("Total Connections", status.get("Connections", 0))
k5.metric("Uptime (sec)",      status.get("Uptime", 0))

st.divider()

# ---------- 4.2  Prozess‑Tabelle -----------------------------------------
st.subheader("Aktive Connections")

if proclst.empty:
    st.info("Es sind aktuell keine Verbindungen offen.")
else:
    cols_hdr = st.columns((2, 2, 2, 1, 1, 1, 1))  # User | DB | Host | Time | Cmd | ID | Kill
    for c, h in zip(cols_hdr, ["User", "DB", "Host", "Time", "Cmd", "Id", ""]):
        c.markdown(f"**{h}**")

    for _idx, row in proclst.iterrows():
        c_user, c_db, c_host, c_time, c_cmd, c_id, c_btn = st.columns((2, 2, 2, 1, 1, 1, 1))
        c_user.write(row["User"])
        c_db.write(row["db"] or "-")
        c_host.write(row["Host"].split(":")[0])
        c_time.write(row["Time"])
        c_cmd.write(row["Command"])
        c_id.write(row["Id"])
        if c_btn.button("✖", key=f"kill-{row['Id']}"):
            try:
                kill_connection(int(row["Id"]))
                st.success(f"Connection {row['Id']} gekillt.")
                st_rerun()
            except mysql.Error as err:
                st.error(f"Konnte Thread {row['Id']} nicht beenden: {err}")

st.divider()

# ---------- 4.3  Verbindungen pro User ------------------------------------
if not proclst.empty:
    counts = Counter(proclst["User"])
    df_chart = (
        pd.DataFrame(counts.items(), columns=["User", "Connections"])
        .sort_values("Connections", ascending=False)
        .set_index("User")
    )
    st.subheader("Connections nach Benutzer")
    st.bar_chart(df_chart)

# ---------- 4.4  Fußnote --------------------------------------------------
st.caption("Aktualisiert: " + time.strftime("%H:%M:%S"))
