#!/usr/bin/env python3
"""
Blueprint  /grouping   –   Netzwerk-Analyse von Werken
──────────────────────────────────────────────────────
•  GET   /grouping                 → Auswahl-Seite (grouping.html)
•  POST  /grouping/analyze         → erzeugt JSON (Nodes + Edges)
•  GET   /grouping/data/<gid>      → reines JSON für JS-Fetch
•  GET   /grouping/network/<gid>   → Ergebnisse (network.html)
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Set

from dotenv import load_dotenv
from flask import Blueprint, jsonify, render_template, request
from mysql.connector import pooling  # ✅ sauberer Import

# ─────────────────────────  DB-Pool  ──────────────────────────────────────
load_dotenv()

POOL = pooling.MySQLConnectionPool(
    pool_name="bibbud_group",
    pool_size=int(os.getenv("POOL_SIZE", "5")),
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME", "Bibbud"),
    charset="utf8mb4",
    autocommit=True,
)

# ─────────────────────────  Blueprint  ────────────────────────────────────
grouping_bp = Blueprint(
    "grouping",
    __name__,
    url_prefix="/grouping",
    template_folder="templates",
    static_folder="static",
)

# ─────────────────────────  Helper  ───────────────────────────────────────
_RE_FIRST_AUTHOR = re.compile(r"^\s*([A-Za-zÄÖÜäöüß\-’']+)")


def _first_author(auth: str | None) -> str:
    """Nachnamen des ersten Autors in Kleinbuchstaben zurückgeben."""
    if not auth:
        return ""
    match = _RE_FIRST_AUTHOR.match(auth)
    return (match.group(1) if match else auth).lower()


def _fetch_own_works() -> List[Dict[str, Any]]:
    """
    Werke, zu denen der aktuelle Benutzer Dokumente hochgeladen hat, inkl.
    In-/Out-Degree. (Der Benutzer-Filter müsste ggf. ergänzt werden.)
    """
    sql = """
        SELECT  w.id,
                w.title,
                w.authors,
                w.year,
                COALESCE(cin.c_in , 0)  AS c_in,
                COALESCE(cout.c_out, 0) AS c_out,
                1 AS own
          FROM works w
          JOIN documents d ON d.work_id = w.id              -- nur eigene Uploads
          LEFT JOIN (
              SELECT to_work_id   AS wid, SUM(count) c_in
                FROM citations
            GROUP BY to_work_id
          ) cin  ON cin.wid  = w.id
          LEFT JOIN (
              SELECT from_work_id AS wid, SUM(count) c_out
                FROM citations
            GROUP BY from_work_id
          ) cout ON cout.wid = w.id
         GROUP BY w.id
         ORDER BY w.year DESC
    """
    cnx = POOL.get_connection()
    cur = cnx.cursor(dictionary=True)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows


# ─────────────────────────  Netzwerk-Sampler  ────────────────────────────
def _collect(ids: List[int]) -> Dict[str, Any]:
    """
    Erstellt das komplette Netzwerk:
      • primäre Nodes (User-Auswahl, own = 1)
      • zusätzliche Nodes (indirekt vorkommende Werke, own = 0)
      • Kanten-Typ »cite«   (gerichtet, Gewicht = Zitationshäufigkeit)
      • Kanten-Typ »author« (gleicher Erstautor, ungerichtet)
    """
    if not ids:
        return {"nodes": [], "edges": []}

    placeholder = ",".join(["%s"] * len(ids))
    cnx = POOL.get_connection()
    cur = cnx.cursor(dictionary=True)

    # 1) Primäre Nodes -----------------------------------------------------
    cur.execute(
        f"""
        SELECT id, title, authors, year, 1 AS own
          FROM works
         WHERE id IN ({placeholder})
        """,
        ids,
    )
    nodes: List[Dict[str, Any]] = cur.fetchall()
    node_ids: Set[int] = {n["id"] for n in nodes}

    # 2) Zitations-Kanten + fremde Node-IDs -------------------------------
    cur.execute(
        f"""
        SELECT from_work_id, to_work_id, SUM(count) AS w
          FROM citations
         WHERE from_work_id IN ({placeholder})
            OR to_work_id   IN ({placeholder})
         GROUP BY from_work_id, to_work_id
        """,
        ids * 2,  # zweimal, weil zwei Platzhalter-Blöcke
    )

    cite_edges: List[Dict[str, Any]] = []
    external_ids: Set[int] = set()

    for row in cur.fetchall():
        src = int(row["from_work_id"])
        dst = int(row["to_work_id"])
        cite_edges.append(
            {
                "source": src,
                "target": dst,
                "weight": int(row["w"]),
                "type": "cite",
            }
        )
        if src not in node_ids:
            external_ids.add(src)
        if dst not in node_ids:
            external_ids.add(dst)

    # 3) fehlende Nodes nachladen -----------------------------------------
    if external_ids:
        ph_ext = ",".join(["%s"] * len(external_ids))
        cur.execute(
            f"""
            SELECT id, title, authors, year, 0 AS own
              FROM works
             WHERE id IN ({ph_ext})
            """,
            list(external_ids),
        )
        nodes.extend(cur.fetchall())
        node_ids |= external_ids

    # 4) Author-Kanten -----------------------------------------------------
    by_author: Dict[str, List[int]] = {}
    for n in nodes:
        by_author.setdefault(_first_author(n["authors"]), []).append(n["id"])

    author_edges: List[Dict[str, Any]] = []
    for same in by_author.values():
        if len(same) < 2:
            continue
        same.sort()
        for i, src in enumerate(same):
            for dst in same[i + 1 :]:
                author_edges.append(
                    {
                        "source": src,
                        "target": dst,
                        "weight": 1,
                        "type": "author",
                    }
                )

    cur.close()
    cnx.close()
    return {"nodes": nodes, "edges": cite_edges + author_edges}


# ─────────────────────────  Routes  ───────────────────────────────────────
@grouping_bp.get("/")
def select_page():
    """Erste Seite – Werkauswahl."""
    return render_template("grouping.html", works=_fetch_own_works())


@grouping_bp.post("/analyze")
def analyze():
    ids = [int(x) for x in request.form.getlist("work_id")]
    if not ids:
        return jsonify(error="keine Auswahl"), 400

    gid = uuid.uuid4().hex
    cache = Path(tempfile.gettempdir(), f"bib_group_{gid}.json")
    cache.write_text(json.dumps(_collect(ids), ensure_ascii=False))

    return jsonify(group_id=gid)


@grouping_bp.get("/data/<gid>")
def data(gid: str):
    cache = Path(tempfile.gettempdir(), f"bib_group_{gid}.json")
    if not cache.exists():
        return jsonify(error="group not found"), 404
    return jsonify(json.loads(cache.read_text()))


@grouping_bp.get("/network/<gid>")
def network_view(gid: str):
    cache = Path(tempfile.gettempdir(), f"bib_group_{gid}.json")
    nodes: List[Dict[str, Any]] = []
    if cache.exists():
        try:
            nodes = json.loads(cache.read_text())["nodes"]
        except (json.JSONDecodeError, KeyError):
            pass
    return render_template("network.html", group_id=gid, nodes=nodes)