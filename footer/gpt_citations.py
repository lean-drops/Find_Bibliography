#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
gpt_citations.py
----------------
Optionaler GPT-Schritt (Responses API) zur strukturierten Zitat-Extraktion aus
einem Fußnotentext. Gibt Liste von Citation-Dicts zurück (kompatibel zu DB-Feldern).
Wenn kein OPENAI_API_KEY vorhanden ist, liefert None.

Env:
- OPENAI_API_KEY
- AZK_GPT_MODEL (optional, default 'gpt-4o-mini')
"""

from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Optional

def dprint(msg: str) -> None:
    print(msg, flush=True)

try:
    from openai import OpenAI
    _OPENAI_OK = True
except Exception:
    _OPENAI_OK = False

DEFAULT_MODEL = os.environ.get("AZK_GPT_MODEL", "gpt-4o-mini")

SYSTEM = (
    "Extract ALL bibliographic citations from the footnote text. "
    "Return STRICT JSON:\n"
    "{ \"citations\": [\n"
    "  {\"raw_text\":\"...\",\"type\":\"article|book|chapter|web|unknown\",\n"
    "   \"authors\":[\"Last, First\", ...],\"year\":\"YYYY|null\",\"title\":\"...|null\",\n"
    "   \"container_title\":\"...|null\",\"volume\":\"...|null\",\"issue\":\"...|null\",\n"
    "   \"pages\":\"...|null\",\"publisher\":\"...|null\",\"place\":\"...|null\",\n"
    "   \"doi\":\"...|null\",\"url\":\"...|null\" }\n"
    "]}\n"
    "Rules: keep line-wrapped text merged, keep diacritics, keep punctuation inside titles. "
    "Return only JSON, no comments."
)

def refine_citations_with_gpt(footnote_text: str, locale_hint: str = "de") -> Optional[List[Dict[str, Any]]]:
    if not _OPENAI_OK or not os.environ.get("OPENAI_API_KEY"):
        dprint("[WARN][GPT] deaktiviert – kein OPENAI_API_KEY oder SDK fehlt.")
        return None
    try:
        client = OpenAI()
        prompt = f"Language hint: {locale_hint}\nFootnote text:\n----\n{footnote_text}\n----"
        dprint(f"[INFO][GPT] Responses API call (model={DEFAULT_MODEL}) …")
        resp = client.responses.create(
            model=DEFAULT_MODEL,
            input=prompt,
            instructions=SYSTEM,
            temperature=0.0,
            max_output_tokens=2000,
        )
        out = resp.output_text or ""
        # JSON robust parsen
        a = out.find("{"); b = out.rfind("}")
        snippet = out[a:b+1] if (a >= 0 and b > a) else out
        data = json.loads(snippet)
        items = data.get("citations") or []
        cleaned = []
        for it in items:
            cleaned.append({
                "raw_text": (it.get("raw_text") or "").strip(),
                "type": (it.get("type") or "unknown").strip(),
                "authors": [str(x).strip() for x in (it.get("authors") or []) if str(x).strip()],
                "year": (it.get("year") or "") or None,
                "title": (it.get("title") or "") or None,
                "container_title": (it.get("container_title") or "") or None,
                "volume": (it.get("volume") or "") or None,
                "issue": (it.get("issue") or "") or None,
                "pages": (it.get("pages") or "") or None,
                "publisher": (it.get("publisher") or "") or None,
                "place": (it.get("place") or "") or None,
                "doi": (it.get("doi") or "") or None,
                "url": (it.get("url") or "") or None,
            })
        dprint(f"[INFO][GPT] citations={len(cleaned)}")
        return cleaned
    except Exception as e:
        dprint(f"[ERROR][GPT] {e}")
        return None