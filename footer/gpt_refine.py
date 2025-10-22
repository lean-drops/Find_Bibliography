"""
Nutzt die OpenAI Responses API für die strukturierte Extraktion von Fußnoten
aus 'rohem' Bottom-Text einer Seite. Fällt automatisch aus, wenn kein API-Key
vorhanden ist. Liefert JSON-Objekte mit marker/text und Heuristik-Score.
"""

from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    from openai import OpenAI  # offizielles SDK
    _OPENAI_OK = True
except Exception:
    _OPENAI_OK = False

def dprint(msg: str) -> None:
    print(msg, flush=True)

DEFAULT_MODEL = os.environ.get("AZK_GPT_MODEL", "gpt-4o-mini")  # günstig/schnell

SYSTEM = (
    "You extract ONLY footnotes from document bottom text. "
    "Return STRICT JSON with this shape:\n"
    "{ \"footnotes\": [ {\"marker\": \"string|null\", \"text\": \"string\" }, ... ] }\n"
    "Rules:\n"
    "- Footnotes are the bibliographic or commentary notes referenced from body text.\n"
    "- Ignore page numbers, headers, captions, figure legends unless clearly part of footnotes.\n"
    "- Merge line-wrapped notes.\n"
    "- Keep numbering/marker (1., 1), [1], *, †, ‡ ...) when present; else marker=null.\n"
    "- Output plain text (no HTML), no explanations."
)

def refine_bottom_text_with_gpt(bottom_text: str,
                                locale_hint: str = "de") -> Optional[List[Dict[str, Any]]]:
    """
    bottom_text: zusammenhängender Text aus dem Fußnoten-Bereich einer Seite.
    Rückgabe: Liste von Dicts {marker, text, gpt_score}, oder None bei Deaktivierung/Fehler.
    """
    if not _OPENAI_OK or not os.environ.get("OPENAI_API_KEY"):
        dprint("[WARN][GPT] Kein OPENAI_API_KEY oder openai-SDK nicht installiert – GPT-Schritt wird übersprungen.")
        return None
    try:
        client = OpenAI()
        prompt = (
            f"Language hint: {locale_hint}\n"
            "Extract footnotes from this page-bottom text:\n"
            "----\n" + bottom_text + "\n----"
        )
        dprint(f"[INFO][GPT] Call Responses API (model={DEFAULT_MODEL}) …")
        resp = client.responses.create(
            model=DEFAULT_MODEL,
            input=prompt,
            instructions=SYSTEM,
            temperature=0.0,
            max_output_tokens=1500,
        )
        out = resp.output_text or ""
        dprint(f"[DEBUG][GPT] Raw output_text length: {len(out)}")
        # Robust JSON-Find: suche erstes { … }
        first_brace = out.find("{")
        last_brace  = out.rfind("}")
        if first_brace >= 0 and last_brace > first_brace:
            snippet = out[first_brace:last_brace+1]
        else:
            snippet = out
        data = json.loads(snippet)
        items = data.get("footnotes") or []
        cleaned: List[Dict[str, Any]] = []
        for it in items:
            marker = it.get("marker")
            text = (it.get("text") or "").strip()
            if not text:
                continue
            cleaned.append({"marker": (marker if marker else None),
                            "text": text,
                            "gpt_score": 0.95})
        dprint(f"[INFO][GPT] Parsed footnotes: {len(cleaned)}")
        return cleaned
    except Exception as e:
        dprint(f"[ERROR][GPT] Responses API Fehler: {e}")
        return None


