"""
search_term.py — OpenAI-basiertes Konzept-Fetching mit Cache und Seed-Garantie.

Nutzung
-------
python organizer/keyword/search_term.py
.env im Projekt-Root mit OPENAI_API_KEY (optional OPENAI_MODEL).

Funktion
--------
generate_keyword_candidates(seed_term, language, domain_hint, model, use_cache=True)
→ Ruft OpenAI ab (v1 oder legacy), cached unter ~/.cache/azk_concepts/<sha1>.json,
erzwingt, dass der Seed-Begriff in 'core' enthalten ist.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

# SDK-Erkennung
_SDK_PATH = "none"
_OpenAIClass = None
_openai_legacy = None
try:
    from openai import OpenAI  # v1.x
    _OpenAIClass = OpenAI
    _SDK_PATH = "v1"
except Exception:
    try:
        import openai as _openai_legacy  # v0.x
        _SDK_PATH = "legacy"
    except Exception:
        _SDK_PATH = "none"

CACHE_DIR = Path(os.getenv("KEYWORD_CACHE_DIR", "~/.cache/azk_concepts")).expanduser()
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM_PROMPT = """Du bist ein wissenschaftlicher Terminologe.
Ziel: Für einen Seed-Begriff im Kontext der spätmittelalterlichen Geschichtsschreibung (DE/CH/AT)
eine präzise, kuratierte Liste an Suchbegriffen erzeugen, die dasselbe Konzept signalisieren
(Synonyme, Formulierungen, lateinische Äquivalente, ältere Orthographie, juristische Formeln).
Gib ausschließlich JSON zurück, Schema:
{
  "seed": "<string>",
  "concept_hint": "<string>",
  "terms_by_category": {
    "core": [],
    "truce": [],
    "settlement": [],
    "legal_instrument": [],
    "end_of_hostilities": [],
    "latin": [],
    "historic_variants": []
  },
  "notes": "<max 3 Sätze>"
}
Regeln:
- Max 12 Einträge pro Kategorie.
- Nur relevante Terme; vermeide generische Wortwahl.
- Ambige Terme wie „Abschied“, „Bund“, „Stillstand“ nur bei juristisch-historischer Verwendung.
- Fokus auf Vorkommen in Titeln/Abstracts/Bibliographien.
"""

USER_TMPL = """Seed: "{seed}"
Domänenhinweis: "{domain}"
Sprache: {lang}
Aufgabe: Liefere JSON nach Schema.
"""


def _strip_code_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", s)
    if s.endswith("```"):
        s = s[:-3]
    return s.strip()


def _coerce_json(text: str) -> Dict[str, Any]:
    t = _strip_code_fences(text)
    try:
        return json.loads(t)
    except Exception:
        pass
    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(t[start : end + 1])
    raise ValueError("Antwort nicht als JSON interpretierbar.")


def _cache_key(seed: str, language: str, domain: str, model: str) -> Tuple[str, Path]:
    base = f"{seed}||{language}||{domain}||{model}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()
    return h, CACHE_DIR / f"{h}.json"


def _ensure_categories(payload: Dict[str, Any]) -> None:
    tb = payload.setdefault("terms_by_category", {})
    for cat in ("core", "truce", "settlement", "legal_instrument", "end_of_hostilities", "latin", "historic_variants"):
        tb.setdefault(cat, [])


def _seed_in_payload(payload: Dict[str, Any], seed: str) -> bool:
    tb = payload.get("terms_by_category", {})
    seed_cf = seed.casefold().strip()
    for terms in tb.values():
        for t in terms:
            if str(t).casefold().strip() == seed_cf:
                return True
    return False


def _inject_seed(payload: Dict[str, Any], seed: str) -> None:
    _ensure_categories(payload)
    if not _seed_in_payload(payload, seed):
        payload["terms_by_category"]["core"].insert(0, seed)


def _call_openai_v1(api_key: str, model: str, prompt: str) -> Dict[str, Any]:
    client = _OpenAIClass(api_key=api_key)  # type: ignore
    try:
        resp = client.responses.create(
            model=model,
            input=prompt,
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        text = getattr(resp, "output_text", None)
        if not text:
            try:
                text = resp.output[0].content[0].text  # type: ignore
            except Exception:
                pass
        if not text:
            raise ValueError("Leere Responses-Antwort.")
        return _coerce_json(text)
    except Exception:
        resp = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt.split("\n\n", 1)[-1]},
            ],
        )
        text = resp.choices[0].message.content  # type: ignore
        return _coerce_json(text)


def _map_legacy_model(model: str) -> str:
    if not model:
        return "gpt-3.5-turbo"
    low = model.lower()
    if any(k in low for k in ["4.1", "4o", "mini", "omni"]):
        return "gpt-4"
    return model


def _call_openai_legacy(api_key: str, model: str, prompt: str) -> Dict[str, Any]:
    import openai as openai  # type: ignore
    openai.api_key = api_key
    model = _map_legacy_model(model)
    resp = openai.ChatCompletion.create(  # type: ignore[attr-defined]
        model=model,
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    text = resp["choices"][0]["message"]["content"]
    return _coerce_json(text)


def generate_keyword_candidates(
    seed_term: str,
    language: str = "de",
    domain_hint: str = "Alte Zürichkrieg-Forschung",
    model: Optional[str] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY nicht gefunden (.env).")

    if _SDK_PATH == "v1":
        model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    elif _SDK_PATH == "legacy":
        model = model or os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
    else:
        raise RuntimeError("OpenAI-SDK nicht importierbar. Installiere/aktualisiere 'openai'.")

    key, cache_file = _cache_key(seed_term, language, domain_hint, model)
    if use_cache and cache_file.exists():
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        _ensure_categories(data)
        _inject_seed(data, seed_term)
        data["_meta"] = {"from_cache": True, "cache_key": key, "model": model, "sdk": _SDK_PATH}
        return data

    user_msg = USER_TMPL.format(seed=seed_term, domain=domain_hint, lang=language)
    prompt = f"{SYSTEM_PROMPT}\n\n{user_msg}"

    if _SDK_PATH == "v1":
        payload = _call_openai_v1(api_key, model, prompt)
    else:
        payload = _call_openai_legacy(api_key, model, prompt)

    payload.setdefault("seed", seed_term)
    _ensure_categories(payload)
    _inject_seed(payload, seed_term)

    payload["_meta"] = {"from_cache": False, "cache_key": key, "model": model, "sdk": _SDK_PATH}
    if use_cache:
        cache_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return payload


def main() -> None:
    print("[DEBUG] search_term.py: Start")
    try:
        seed = os.getenv("SEED", "Frieden")
        lang = os.getenv("LANG", "de")
        model = os.getenv("OPENAI_MODEL")
        data = generate_keyword_candidates(seed_term=seed, language=lang, model=model, use_cache=True)
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print(f"[DEBUG] meta: {data.get('_meta')}")
    except Exception as e:
        print("[ERROR]", e)
    print("[DEBUG] search_term.py: Ende")


if __name__ == "__main__":
    main()
