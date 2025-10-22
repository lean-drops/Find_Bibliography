# DELB – Bibliography Detector (ToC ▸ Keywords ▸ Fonts ▸ Heuristic)

Detects bibliography / references sections in academic PDFs, robustly and fast.
This repository contains a small toolkit of cooperating scripts that can be run
stand-alone or orchestrated as a pipeline.

> **TL;DR**
> - We try four increasingly expensive signals:
>   1) **Table of Contents (ToC)** jackpot – if ToC says “Bibliography” → done.
>   2) **Keyword block** – consecutive pages with “References/Bibliography/…”。
>   3) **Heading fonts** – detect chapter titles from font clusters; find “Bibliography”.
>   4) **Full heuristic** – score raw text pages and pick the best block.
> - Primary API: `detect_bibliography(pdf) -> (first_page, last_page) | None`
> - Batch runners process whole folders in parallel and write `bibliography_bounds.json`.

---

## 0) Project layout

```
repo-root/
├─ run.py                        # One-shot runner / CLI orchestrator (this file)
├─ services/
│  └─ delb/
│     ├─ bib_orchestrator.py     # High-level pipeline (ToC → KW → Fonts → Heuristic)
│     ├─ find_bibliography.py    # Pure-text detector + identical pipeline variant
│     ├─ extract_toc.py          # ToC finder (pdfplumber + PyMuPDF bookmarks)
│     ├─ keyword_hits.py         # PDF pages containing “References…” keywords
│     ├─ scan_fonts.py           # Font/layout scanner (see Variant notes!)
│     ├─ detect_chapters.py      # Chapter-heading detection from *.fonts.json
│     ├─ bibliography_terms.csv  # Keyword list (CSV; header: `term`)
│     └─ meta/                   # Auto-created cache: per-PDF JSONs
│         ├─ <pdf-stem>/pages_ratio.json
│         ├─ <pdf-stem>/font_cluster.json
│         ├─ pages_ratio_summary.json
│         └─ index.json
```

> **Namespace note** – Make sure `services/` and `services/delb/` each contain an
> `__init__.py` (may be empty), so `from services.delb ...` imports work.

---

## 1) Installation

### 1.1 Python & OS
- Python **3.10+** recommended.
- Works on macOS, Linux, Windows. For OCR you need a local Tesseract install (optional).

### 1.2 Python packages
```bash
pip install --upgrade pip
pip install pymupdf pdfplumber tqdm python-dotenv langdetect pytesseract
```
Optional (only for GPT assist in the heuristic):
```bash
pip install openai
```

### 1.3 System tools (optional)
- **Tesseract** for OCR fallback in `extract_toc.py` (`pytesseract` wrapper). If not installed, OCR is simply skipped.

### 1.4 Environment variables (optional)
- `OPENAI_API_KEY` – enables GPT refinement in `find_bibliography.py`.
- `OPENAI_INSECURE_TLS=1` – only for special macOS/alpine TLS work-arounds (not recommended unless you know why).

Store them in a local `.env` (loaded by `python-dotenv`) or export them in your shell.

---

## 2) Data flow & JSON artifacts

### 2.1 Pipeline overview
```
PDF ──► extract_toc            → ToC page + lines
   └─► keyword_hits            → pages matching bibliography terms
   └─► scan_fonts              → meta/<stem>/font_cluster.json
        └─► detect_chapters    → headings; find “Bibliography …” page(s)
   └─► find_bibliography/_detect_block (text-only full heuristic)
           ▲
           └─ bib_orchestrator orchestrates: ToC ▸ KW ▸ Fonts ▸ Heuristic
```

### 2.2 Produced meta files (under `services/delb/meta/`)
- `meta/<PDF-STEM>/pages_ratio.json` – `{ toc_page, total_pages }`
- `meta/pages_ratio_summary.json` – overview for all scanned PDFs
- `meta/index.json` – raw ToC lines per PDF (if ToC was detected)
- `meta/<PDF-STEM>/font_cluster.json` – **font clusters used by `detect_chapters`**

> The **font JSON schema must match the expectations of `detect_chapters.py`** (see 4.3).
> If you use a different `scan_fonts.py` variant, see the compatibility notes below.

---

## 3) High-level APIs

### 3.1 `services/delb/bib_orchestrator.py`
Public function:
```python
detect_bibliography(pdf: str|Path, *, kw_min_block=2, head=0.05, tail=0.25,
                    kw_workers=None, font_threads=None, use_gpt=False, boost=1.0)
  -> Optional[tuple[int,int]]
```
Return: `(first_page, last_page)` (1-based inclusive) or `None`.

**Strategy (in order):**
1. **ToC jackpot**  
   - `_scan_pdf()` returns potential ToC page and its lines.  
   - If the ToC entry for “Bibliography/References/…” exists and is the *last* ToC item, we return `(bib_page, EOF)`; otherwise `(bib_page, bib_page)`.
2. **Keyword block**  
   - `keyword_hits.analyse_pdf()` returns pages containing terms from `bibliography_terms.csv`.  
   - We find the longest contiguous run; if its length ≥ `kw_min_block`, return that block.
3. **Heading fonts**  
   - `scan_fonts.analyse_pdf()` + `scan_fonts.write_json()` generate `meta/<stem>/font_cluster.json`.  
   - `detect_chapters.analyse_file()` finds chapter titles; if any title matches a bibliography header (regex), we either return that single page or, if the text-heuristic block includes it, the full block.
4. **Full text heuristic**  
   - `_detect_text()` (aka `_detect_block` from `find_bibliography.py`) scores pages by header hints (caps + terms) and citation density (year/DOI/numbered ref lines), then picks the best continuous block around the median score (with optional GPT refinement).

**Regex details**  
- Bibliography heading pattern supports EN/DE and common variants:  
  `r"\b(bibliograph\w*|references?|reference\s+list|works\s+cited|literaturverzeichnis|quellen(?:verzeichnis| und literatur)?)\b"`

**Notes**
- Uses PyMuPDF (`fitz`) only briefly to get `page_count` for ToC‑EOF decision.
- Writes `meta/<stem>/font_cluster.json` every run to keep caches fresh.

### 3.2 `services/delb/find_bibliography.py`
Offers the same top-level `detect_bibliography(...)` **plus** a fully self-contained text-only block detector:

```python
_detect_block(pdf, head=0.05, tail=0.25, use_gpt=False, gpt_only=False, boost=1.0)
  -> ((first,last), first_page_text) | (None, None)
```
**Scoring per page** (`_score_page`):
- Header bonus if the first ~8 lines contain bib terms **and** have a caps/Titlecase ratio ≥ `HDR_CAPS_MIN` (default 0.45).  
- Citation density using regex pools: DOI, author/year lines, leading numeric markers `[12]`, `(2019)`, bare `2019`, etc.  
- Combine with weights `HDR_W`, `CITE_W`, then require a minimum ratio of cite-like lines.

**Block selection**:
- Pages ≥ median score × `boost` form contiguous runs. Choose the run with higher mean score; tie-break by length.
- Optional **GPT refinement** (tiny prompt) flips page scores to 1.0 if GPT confidently says *BIB*.

**CLI**
```
python -m services.delb.find_bibliography <PATH-OR-FOLDER> [--head 0.05] [--tail 0.25]
                                          [--gpt] [--boost 1.0] [-j N] [--debug]
```

### 3.3 `services/delb/extract_toc.py`
- Detects ToC via **PDF outline/bookmarks** (PyMuPDF) or heuristically scanning the first third of pages (pdfplumber; optional OCR via pytesseract).
- Writes per-PDF `pages_ratio.json` and two central indices:
  - `pages_ratio_summary.json` and `index.json` (all ToC lines).

**CLI**
```
python -m services.delb.extract_toc <PDF|DIR> [--max-pages N] [--ocr] [-v]
```

### 3.4 `services/delb/keyword_hits.py`
- Parallel page scanning via **ProcessPoolExecutor**; finds pages that contain any term from `bibliography_terms.csv` (CSV **must** have a `term` header).

**CLI**
```
python -m services.delb.keyword_hits <PDF|DIR> [-j N] [--debug]
```
Output for single PDF: `{ "file.pdf": [5,6,7] }` (1-based pages).

### 3.5 `services/delb/scan_fonts.py` (variants & compatibility)

There are *two* variants referenced in your materials:

- **Variant A – “v4-fast” (recommended for chapter detection)**  
  - Produces per-page keys like `"n"`, plus per-page `"heading"` font clusters and optional `"spans"` previews.  
  - This is the format **expected by `detect_chapters.py`**.

- **Variant B – “v2.0 turbo”**  
  - Exposes `analyse_pdf(pdf, threads)` and `write_json(data, pdf_name)` (which `bib_orchestrator` imports), **but writes a different schema** (`"page"` instead of `"n"` and no `"heading"/"spans"`).
  - `detect_chapters.py` cannot infer headings from this schema.

**What to do**
- **Prefer Variant A (v4-fast)** and add a tiny wrapper that exports the names expected by the orchestrators:
  ```python
  # at bottom of v4-fast scan_fonts.py
  def analyse_pdf(pdf: Path, threads: int):
      return cluster_fonts(pdf, threads, min_body_ratio=0.40, keep_spans=True)

  def write_json(data: dict, pdf_name: str):
      from pathlib import Path
      import json
      out = Path("meta") / Path(pdf_name).stem / "font_cluster.json"
      out.parent.mkdir(parents=True, exist_ok=True)
      out.write_text(json.dumps(data, indent=2, ensure_ascii=False))
  ```
- Or, if you must use Variant B (v2.0 turbo), `detect_chapters.py` will not work; **the pipeline will gracefully fall back** to ToC → Keywords → full heuristic, but heading-based boosts are skipped.

### 3.6 `services/delb/detect_chapters.py`
- Reads `*.fonts.json` (ideally Variant A format) and votes repeated **(font, text)** pairs that look like headings. Keeps those with ≥ `MIN_FONT_VOTES` occurrences and page font-size above body size.
- Returns JSON to stdout (single file mode) or writes `*.chapters.csv` (batch mode).

**CLI**
```
python -m services.delb.detect_chapters <FONTS-JSON|PDF-DIR> [--fonts DIR] [-o OUT] [-j N] [--debug] [--trace]
```

---

## 4) Configuration, parameters & schema

### 4.1 Orchestrator parameters (both `bib_orchestrator` and `find_bibliography`)
- `head` / `tail` – float `0..1`, fractions of pages to pre-scan before full scan.
- `kw_min_block` – minimum length of a contiguous keyword-hit block (default 2).
- `kw_workers`, `font_threads` – parallelism knobs (auto defaults from CPU count).
- `use_gpt`, `boost` – GPT refinement and selection threshold tuning (advanced).

### 4.2 Keyword CSV (`bibliography_terms.csv`)
- Must be next to `keyword_hits.py` and contain a header `term`, one term per row.
- Example rows:
  ```csv
  term
  References
  Bibliography
  Literaturverzeichnis
  Works Cited
  ```

### 4.3 Font JSON schema (**Variant A – expected**)
Minimal keys used by `detect_chapters.py`:
```json
{
  "pdf_pages": 250,
  "font_ranking": [["TimesNewRomanPSMT", 10.5, 0, 123456]],
  "pages": [
    {
      "n": 1,
      "body": [["TimesNewRomanPSMT", 10.5, 0]],
      "heading": [["TimesNewRomanPS-BoldMT", 14.0, 1]],
      "spans": [
        ["CHAPTER 1 INTRODUCTION", "TimesNewRomanPS-BoldMT", 14.0, 1],
        ["1.1 Motivation", "TimesNewRomanPS-BoldMT", 12.0, 1]
      ]
    }
  ]
}
```

---

## 5) Using the tools

### 5.1 Quick start: single PDF
```bash
python batch_runner.py ./pdfs/book.pdf --debug
```
Output:
- On console: verbose step-by-step trace.
- `services/delb/meta/<book>/font_cluster.json` (fresh cache).
- `bibliography_bounds.json` in the same folder as your PDF (if you passed a directory) or next to the file.

### 5.2 Batch a directory
```bash
python batch_runner.py ./pdfs --gpt --head 0.05 --tail 0.25 -j 6 --font-threads 8 --kw-workers 4
```

### 5.3 Stand-alone scripts
You can run each module directly (see the CLI notes above) if you want to inspect intermediate artifacts (ToC JSONs, font clusters, chapter CSVs, etc.).

---

## 6) Troubleshooting

- **`ModuleNotFoundError: services.delb...`**  
  → Ensure `services/` and `services/delb/` have `__init__.py` and that `run.py` lives at repo root (it auto-adds repo root to `sys.path`).

- **`keyword_hits.py` exits: “bibliography_terms.csv fehlt …”**  
  → Place `bibliography_terms.csv` next to `keyword_hits.py` with header `term`.

- **No ToC detected**  
  → Normal. The pipeline continues with keywords/fonts/heuristic. You can raise `--tail` to scan more pages in the tail side of the document for the heuristic.

- **`detect_chapters` finds 0 headings**  
  - Check you’re on **scan_fonts Variant A** or add the wrapper to match its schema.
  - Try increasing font threads and ensure the PDF really uses larger fonts for headings.

- **OCR required**  
  → Use `extract_toc.py --ocr`. If Tesseract is not installed, OCR is skipped; ToC may not be found in image-only PDFs, but later stages still work.

- **Windows multiprocessing**  
  → Always keep `if __name__ == "__main__":` guards (already present).

- **GPT isn’t used**  
  → Set `OPENAI_API_KEY`. Then add `--gpt` to `run.py` or `find_bibliography.py` CLI.

---

## 7) Performance tips

- Use `-j` (processes) for more PDFs in parallel and `--font-threads` to speed up per-PDF font scans.
- The heuristic’s **head/tail pre-scan** avoids full scans on most documents; tune `--head/--tail` to trade precision vs. speed.
- `keyword_hits.py` is trivially parallelized; ensure `bibliography_terms.csv` is rich enough for your language mix.

---

## 8) Security & privacy

- PDFs are processed locally. No content leaves the machine unless you enable GPT (then the tiny page snippets used for classification are sent to the OpenAI API).

---

## 9) API by example

```python
from pathlib import Path
from services.delb.bib_orchestrator import detect_bibliography

pdf = Path("pdfs/thesis.pdf")
bounds = detect_bibliography(pdf, kw_min_block=2, head=0.05, tail=0.25,
                             kw_workers=None, font_threads=None,
                             use_gpt=False, boost=1.0)
print(bounds)   # e.g., (231, 260)
```

---

## 10) Known compatibility matrix

| Component                     | Needs Variant A (`scan_fonts v4-fast`) | Works with Variant B (`v2.0 turbo`) |
|------------------------------|-----------------------------------------|-------------------------------------|
| `detect_chapters.py`         | ✅ Required                              | ❌                                   |
| `bib_orchestrator.py`        | ✅ (for step 3)                          | ⚠️ falls back to steps 1,2,4         |
| `find_bibliography.py`       | ✅ (for step 3)                          | ⚠️ falls back to steps 1,2,4         |
| `run.py` (this repo)         | ✅ recommended                           | ✅ but without heading boosts        |

If you only care about ToC/Keywords/Heuristic, Variant B is fine. For best accuracy, use Variant A (or add a wrapper that exports the expected keys).

---

## 11) License & credits

- MIT-style intent for the scripts; adjust as needed for your organization.
- Uses: PyMuPDF (`fitz`), pdfplumber, tqdm, langdetect, pytesseract, python-dotenv.

Happy detecting! 🎯
