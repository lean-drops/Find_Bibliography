"""
Nutzung:
  python chronik-search.py

Dieses Skript ist der einzige Einstiegspunkt. Es ruft chronik_finder.run()
auf, erzeugt identische Ausgaben wie die Monolith-Version und priorisiert
Genauigkeit vor Geschwindigkeit.
"""
from __future__ import annotations

from chronik_finder import run as run_finder


def main() -> None:
    print("[INFO] chroniken_library-Finder startet…")
    session_dir, df, agg = run_finder()
    if session_dir is None:
        print("[ERROR] Lauf abgebrochen.")
        return
    print(f"[INFO] Fertig. Session-Ordner: {session_dir}")

if __name__ == "__main__":
    main()

