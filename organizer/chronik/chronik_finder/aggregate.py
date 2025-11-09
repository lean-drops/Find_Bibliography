"""
Aggregation, Gewichtung und Netzwerk.
"""
from __future__ import annotations

from typing import Dict, Tuple, Any

from .env import HAVE_NX, HAVE_PANDAS, nx, pd

def aggregate(hits, weights: Dict[str, float]):
    if not HAVE_PANDAS:
        raise RuntimeError("pandas ist erforderlich. Installation: pip install pandas")
    rows = [{
        "pdf_file": h.pdf_path, "page": h.page, "group": h.group, "label": h.label,
        "pattern": h.pattern, "context": h.context
    } for h in hits]
    df = pd.DataFrame(rows)
    if df.empty:
        # leere, aber typkompatible Frames
        agg = pd.DataFrame(columns=["group", "label", "mentions", "docs", "weight", "weighted_mentions"])
        return df, agg
    agg = df.groupby(["group", "label"]).agg(
        mentions=("pdf_file", "count"),
        docs=("pdf_file", lambda s: len(set(s)))
    ).reset_index()
    agg["weight"] = agg["group"].map(lambda g: float(weights.get(g, 1.0)))
    agg["weighted_mentions"] = (agg["mentions"] * agg["weight"]).round(3)
    return df, agg

def build_co_mention_network(df: Any):
    if not HAVE_NX or df is None or df.empty:
        return None
    dfw = df[df["group"] == "work"].copy()
    if dfw.empty:
        return None
    edges = {}
    for _, sub in dfw.groupby("pdf_file"):
        labels = sorted(set(sub["label"]))
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                key = (labels[i], labels[j])
                edges[key] = edges.get(key, 0) + 1
    G = nx.Graph()
    for (a, b), w in edges.items():
        G.add_edge(a, b, weight=float(w))
    return G

