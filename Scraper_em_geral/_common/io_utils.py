import json, csv, os
from datetime import datetime

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d: os.makedirs(d, exist_ok=True)

def normalize_path(path: str, default_basename: str = "pneustore") -> str:
    if not path:
        path = ""

    if os.path.isdir(path) or path.endswith(("\\", "/")):
        os.makedirs(path, exist_ok=True)
        fname = f"{default_basename}_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
        return os.path.join(path, fname)

    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    root, ext = os.path.splitext(path)
    if not ext:
        path = path + ".jsonl"
    return path

def write_jsonl(path: str, rows):
    path = normalize_path(path, default_basename="pneustore")
    ensure_dir_for(path)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows or []:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(path: str, rows):
    rows = list(rows or [])
    if not rows:
        return
    path = normalize_path(path, default_basename="pneustore")  
    root, _ = os.path.splitext(path)
    path = root + ".csv"

    ensure_dir_for(path)
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
