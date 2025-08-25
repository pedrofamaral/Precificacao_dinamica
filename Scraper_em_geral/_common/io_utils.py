import json, csv, os

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d: os.makedirs(d, exist_ok=True)

def write_jsonl(path: str, rows):
    ensure_dir_for(path)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(path: str, rows):
    if not rows: return
    ensure_dir_for(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
