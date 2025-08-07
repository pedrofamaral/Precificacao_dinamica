import glob
import json
from pathlib import Path
import pandas as pd

def extract_from_json_folder(path_pattern: str) -> pd.DataFrame:
    records = []
    for fname in glob.glob(path_pattern, recursive=True):
        with open(fname, 'r', encoding='utf-8') as f:
            data = json.load(f)
        items = data if isinstance(data, list) else [data]
        rel_path = str(Path(fname).relative_to(Path(path_pattern.split('/')[0])))
        for item in items:
            if isinstance(item, dict):
                item['_source_file'] = rel_path
                records.append(item)
    return pd.DataFrame(records)
