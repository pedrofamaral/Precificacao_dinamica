from __future__ import annotations
import argparse, sys, os, pandas as pd
from datetime import datetime

REQUIRED = ["marketplace","sku_norm","price","title","captured_at","url"]

def load_table(path: str) -> pd.DataFrame:
    p = path.lower()
    if p.endswith(".parquet"):
        try:
            return pd.read_parquet(path)
        except Exception as e:
            print(f"[!] Não consegui ler Parquet ({e}). Tente salvar CSV também e apontar para o CSV.")
            raise
    elif p.endswith(".csv"):
        return pd.read_csv(path)
    else:
        raise SystemExit("Forneça um .parquet ou .csv")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="Caminho do unifier_input.parquet (ou .csv)")
    args = ap.parse_args()

    df = load_table(args.path)
    print(f"[i] Linhas: {len(df)}  |  Colunas: {list(df.columns)}")

    if "captured_at" in df.columns:
        df["_capt"] = pd.to_datetime(df["captured_at"], errors="coerce")
    else:
        df["_capt"] = pd.NaT

    ok = True
    for c in REQUIRED:
        if c not in df.columns:
            ok = False
            print(f"[X] COLUNA OBRIGATÓRIA AUSENTE: {c}")
        else:
            nnull = int(df[c].isna().sum())
            if nnull > 0:
                ok = False
                print(f"[X] NULOS EM {c}: {nnull}")

    if "price" in df.columns:
        bad = int((df["price"] <= 0).sum())
        if bad > 0:
            ok = False
            print(f"[X] Preços <= 0: {bad}")

    if {"marketplace","sku_norm"}.issubset(df.columns):
        dups = int(df.duplicated(subset=["marketplace","sku_norm"]).sum())
        if dups > 0:
            ok = False
            print(f"[X] Duplicatas por (marketplace, sku_norm): {dups}")

    if {"marketplace","sku_norm","_capt"}.issubset(df.columns):
        mx = df.groupby(["marketplace","sku_norm"])["_capt"].transform("max")
        bad_recent = int((df["_capt"] != mx).sum())
        if bad_recent > 0:
            ok = False
            print(f"[X] Recência incorreta (não é o registro mais recente) em {bad_recent} linhas")

    prev = df.head(5)[["marketplace","sku_norm","title","price","captured_at","url"]].to_dict(orient="records")
    print("[i] Amostra de 5 linhas:")
    for r in prev:
        print("   ", r)

    if "marketplace" in df.columns:
        print("[i] Contagem por marketplace:")
        print(df["marketplace"].value_counts())

    print("\n" + ("[✓] Parece OK ✔️" if ok else "[!] Há problemas acima ☝️"))

if __name__ == "__main__":
    main()
