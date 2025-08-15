from __future__ import annotations
import os, sqlite3
from typing import Optional, Dict
from pathlib import Path
import pandas as pd

def _ensure_dir_for_file(path: str):
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

def generate_final( unified_db: str, audit_db: str, final_db: str, final_csv: str, threshold: float = 0.70,
    exclude_precheck_alerts: bool = True, reports_dir: Optional[str] = None, quiet: bool = True, clear_screen: bool = False,
) -> Dict[str,int]:
    """
    Seleciona linhas aprovadas pela auditoria (soft-delete) e produz:
      - final.db (tabela unified_listings_final [+ canonical_summary_final])
      - unified_listings_final.csv
      - relatórios CSV opcionais

    Critério padrão:
      llm_ok=1 AND llm_confidence >= threshold AND
      (precheck_alerts_json is NULL/''/'[]'   se exclude_precheck_alerts=True)
    """
    if clear_screen:
        os.system("cls" if os.name == "nt" else "clear")

    _ensure_dir_for_file(final_db)
    _ensure_dir_for_file(final_csv)

    con = sqlite3.connect(unified_db)
    con.execute(f"ATTACH DATABASE ? AS audit", (audit_db,))

    where = ["COALESCE(a.llm_ok,0)=1", "COALESCE(a.llm_confidence,0)>=?"]
    params = [threshold]
    if exclude_precheck_alerts:
        where.append("(a.precheck_alerts_json IS NULL OR a.precheck_alerts_json='' OR a.precheck_alerts_json='[]')")
    where_sql = " AND ".join(where)

    select_sql = f"""
        SELECT u.*
        FROM main.unified_listings u
        JOIN audit.ai_audit a
          ON a.canonical_key = u.canonical_key
        WHERE {where_sql}
    """
    df_final = pd.read_sql_query(select_sql, con, params=params)

    kept = len(df_final)
    total = pd.read_sql_query("SELECT COUNT(*) AS n FROM main.unified_listings;", con)["n"].iloc[0]
    drop_sql = f"""
        SELECT COUNT(*) AS n FROM (
            SELECT u.rowid
            FROM main.unified_listings u
            LEFT JOIN audit.ai_audit a ON a.canonical_key = u.canonical_key
            WHERE NOT ({where_sql})
            GROUP BY u.rowid
        )
    """
    dropped = pd.read_sql_query(drop_sql, con, params=params)["n"].iloc[0]

    con_final = sqlite3.connect(final_db)
    df_final.to_sql("unified_listings_final", con_final, if_exists="replace", index=False)

    try:
        df_sum = pd.read_sql_query("SELECT * FROM main.canonical_summary;", con)
        if "canonical_key" in df_sum.columns and "canonical_key" in df_final.columns:
            keys = df_final[["canonical_key"]].drop_duplicates()
            df_sum_f = df_sum.merge(keys, on="canonical_key", how="inner")
            df_sum_f.to_sql("canonical_summary_final", con_final, if_exists="replace", index=False)
    except Exception:
        pass

    con_final.close()

    df_final.to_csv(final_csv, index=False, encoding="utf-8-sig")

    if reports_dir:
        rdir = Path(reports_dir).expanduser().resolve()
        rdir.mkdir(parents=True, exist_ok=True)

        brand_col = None
        for c in df_final.columns:
            if c.lower() in ("brand","marca","brand_name","seller_brand"):
                brand_col = c; break

        def _num(s):
            try: return pd.to_numeric(s, errors="coerce")
            except Exception: return pd.Series(dtype="float64")

        price_col = None
        for c in df_final.columns:
            if c.lower() in ("price","preco","price_value","price_brl"):
                price_col = c; break

        if brand_col and price_col:
            g = (df_final
                    .assign(_price=_num(df_final[price_col]))
                    .groupby(brand_col, dropna=False)
                    .agg(qtd=("canonical_key","count"),
                         preco_medio=("_price","mean"),
                         preco_mediana=("_price","median"))
                    .reset_index()
                    .sort_values(["qtd","preco_medio"], ascending=[False, True]))
            g.to_csv(rdir / "top_marcas.csv", index=False, encoding="utf-8-sig")

        idcol = "canonical_key" if "canonical_key" in df_final.columns else None
        if idcol and price_col:
            cols = [idcol] + ([brand_col] if brand_col else [])
            disp = (df_final
                      .assign(_price=_num(df_final[price_col]))
                      .groupby(cols, dropna=False)["_price"]
                      .agg(qtd="count",
                           minimo="min",
                           q1=lambda s: s.quantile(0.25),
                           mediana="median",
                           media="mean",
                           q3=lambda s: s.quantile(0.75),
                           maximo="max",
                           desvio="std")
                      .reset_index())
            disp["cv"] = disp["desvio"] / disp["media"]
            disp.to_csv(rdir / "dispersao_precos.csv", index=False, encoding="utf-8-sig")

            def _iqr_outliers(grp):
                p = _num(grp[price_col])
                q1, q3 = p.quantile(0.25), p.quantile(0.75)
                iqr = q3 - q1
                low, high = q1 - 1.5*iqr, q3 + 1.5*iqr
                mask = (p < low) | (p > high)
                out = grp.loc[mask, [idcol] + ([brand_col] if brand_col else [])].copy()
                out["price"] = p[mask].values
                out["limite_inferior"] = low
                out["limite_superior"] = high
                out["q1"] = q1; out["q3"] = q3; out["iqr"] = iqr
                return out

            outliers = df_final.groupby(idcol, group_keys=False).apply(_iqr_outliers)
            if not outliers.empty:
                outliers.to_csv(rdir / "outliers_precos.csv", index=False, encoding="utf-8-sig")

    con.close()

    return {"kept": int(kept), "dropped": int(dropped), "total": int(total)}
