import pandas as pd
import re
import importlib

um = importlib.import_module("unificar_marketplaces")

def row(**k):
    return k

def test_extract_size_basic():
    r = row(size_raw="205/55R16")
    assert um.extract_size(r) == "205/55R16"
    r2 = row(title="Pneu 195 65 r15 Goodyear")
    assert um.extract_size(r2) == "195/65R15"

def test_extract_brand_known_and_in_title(cfg):
    with cfg(um) as CONFIG:
        CONFIG["known_brands"] = ["goodyear","dunlop"]
        CONFIG["brand_aliases"] = {}
        um.apply_config_lowerdedup()
        r = row(brand_raw="Goodyear")
        assert um.extract_brand(r) == "goodyear"
        r2 = row(title="Pneu Dunlop 205/55R16")
        assert um.extract_brand(r2) == "dunlop"

def test_extract_model_phrases_and_after_brand(cfg):
    with cfg(um) as CONFIG:
        CONFIG["known_model_phrases"] = ["assurance maxlife", "fm800"]
        CONFIG["model_aliases"] = {}
        um.apply_config_lowerdedup()
        # frase conhecida
        r = row(title="Pneu Goodyear Assurance MaxLife 205/55R16", brand_raw="Goodyear")
        m = um.extract_model({"title":r["title"], "model_raw":""}, "goodyear")
        assert m == "assurance maxlife"
        # após a marca, corta antes da medida
        r2 = row(title="Dunlop SP Touring FM800 195/65R15 91H", brand_raw="Dunlop")
        m2 = um.extract_model({"title":r2["title"], "model_raw":""}, "dunlop")
        assert ("fm800" in m2) or ("sp touring" in m2)

def test_summarize_evidence(cfg):
    with cfg(um):
        df = pd.DataFrame([
            {"brand":"goodyear","model":"assurance maxlife","size":"205/55R16","price":100.0,"marketplace":"mercadolivre","source_file":"MercadoLivre/data/raw/a.csv"},
            {"brand":"goodyear","model":"assurance maxlife","size":"205/55R16","price":110.0,"marketplace":"magazineluiza","source_file":"MagazineLuiza/data/raw/b.csv"},
        ])
        summary = um.summarize_canonical(df)
        assert len(summary) == 1
        row = summary.iloc[0]
        assert "MercadoLivre/data/raw/a.csv" in row["evidence_files"]
        assert "MagazineLuiza/data/raw/b.csv" in row["evidence_files"]

def test_make_source_tag_slashes(cfg, tmp_path, monkeypatch):
    with cfg(um):
        base = tmp_path / "PriceMonitor" / "MercadoLivre" / "data" / "raw"
        base.mkdir(parents=True, exist_ok=True)
        f = base / "lote" / "itens.csv"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("x,y\n", encoding="utf-8")
        tag = um.make_source_tag(f, base)
        # Deve usar '/'
        assert "\\" not in tag
        assert tag.endswith("MercadoLivre/data/raw/lote/itens.csv")
