import re
from typing import Optional, Dict
from ETL.common import SPEED_RE, _norm, clean_speed_tokens

_RE_METRIC = re.compile(
    r"""
    \b
    (?P<w>\d{3})            
    \s* [/]\s*
    (?P<a>\d{2})            
    \s* [- ]? \s*
    (?P<cons>(?:Z?R)?)      
    \s*
    (?P<rim>\d{2})          
    \b
    (?: [^\d]*
        (?P<li>\d{2,3})     
        \s* (?P<ss>[A-Z])   
    )?
    """,
    re.IGNORECASE | re.VERBOSE
)

_RE_MOTO = re.compile(
    r"\b(?P<w>\d{3})\s*/\s*(?P<a>\d{2})\s*[-]\s*(?P<rim>\d{2})\b",
    re.IGNORECASE
)

_RE_FLOT = re.compile(
    r"\b(?P<od>\d{2,3}(?:\.\d+)?)x(?P<section>\d{2}(?:\.\d+)?)(?P<cons>[R-])(?P<rim>\d{2})\b",
    re.IGNORECASE
)

_RE_TRUCK = re.compile(
    r"\b(?P<sec>\d(?:\.\d{2})?)\s*[-]\s*(?P<rim>\d{2})\b",
    re.IGNORECASE
)

_RE_LT_METRIC = re.compile(
    r"\bLT\s*(?P<w>\d{3})\s*/\s*(?P<a>\d{2})\s*(?P<cons>Z?R)?\s*(?P<rim>\d{2})\b"
    r"(?:[^\d]*(?P<li>\d{2,3})(?:/(?P<li2>\d{2,3}))?\s*(?P<ss>[A-Z]))?",
    re.IGNORECASE
)

_RE_FLAGS = re.compile(r"\b(?P<xl>XL|EXTRA\s*LOAD|RF|REINFORCED)\b|(?P<c>\d{1,2}PR|[CD]\b)", re.IGNORECASE)


def _canon_metric(w: str, a: str, rim: str, cons: Optional[str]) -> str:
    cons = (cons or "").upper()
    if cons and cons.upper() not in ("R", "ZR"):
        cons = "R"
    if not cons:
        cons = "R"
    return f"{int(w):03d}/{int(a):02d}{cons.upper()}{int(rim):02d}"

def _pick_first_group(m, *names):
    for n in names:
        v = m.groupdict().get(n)
        if v: return v
    return None

import re
import pandas as pd
from typing import Optional, Dict

try:
    from ETL.common import SPEED_RE, _norm, clean_speed_tokens
except ImportError:
    from .common import SPEED_RE, _norm, clean_speed_tokens


def enrich_with_parsed_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def _parse_size(title: Optional[str]) -> Dict[str, object]:
        d = extract_tire_size_from_title(title or "")
        return {
            "width": d.get("width"),
            "aspect": d.get("aspect"),
            "rim": d.get("rim"),
            "construction": d.get("construction"),
            "size_norm": d.get("size_norm"),
            "xl_flag": bool(d.get("xl_flag", False)),
        }

    sizes = out["title"].fillna("").map(_parse_size).apply(pd.Series)
    out = pd.concat([out, sizes], axis=1)

    def _parse_speed(title: Optional[str]):
        s = title or ""
        m = SPEED_RE.search(s)
        if not m:
            return pd.Series({"load_index": pd.NA, "speed_index": pd.NA, "title_wo_speed": s})
        token = m.group(0).upper()
        m2 = re.match(r"(?P<load>\d{2,3})(?P<speed>[A-Z]{1,2})", token)
        load = int(m2.group("load")) if m2 else pd.NA
        speed = m2.group("speed") if m2 else token
        return pd.Series({"load_index": load, "speed_index": speed, "title_wo_speed": clean_speed_tokens(s)})

    sp = out["title"].fillna("").map(_parse_speed).apply(pd.Series)
    out = pd.concat([out, sp], axis=1)

    for col in ["width", "aspect", "rim", "load_index"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    mask = (
        out["size_norm"].isna()
        & out["width"].notna()
        & out["aspect"].notna()
        & out["rim"].notna()
    )
    out.loc[mask, "size_norm"] = out.loc[mask].apply(
        lambda r: f"{int(r['width']):03d}/{int(r['aspect']):02d}R{int(r['rim']):02d}",
        axis=1,
    )

    return out


def extract_brand(text: str, known_brands: set[str], brand_aliases: dict[str,str] | None = None) -> str:
    brand_aliases = brand_aliases or {}
    t = _norm(text)
    for alias, target in brand_aliases.items():
        a = _norm(alias); tgt = _norm(target)
        if re.search(rf"\b{re.escape(a)}\b", t): return target.title()
    for b in sorted(known_brands, key=len, reverse=True):
        nb = _norm(b)
        if nb and re.search(rf"\b{re.escape(nb)}\b", t): return b.title()
    tokens = [x for x in re.split(r"[^\w]+", t) if x]
    for tok in tokens:
        if SPEED_RE.fullmatch(tok): continue
        if tok.isdigit(): continue
        if len(tok) >= 2: return tok.title()
    return ""

def extract_model(text: str, brand: str, size_norm: str) -> str:
    t = clean_speed_tokens(text or "")
    if size_norm:
        nums = re.findall(r"\d{2,3}", size_norm)
        if len(nums) >= 3:
            w,a,r = nums[0], nums[1], nums[-1]
            t = re.sub(rf"\b{w}\D*{a}\D*[Rr]?\D*{r}\b", " ", t, flags=re.I)
    if brand:
        b = _norm(brand)
        t = re.sub(rf"\b{re.escape(b)}\b", " ", _norm(t), flags=re.I)
    t = re.sub(r"\b(pneu|aro|tl|xl|runflat|rf|indice|velocidade|tubeless|std)\b", " ", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -/").strip()
    return t.title()

def extract_tire_size_from_title(title: Optional[str]) -> Dict[str, Optional[str]]:
    out = {
        "size_norm": None,
        "width_mm": None,
        "aspect_pct": None,
        "rim_in": None,
        "load_index": None,
        "speed_symbol": None,
        "construction": None,
        "lt_flag": False,
        "xl_flag": False,
        "raw_pattern": None,
    }
    if not isinstance(title, str) or not title.strip():
        return out

    t = title.strip()

    m = _RE_LT_METRIC.search(t)
    if m:
        w, a, rim = m.group("w"), m.group("a"), m.group("rim")
        cons = _pick_first_group(m, "cons")
        size_norm = _canon_metric(w, a, rim, cons)
        out.update(
            size_norm=size_norm,
            width_mm=int(w),
            aspect_pct=int(a),
            rim_in=int(rim),
            construction=(cons or "R").upper() if cons else "R",
            lt_flag=True,
            raw_pattern="lt_metric",
        )
        li = _pick_first_group(m, "li")
        ss = _pick_first_group(m, "ss")
        if li and li.isdigit():
            out["load_index"] = int(li)
        if ss and ss.isalpha():
            out["speed_symbol"] = ss.upper()
        if _RE_FLAGS.search(t):
            out["xl_flag"] = True
        return out

    m = _RE_METRIC.search(t)
    if m:
        w, a, rim = m.group("w"), m.group("a"), m.group("rim")
        cons = _pick_first_group(m, "cons")
        size_norm = _canon_metric(w, a, rim, cons)
        out.update(
            size_norm=size_norm,
            width_mm=int(w),
            aspect_pct=int(a),
            rim_in=int(rim),
            construction=(cons or "R").upper() if cons else "R",
            raw_pattern="metric",
        )
        li = _pick_first_group(m, "li")
        ss = _pick_first_group(m, "ss")
        if li and li.isdigit():
            out["load_index"] = int(li)
        if ss and ss.isalpha():
            out["speed_symbol"] = ss.upper()
        if _RE_FLAGS.search(t):
            out["xl_flag"] = True
        return out

    m = _RE_MOTO.search(t)
    if m:
        w, a, rim = m.group("w"), m.group("a"), m.group("rim")
        size_norm = _canon_metric(w, a, rim, "R")
        out.update(
            size_norm=size_norm,
            width_mm=int(w),
            aspect_pct=int(a),
            rim_in=int(rim),
            construction="R",
            raw_pattern="moto_dash",
        )
        if _RE_FLAGS.search(t):
            out["xl_flag"] = True
        return out

    m = _RE_FLOT.search(t)
    if m:
        od, section, rim = m.group("od"), m.group("section"), m.group("rim")
        cons = m.group("cons").upper()
        sec = section if "." in section else f"{int(float(section)):.2f}".rstrip("0").rstrip(".")
        odn = od if "." in od else f"{int(float(od))}"
        norm = f"{odn}x{sec}{cons}{int(rim):02d}"
        out.update(
            size_norm=norm,
            construction=cons,
            raw_pattern="flotation",
        )
        if _RE_FLAGS.search(t):
            out["xl_flag"] = True
        return out

    m = _RE_TRUCK.search(t)
    if m:
        sec, rim = m.group("sec"), m.group("rim")
        try:
            secf = float(sec)
            sec_norm = f"{secf:.2f}".rstrip("0").rstrip(".") if "." in sec else f"{secf:.2f}"
        except Exception:
            sec_norm = sec
        norm = f"{sec_norm}-{int(rim):02d}"
        out.update(
            size_norm=norm,
            construction="-",
            raw_pattern="truck_diag",
        )
        if _RE_FLAGS.search(t):
            out["xl_flag"] = True
        return out

    if _RE_FLAGS.search(t):
        out["xl_flag"] = True

    return out
