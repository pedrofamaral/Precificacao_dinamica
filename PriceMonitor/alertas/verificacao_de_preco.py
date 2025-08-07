"""
Compara o snapshot *novo* (passado via argumento) com o snapshot anterior
do MESMO slug. Dispara e-mail se |Δmédia| ≥ 5 %.
"""
from pathlib import Path
from datetime import datetime
import re
import pandas as pd
from alertas.notificacao_email import send_email

THRESHOLD = 0.05            # 5 %
PRICE_COL = "preco"         # campo já visto no JSON :contentReference[oaicite:0]{index=0}

# ------------ helpers -------------------------------------------------
def _parse_slug(fname: str) -> str:
    """
    Extrai o slug removendo a data final:  foo_bar_top10_20250703.json  → foo_bar_top10
    Se o padrão não bater, devolve fname sem extensão.
    """
    m = re.match(r"(.+?)_\d{8}\.json$", fname)
    return m.group(1) if m else Path(fname).stem

def _mean_price(path: Path) -> float:
    df = pd.read_json(path)
    return df[PRICE_COL].astype(float).mean()

# ------------ API externa --------------------------------------------
def check_variation(new_path: str | Path) -> None:
    new_path = Path(new_path)
    slug     = _parse_slug(new_path.name)

    # pega TODOS os históricos desse slug e ordena por data
    pattern      = f"{slug}_*.json"
    slug_files   = sorted(new_path.parent.glob(pattern))
    if len(slug_files) < 2:
        # primeiro snapshot desse termo ⇒ nada a comparar
        return

    prev_path = slug_files[-2]          # penúltimo = “dia anterior”
    new_mean  = _mean_price(new_path)
    prev_mean = _mean_price(prev_path)
    pct       = (new_mean - prev_mean) / prev_mean

    if abs(pct) >= THRESHOLD:
        direction = "↑ subiu" if pct > 0 else "↓ caiu"
        pct_txt   = f"{pct:+.1%}"

        subject = f"[Alerta Amazon] {slug} {direction} {pct_txt}"
        body = (
            f"🛍️ Amazon – {slug.replace('_', ' ')}\n\n"
            f"Snapshot anterior: {prev_path.name}\n"
            f"   Média: R$ {prev_mean:,.2f}\n\n"
            f"Snapshot atual   : {new_path.name}\n"
            f"   Média: R$ {new_mean:,.2f}\n\n"
            f"Variação: {pct_txt} ({direction})\n"
            f"Data/Hora: {datetime.now():%Y-%m-%d %H:%M:%S}"
        )
        send_email(subject, body)
# ---------------------------------------------------------------------
if __name__ == "__main__":        # uso CLI opcional
    import sys
    if len(sys.argv) != 2:
        sys.exit("Uso: python -m alertas.verificacao_de_preco <snapshot.json>")
    check_variation(sys.argv[1])
