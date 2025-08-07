# dashboards/dashboard_amazon.py
import json, pandas as pd, matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from typing import Dict
import subprocess as sp
import os, platform

BASE_DIR = Path(__file__).resolve().parents[1] / "amazon" / "data" / "processed"
RAW_DIR  = BASE_DIR / "amazon"
HIST_DIR = BASE_DIR / "historico"

class DashboardAmazon:
    # ---------- carregamento ----------
    def carregar_dados(self) -> Dict[str, pd.DataFrame]:
        """
        Lê **somente** os JSON que terminam em '_top10.json' e
        devolve dict {slug: DataFrame}.
        """
        dados = {}
        padrao = "*_top10.json"                 # ← filtro aplicado aqui
        for f in RAW_DIR.glob(padrao):
            # slug original = tudo antes de '_top10'
            slug = f.stem.rsplit("_top10", 1)[0]
            df   = pd.read_json(f)
            if not df.empty:
                dados[slug] = df
        return dados


    def gerar_dashboard_termo(self, df: pd.DataFrame, slug: str):
        # ---------- estatísticas ----------
        stats = df["preco"].describe()
        media   = stats["mean"]
        mediana = stats["50%"]
        desvio  = stats["std"]
        q1, q3  = stats["25%"], stats["75%"]

        # ---------- box-plot ----------
        fig, ax = plt.subplots()
        ax.boxplot(df["preco"], labels=[slug])
        ax.set_title(f"Distribuição de preços — {slug}")
        ax.set_ylabel("Preço (R$)")
        ax.grid(axis="y")

        # ---------- bloco de texto ----------
        texto = (
            f"N = {int(stats['count'])}\n"
            f"Média  : R$ {media:,.2f}\n"
            f"Mediana: R$ {mediana:,.2f}\n"
            f"Q1 (25%): R$ {q1:,.2f}\n"
            f"Q3 (75%): R$ {q3:,.2f}\n"
            f"σ (std) : R$ {desvio:,.2f}"
        )
        # posição (x=1.15, y=0.5) -> meio do gráfico, um pouco à direita
        ax.text(1.15, 0.5, texto, transform=ax.transAxes,
                va="center", ha="left", fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc="w", ec="gray"))

        plt.tight_layout()
        plt.show()


    # ---------- comparação entre termos ----------
    def comparativo_termos(self, dados: Dict[str, pd.DataFrame]):
        if len(dados) < 2:
            print("⚠️  Precisa de pelo menos 2 termos.")
            return
        # média de cada slug
        medias = {k: v["preco"].mean() for k,v in dados.items()}
        df = pd.Series(medias, name="média").sort_values()
        print("\nMédias por termo:\n", df)

        plt.figure()
        df.plot(kind="bar")
        plt.ylabel("Preço médio (R$)")
        plt.title("Comparativo de médias entre termos")
        plt.tight_layout(); plt.show()

    # ---------- Excel ----------
    # ---------- Excel com Resumo_Geral + Raw ----------
    def gerar_relatorio_excel(self, dados: Dict[str, pd.DataFrame], lim_alerta_pct: float = 5.0):
        from datetime import datetime
        import os, platform, subprocess as sp
        from pathlib import Path

        # --------------- monta Resumo_Geral -----------------
        linhas = []
        hoje   = datetime.now().strftime("%Y-%m-%d")
        for slug, df in dados.items():
            df = df.copy()
            df["data_coleta"] = hoje         # adiciona coluna solicitada

            stats = df["preco"].describe()
            free_pct = (df["frete_gratis"]).mean() * 100

            # tenta pegar snapshot de ontem para variação %
            ontem_stamp = (datetime.now().date()
                           .fromordinal(datetime.now().toordinal()-1)
                           .strftime("%Y%m%d"))
            snap_ontem = (HIST_DIR / f"{slug}_{ontem_stamp}.json")
            if snap_ontem.exists():
                m_yday = pd.read_json(snap_ontem)["preco"].mean()
                var_pct = (stats["mean"] - m_yday) / m_yday * 100
            else:
                var_pct = None

            linhas.append({
                "termo": slug,
                "N": int(stats["count"]),
                "mín": stats["min"], "Q1": stats["25%"],
                "mediana": stats["50%"], "média": stats["mean"],
                "Q3": stats["75%"], "máx": stats["max"],
                "std": stats["std"],
                "% frete grátis": free_pct,
                "var_%_vs_ontem": var_pct
            })

            # atualiza o dict (com data_coleta) para salvar no Raw
            dados[slug] = df

        resumo_df = pd.DataFrame(linhas)

        # --------------- salva Excel ------------------------
        rel_dir = Path(__file__).resolve().parent / "relatorios"
        rel_dir.mkdir(parents=True, exist_ok=True)
        nome = rel_dir / f"relatorio_amazon_{datetime.now():%Y%m%d}.xlsx"

        with pd.ExcelWriter(nome) as xls:
            resumo_df.to_excel(xls, sheet_name="Resumo_Geral", index=False)

            # empilha todos os termos em uma única aba Raw
            raw_df = pd.concat(dados.values(), ignore_index=True)
            raw_df.to_excel(xls, sheet_name="Raw", index=False)

        print(f"📑 Relatório salvo em: {nome}")

        # --------- alerta simples no terminal ---------------
        alertas = resumo_df.dropna(subset=["var_%_vs_ontem"])
        alertas = alertas[alertas["var_%_vs_ontem"].abs() >= lim_alerta_pct]
        for _, row in alertas.iterrows():
            print(f"⚠️  Alerta: {row['termo']} variou {row['var_%_vs_ontem']:+.2f} %")

        # --------- abrir no Excel se usuário quiser ----------
        if input("➜ Abrir no Excel agora? (s/n): ").strip().lower() == "s":
            try:
                sys = platform.system()
                if sys == "Windows":  os.startfile(nome)
                elif sys == "Darwin": sp.call(["open", nome])
                else:                 sp.call(["xdg-open", nome])
            except Exception as e:
                print(f"⚠️  Não foi possivel abrir: {e}")


# ---------- monitoramento de variação ----------
class MonitorAmazon:
    def salvar_snapshot(self, dados: Dict[str, pd.DataFrame]):
        HIST_DIR.mkdir(exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d")
        for slug, df in dados.items():
            path = HIST_DIR / f"{slug}_{stamp}.json"
            df.to_json(path, orient="records", force_ascii=False, indent=2)
        print("💾 Snapshots do dia salvos.")

    def comparar_com_historico(self, slug: str, dias: int = 1):
        hoje  = datetime.now().strftime("%Y%m%d")
        ontem = (datetime.now().date()).fromordinal(
                datetime.now().toordinal()-dias).strftime("%Y%m%d")

        f_today = HIST_DIR / f"{slug}_{hoje}.json"
        f_yday  = HIST_DIR / f"{slug}_{ontem}.json"
        if not (f_today.exists() and f_yday.exists()):
            print(f"⚠️  Sem snapshot para {slug} em {ontem} ou {hoje}")
            return

        df_today = pd.read_json(f_today)
        df_yday  = pd.read_json(f_yday)
        m_today  = df_today["preco"].mean()
        m_yday   = df_yday["preco"].mean()
        var_pct  = (m_today - m_yday)/m_yday*100
        print(f"🔔 {slug}: {m_yday:.2f} → {m_today:.2f}  ({var_pct:+.2f} %)")
