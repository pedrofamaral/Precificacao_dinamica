from dashboard_amazon import DashboardAmazon, MonitorAmazon

def executar_analise_completa(dash: DashboardAmazon, mon: MonitorAmazon):
    dados = dash.carregar_dados()
    if not dados:
        print("❌ Nenhum dado encontrado. Rode o scraper antes.")
        return

    # 1) dashboards individuais
    for slug, df in dados.items():
        dash.gerar_dashboard_termo(df, slug)

    # 2) comparativo entre termos
    dash.comparativo_termos(dados)

    # 3) relatório Excel
    dash.gerar_relatorio_excel(dados)

    # 4) snapshots + variação diária
    mon.salvar_snapshot(dados)
    for slug in dados.keys():
        mon.comparar_com_historico(slug)


def main():
    print("🎯 ANALISADOR DE PREÇOS - AMAZON")
    print("="*50)

    opcoes = {
        "1": "Executar análise completa",
        "2": "Dashboards individuais",
        "3": "Comparativo entre termos",
        "4": "Relatório Excel",
        "5": "Monitorar variações de preço",
        "0": "Sair"
    }

    dash   = DashboardAmazon()
    mon    = MonitorAmazon()

    while True:
        print("\nOpções:")
        for k,v in opcoes.items(): print(f"{k}. {v}")
        esc = input("Escolha: ").strip()

        if esc == "0":
            print("👋 Até logo!"); break
        elif esc == "1":
            executar_analise_completa(dash,mon)

        else:
            dados = dash.carregar_dados()
            if not dados:
                print("❌ Nenhum dado encontrado. Rode o scraper antes."); continue

            if esc == "2":
                for slug, df in dados.items():
                    dash.gerar_dashboard_termo(df, slug)
            elif esc == "3":
                dash.comparativo_termos(dados)
            elif esc == "4":
                dash.gerar_relatorio_excel(dados)
            elif esc == "5":
                mon.salvar_snapshot(dados)
                for slug in dados.keys():
                    mon.comparar_com_historico(slug)
            else:
                print("❌ Opção inválida!")

if __name__ == "__main__":
    main()
