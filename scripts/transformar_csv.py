import json
import csv
import os


def transformar_para_csv(cnpj, pasta_origem='data', pasta_saida='data'):
    json_path = os.path.join(pasta_origem, f"{cnpj}.json")
    with open(json_path, encoding='utf-8') as f:
        dados = json.load(f)

    # Criar pastas se necessário
    os.makedirs(pasta_saida, exist_ok=True)

    # Forçar CNPJ como string com 14 dígitos
    if "cnpj" in dados:
        dados["cnpj"] = str(dados["cnpj"]).zfill(14)

    # CSV principal
    dados_gerais = {k: v for k, v in dados.items() if k not in [
        "socios", "cnaes_secundarios"]}
    csv_principal = os.path.join(pasta_saida, f"{cnpj}_geral.csv")
    with open(csv_principal, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=dados_gerais.keys())
        writer.writeheader()
        writer.writerow(dados_gerais)
    print(f"CSV principal gerado: {csv_principal}")

    # 2CSV de sócios
    csv_socios = os.path.join(pasta_saida, f"{cnpj}_socios.csv")
    socios = dados.get("socios", [])
    if socios:
        # Forçar cnpj_cpf_do_socio como texto com 11 ou 14 dígitos (detecta automaticamente)
        for s in socios:
            doc = s.get("cnpj_cpf_do_socio", "")
            if doc:
                s["cnpj_cpf_do_socio"] = str(doc).zfill(
                    14 if len(str(doc)) > 11 else 11)

        with open(csv_socios, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=socios[0].keys())
            writer.writeheader()
            writer.writerows(socios)
        print(f"CSV de sócios gerado: {csv_socios}")
    else:
        print(f"Nenhum sócio encontrado para {cnpj}.")

    # CSV de CNAEs secundários
    csv_cnaes = os.path.join(pasta_saida, f"{cnpj}_cnaes.csv")
    cnaes = dados.get("cnaes_secundarios", [])
    if cnaes:
        with open(csv_cnaes, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=cnaes[0].keys())
            writer.writeheader()
            writer.writerows(cnaes)
        print(f"CSV de CNAEs gerado: {csv_cnaes}")
    else:
        print(f"Nenhum CNAE secundário encontrado para {cnpj}.")
