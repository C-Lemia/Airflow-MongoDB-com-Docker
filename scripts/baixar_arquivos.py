import requests
import json
import os


def baixar_dados_cnpj(cnpj, pasta_destino='data'):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

    # A BrasilAPI não exige headers específicos ??
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao buscar CNPJ {cnpj} - Status: {response.status_code}")
        return None

    dados = response.json()

    os.makedirs(pasta_destino, exist_ok=True)
    caminho = os.path.join(pasta_destino, f"{cnpj}.json")
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

    print(f"✅ Dados baixados: {caminho}")
    return dados
