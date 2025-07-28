from airflow import DAG  #--- classe DAG do Airflow
#--- executar funções Python no airflow...
from airflow.operators.python import PythonOperator
from datetime import datetime  #--- representar datas e horas
import requests  #--- requisições HTTP
import json  #--- json
import os  #--- sistema de arquivos - pasta data
import csv  #--- csv
from pymongo import MongoClient  #--- banco de dados MongoDB
from datetime import timedelta  #--- DAG roda uma vez por dia


#--- Lista de CNPJ para processar
CNPJS = ["60444437000146", "33050071023956", '60701190000104']


#--- Requisição a API 

def consultar_e_salvar():
    for cnpj in CNPJS:
        url = f"https://api.cnpjs.dev/v1/{cnpj}"
        response = requests.get(url)

        if response.status_code != 200:  #--- Ignora CNPJ com erro na API e segue para o próximo
            print(f"Erro ao consultar CNPJ {cnpj}: {response.status_code}")
            continue

        dados = response.json()
        print(f"Dados recebidos para {cnpj}")

        #--- Forçar formato de CNPJ como string
        dados["cnpj"] = str(dados["cnpj"]).zfill(14)

        #--- Criar pasta se necessário
        os.makedirs("data", exist_ok=True)

        #--- Salvar JSON bruto
        with open(f"data/{cnpj}.json", "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)

        #--- Gravar no MongoDB
        try:
            client = MongoClient("mongodb://mongo:27017/")
            db = client["receita_cnpj"]
            colecao = db["empresas"]
            colecao.update_one({"cnpj": dados["cnpj"]}, {
                               "$set": dados}, upsert=True)
            print(f"Gravado no MongoDB: {cnpj}")
        except Exception as e:
            print(f"Erro no MongoDB: {e}")

        #--- Criar CSVs
        #--- csv geral
        dados_geral = {k: v for k, v in dados.items() if k not in [
            "socios", "estabelecimento"]}
        with open(f"data/{cnpj}_geral.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=dados_geral.keys())
            writer.writeheader()
            writer.writerow(dados_geral)

        socios = dados.get("socios", [])
        for s in socios:
            if s.get("cnpj_cpf_do_socio"):
                s["cnpj_cpf_do_socio"] = str(s["cnpj_cpf_do_socio"]).zfill(
                    14 if len(str(s["cnpj_cpf_do_socio"])) > 11 else 11)

        if socios:
            with open(f"data/{cnpj}_socios.csv", "w", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=socios[0].keys())
                writer.writeheader()
                writer.writerows(socios)

        # csv de CNAE secundários
        cnaes = dados.get("estabelecimento", {}).get("cnaes_secundarios", [])
        if cnaes:
            with open(f"data/{cnpj}_cnaes.csv", "w", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=cnaes[0].keys())
                writer.writeheader()
                writer.writerows(cnaes)


# DAG do Airflow
with DAG(
    dag_id="etl_cnpj_simples",
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(days=1),  #--- day
    catchup=False,
    tags=["cnpj", "mongo"]
) as dag:

    tarefa_etl = PythonOperator(
        task_id="consultar_salvar_cnpjs",
        python_callable=consultar_e_salvar
    )
