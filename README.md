# CNPJ Airflow Pipeline

Este projeto implementa uma pipeline de dados utilizando **Apache Airflow + MongoDB via Docker** para consultar dados de CNPJs através da API pública [CNPJs.dev](https://cnpjs.dev/).

Exemplo de resposta:

<img width="676" height="370" alt="image" src="https://github.com/user-attachments/assets/cdac7e28-3cfb-41cc-ac80-037722778fc6" />


## Tecnologias Utilizadas

- Apache Airflow
- Python 3.12
- MongoDB 
- Docker / Docker Compose
- API pública de CNPJs (https://cnpjs.dev)
- pymongo

---

## Estrutura do Projeto

```plaintext
cnpj_airflow_pipeline/
├── dags/                  # DAGs do Airflow
├── data/                  # Pasta de dados 
├── logs/                  # Logs do Airflow (ignorado no git)
├── docker-compose.yml     # Orquestra Airflow + MongoDB
├── Dockerfile             # Personalização do container 
├── requirements.txt       # Dependências do projeto
└── .gitignore             # Arquivos ignorados no Git
```

## Como Executar Localmente

1. **Clone o repositório**:
   ```bash
   git clone https://github.com/C-Lemia/Airflow-MongoDB-com-Docker
   cd cnpj_airflow_pipeline
   ```


2. Suba os containers com Docker Compose:
```bash
   docker compose up -d
```
3. Acesse o Airflow:
```bash
   http://localhost:8080
```
4. Execute a DAG manualmente:
   
-Acesse a interface do Airflow

-Ative e rode a DAG dag_consulta_cnpj 

<img width="1743" height="906" alt="image" src="https://github.com/user-attachments/assets/4825edeb-5a8a-4fa4-863e-87dfdb8c9811" />
