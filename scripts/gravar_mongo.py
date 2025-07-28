from pymongo import MongoClient


def gravar_em_mongo(cnpj, geral, socios, cnaes):
    try:
        # Conectar ao MongoDB
        client = MongoClient("mongodb://mongo:27017/")
        db = client["receita_cnpj"]
        colecao = db["empresas"]

        # Adiciona sócios e cnaes ao dicionário principal
        doc = geral.copy()
        doc["socios"] = socios
        doc["cnaes_secundarios"] = cnaes

        # Upsert (atualiza se já existe, insere se não)
        colecao.update_one({"cnpj": geral.get("cnpj")},
                           {"$set": doc}, upsert=True)

        print(f"✅ Dados gravados no MongoDB para CNPJ: {cnpj}")

    except Exception as e:
        print(f"🚨 Erro ao gravar no MongoDB para {cnpj}: {str(e)}")
