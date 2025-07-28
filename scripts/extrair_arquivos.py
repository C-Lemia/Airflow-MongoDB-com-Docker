def extrair_dados(dados):
    geral = {
        "cnpj": dados.get("cnpj"),
    }

    # Lista de sócios formatada com campos pedidos
    socios_brutos = dados.get("socios", [])
    socios = []
    for s in socios_brutos:
        socios.append({
            "nome_socio": s.get("nome_socio"),
            "cnpj_cpf_do_socio": s.get("cnpj_cpf_do_socio"),
            "qualificacao_socio": s.get("qualificacao_socio"),
            "data_entrada_sociedade": s.get("data_entrada_sociedade"),
            "faixa_etaria": s.get("faixa_etaria"),
            "codigo_faixa_etaria": s.get("codigo_faixa_etaria"),
            "pais": s.get("pais"),
            "codigo_pais": s.get("codigo_pais")
        })

    cnaes_brutos = dados.get("estabelecimento", {}).get(
        "cnaes_secundarios", [])
    cnaes = []
    for c in cnaes_brutos:
        cnaes.append({
            "codigo": c.get("codigo"),
            "descricao": c.get("descricao")
        })

    return geral, socios, cnaes
