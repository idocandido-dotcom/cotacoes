#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nordeste Agro — Coletor Automático de Cotações v1.1.7

Melhorias desta versão:
- Mantém AIBA funcionando.
- Mantém CONAB para estados do Nordeste.
- Mantém CEPEA/ESALQ como tentativa de referência, sem travar se houver 403.
- Mantém B3 como referência de mercado futuro, sem criar preço falso por praça.
- Reduz o JSON final:
  * coleta todos os registros brutos;
  * agrupa por produto + estado + praça + unidade + fonte;
  * mostra apenas o preço mais recente na tabela;
  * guarda as observações anteriores em "historico_30d" para o gráfico.
- Converte grãos comercializados em saco para preço por Saca 60 kg:
  * soja, milho, sorgo, arroz e feijão.
  * Exemplo: preço CONAB em Kg x 60 = preço por Saca 60 kg.
- Filtra produtos derivados/industrializados para não poluir a página:
  * óleo de soja, fubá de milho, flocos de milho, semente de feijão,
    farelo, farinha, canjica, creme, ração e similares.
- Remove da página principal cotações antigas:
  * só entram na tabela cotações com data dentro dos últimos 90 dias.
  * cotações antigas continuam fora da página para evitar preço desatualizado.
- Classifica o tipo de preço da CONAB:
  * produtor, atacado, varejo, média UF ou não informado.
  * prioriza produtor/regional na organização dos dados.
- Mantém CEPEA/ESALQ pelo widget oficial no HTML:
  * o widget funciona no navegador do visitante.
  * o GitHub Actions pode receber 403 ao tentar baixar o script.
  * por isso o coletor não força CEPEA no JSON para não gerar erro falso.
- Mantém o layout do site:
  * o HTML continua puxando o mesmo JSON.
  * apenas os campos e informações ficam mais bem classificados.
- Corrige filtros de produtos:
  * remove insumos e serviços que estavam entrando indevidamente:
    inoculante para milho, inoculante para soja e beneficiamento de algodão.
- Valida preço e unidade comercial:
  * remove valores absurdos ou incompatíveis com a unidade.
  * exemplo: milho acima de R$ 200/saca ou algodão abaixo de R$ 50/@ sai da tabela.
  * registra no JSON quantas cotações foram descartadas e por qual motivo.
- Corrige histórico do gráfico:
  * remove datas duplicadas dentro de cada praça/produto.
  * mantém apenas um valor por data no historico_30_dias.
- Melhora classificação visual:
  * reforça Regional, Atacado, Produtor e Média UF para o HTML.
- Política de publicação v1.1.7:
  * publica preço ao produtor, cotação regional produtiva ou referência oficial CONAB.
  * remove Varejo, Atacado e Média UF da tabela principal.
  * CEPEA/ESALQ permanece como widget/indicador de mercado separado no HTML.
  * isso evita que valores de varejo sejam confundidos com preço pago ao produtor.
- Gera:
  * cotacoes/public/cotacoes_nordeste.json
  * cotacoes/public/cotacoes_regionais.json
  * cotacoes/public/cotacoes_nordeste.csv
  * cotacoes/logs/status_ultima_execucao.json
"""

import csv
import io
import json
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


TIMEZONE = "America/Fortaleza"

ROOT_DIR = Path(__file__).resolve().parents[1]
PUBLIC_DIR = ROOT_DIR / "public"
LOGS_DIR = ROOT_DIR / "logs"

OUTPUT_JSON = PUBLIC_DIR / "cotacoes_nordeste.json"
OUTPUT_JSON_REGIONAL = PUBLIC_DIR / "cotacoes_regionais.json"
OUTPUT_CSV = PUBLIC_DIR / "cotacoes_nordeste.csv"
OUTPUT_LOG = LOGS_DIR / "status_ultima_execucao.json"

AIBA_URL = "https://aiba.org.br/cotacoes/"

CONAB_URLS = [
    {
        "nome": "CONAB - Preços Agropecuários Semanal UF",
        "url": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt",
        "tipo": "semanal_uf",
    },
    {
        "nome": "CONAB - Preços Agropecuários Semanal Município",
        "url": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalMunicipio.txt",
        "tipo": "semanal_municipio",
    },
]

CEPEA_INDICADORES = [
    {
        "produto": "Soja",
        "url": "https://cepea.org.br/br/indicador/soja.aspx",
        "praca": "Indicador CEPEA/ESALQ - Paraná",
        "uf": "REF",
        "estado": "Referência CEPEA",
        "unidade": "Saca 60 kg",
    },
    {
        "produto": "Milho",
        "url": "https://cepea.org.br/br/indicador/milho.aspx",
        "praca": "Indicador ESALQ/BM&FBovespa - Campinas/SP",
        "uf": "REF",
        "estado": "Referência CEPEA",
        "unidade": "Saca 60 kg",
    },
    {
        "produto": "Arroz",
        "url": "https://cepea.org.br/br/indicador/arroz.aspx",
        "praca": "Indicador CEPEA/IRGA-RS",
        "uf": "REF",
        "estado": "Referência CEPEA",
        "unidade": "Saca 50 kg",
    },
    {
        "produto": "Algodão",
        "url": "https://cepea.org.br/br/indicador/algodao.aspx",
        "praca": "Indicador Algodão em Pluma CEPEA/ESALQ",
        "uf": "REF",
        "estado": "Referência CEPEA",
        "unidade": "Centavos R$/lp",
    },
    {
        "produto": "Boi Gordo",
        "url": "https://cepea.org.br/br/indicador/boi-gordo.aspx",
        "praca": "Indicador Boi Gordo CEPEA",
        "uf": "REF",
        "estado": "Referência CEPEA",
        "unidade": "@",
    },
]

B3_COMMODITIES_URL = "https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/commodities/"

CEPEA_WIDGET_URL = "https://cepea.org.br/br/widgetproduto.js.php?fonte=arial&tamanho=10&largura=400px&corfundo=dbd6b2&cortexto=333333&corlinha=ede7bf&id_indicador%5B%5D=54&id_indicador%5B%5D=91&id_indicador%5B%5D=50&id_indicador%5B%5D=149&id_indicador%5B%5D=35&id_indicador%5B%5D=53&id_indicador%5B%5D=2&id_indicador%5B%5D=381-56&id_indicador%5B%5D=leitep&id_indicador%5B%5D=77&id_indicador%5B%5D=92"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NordesteAgroBot/1.0; +https://nordesteagro.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

UFS_NORDESTE = {
    "AL": "Alagoas",
    "BA": "Bahia",
    "CE": "Ceará",
    "MA": "Maranhão",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RN": "Rio Grande do Norte",
    "SE": "Sergipe",
}

PRODUTOS_ALVO = [
    "soja",
    "milho",
    "algodao",
    "algodão",
    "sorgo",
    "feijao",
    "feijão",
    "arroz",
    "leite",
    "boi",
    "boi gordo",
    "carne bovina",
]

TIPOS_REAIS = {
    "disponivel",
    "balcao",
    "futuro",
    "spot",
    "pluma",
    "caroco",
    "carioca",
    "preto",
    "verde",
    "irrigado",
    "sequeiro",
    "gordo",
    "vaca",
}

# Produtos que devem ser exibidos no site em preço por Saca 60 kg.
# Regra definida para Nordeste Agro:
# soja, milho, sorgo, arroz e feijão devem aparecer por saca de 60 kg.
PRODUTOS_SACA_60KG = {"Soja", "Milho", "Sorgo", "Arroz", "Feijão"}

# Regra v1.1.7: na CONAB vamos publicar somente os 5 produtos definidos
# para esta etapa da página Cotações. Isso evita que leite/carne/boi entrem
# pela CONAB com unidade ou nível de comercialização inadequado.
PRODUTOS_CONAB_OFICIAIS = {"Soja", "Milho", "Algodão", "Feijão", "Sorgo"}

FONTE_CONAB_POR_PRODUTO = {
    "Soja": "CONAB Produtos 360º / Preços Agropecuários",
    "Milho": "CONAB Produtos 360º / Preços Agropecuários",
    "Algodão": "CONAB Produtos 360º / Preços Agropecuários",
    "Feijão": "CONAB Preços Agropecuários / Preços de Mercado",
    "Sorgo": "CONAB Preços Agropecuários / Preços de Mercado",
}

# Regra de segurança para não mostrar preços antigos como se fossem atuais.
# Se uma praça/produto não tiver cotação dentro desse prazo, ela sai da tabela principal.
DIAS_MAXIMOS_COTACAO_ATIVA = 90

NIVEIS_PRECO_PRIORIDADE = {
    "preco_produtor": 1,
    "preco_regional": 2,
    "preco_referencia_conab": 3,
    "preco_atacado": 4,
    "preco_varejo": 5,
    "media_uf": 6,
    "indicador_mercado": 7,
    "mercado_futuro": 8,
    "nao_informado": 99,
}

# Faixas de segurança para evitar que valores absurdos sejam publicados.
# Essas faixas não substituem a fonte oficial; elas apenas bloqueiam erro de unidade,
# produto indevido ou conversão incorreta.
FAIXAS_VALIDACAO_COMERCIAL = {
    "Soja": {
        "Saca 60 kg": (50.0, 250.0),
    },
    "Milho": {
        "Saca 60 kg": (20.0, 200.0),
    },
    "Sorgo": {
        "Saca 60 kg": (15.0, 180.0),
    },
    "Arroz": {
        "Saca 60 kg": (30.0, 250.0),
    },
    "Feijão": {
        "Saca 60 kg": (80.0, 700.0),
    },
    "Algodão": {
        "Arroba (@)": (50.0, 300.0),
        "@": (50.0, 300.0),
        "Tonelada": (200.0, 3000.0),
    },
    "Leite": {
        "Litro": (1.0, 10.0),
    },
    "Boi Gordo": {
        "@": (100.0, 500.0),
        "Arroba (@)": (100.0, 500.0),
    },
    "Carne Bovina": {
        "Arroba (@)": (100.0, 1000.0),
        "@": (100.0, 1000.0),
        "Kg": (5.0, 80.0),
    },
}

PRODUTOS_COM_VALIDACAO_OBRIGATORIA = set(FAIXAS_VALIDACAO_COMERCIAL.keys())

# Termos que indicam produtos industrializados, processados, insumos ou derivados.
# Esses itens não devem entrar na página principal de commodities agrícolas.
TERMOS_EXCLUIR_DERIVADOS = [
    "oleo",
    "óleo",
    "oleo de soja",
    "óleo de soja",
    "fuba",
    "fubá",
    "fuba de milho",
    "fubá de milho",
    "floco",
    "flocos",
    "flocos de milho",
    "canjica",
    "farinha",
    "farinha de milho",
    "farinha de arroz",
    "creme",
    "creme de milho",
    "farelo",
    "farelo de soja",
    "torta",
    "torta de algodao",
    "torta de algodão",
    "semente",
    "semente de feijao",
    "semente de feijão",
    "semente de milho",
    "semente de soja",
    "racao",
    "ração",
    "mistura",
    "extrato",
    "derivado",
    "beneficiado",
    "polido",
    "parboilizado",
    "inoculante",
    "inoculante para milho",
    "inoculante para soja",
    "beneficiamento",
    "beneficiamento de algodao",
    "beneficiamento de algodão",
    "servico",
    "serviço",
    "servicos",
    "serviços",
    "adubo",
    "fertilizante",
    "fertilizantes",
    "ureia",
    "uréia",
    "calcario",
    "calcário",
    "defensivo",
    "defensivos",
    "herbicida",
    "fungicida",
    "inseticida",
    "maquina",
    "máquina",
    "maquinario",
    "maquinário",
    "trator",
    "plantio",
    "colheita",
    "pulverizacao",
    "pulverização",
    "frete",
    "transporte",
    "armazenagem",
    "secagem",
    "classificacao",
    "classificação",
]


def agora_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def limpar_texto(valor: Any) -> str:
    texto = str(valor or "").replace("\xa0", " ")
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def remover_acentos(valor: Any) -> str:
    texto = unicodedata.normalize("NFD", str(valor or ""))
    return "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")


def slugify(valor: Any) -> str:
    texto = remover_acentos(valor).lower()
    texto = re.sub(r"[^a-z0-9]+", "-", texto)
    return texto.strip("-")


def chave_normalizada(valor: Any) -> str:
    return slugify(limpar_texto(valor))


def normalizar_nivel_preco(valor: Any) -> tuple[str, str, int]:
    """
    Classifica o tipo de preço para não misturar produtor, atacado,
    varejo, média UF e indicadores de mercado.
    """
    texto = remover_acentos(valor).lower()

    if any(chave in texto for chave in [
        "pago pelo prod",
        "preco pago pelo prod",
        "preço pago pelo prod",
        "recebido pelo prod",
        "preco recebido pelo produtor",
        "preço recebido pelo produtor",
        "produtor",
    ]):
        return "preco_produtor", "Produtor", NIVEIS_PRECO_PRIORIDADE["preco_produtor"]

    if any(chave in texto for chave in [
        "referencia conab",
        "referência conab",
        "oficial conab",
        "conab estadual",
        "conab municipal",
    ]):
        return "preco_referencia_conab", "Referência CONAB", NIVEIS_PRECO_PRIORIDADE["preco_referencia_conab"]

    if "aiba" in texto or "regional" in texto or "oeste baiano" in texto:
        return "preco_regional", "Regional", NIVEIS_PRECO_PRIORIDADE["preco_regional"]

    if "atacado" in texto or "atacadista" in texto:
        return "preco_atacado", "Atacado", NIVEIS_PRECO_PRIORIDADE["preco_atacado"]

    if "varejo" in texto or "varejista" in texto:
        return "preco_varejo", "Varejo", NIVEIS_PRECO_PRIORIDADE["preco_varejo"]

    if "media uf" in texto or "média uf" in texto or "medio uf" in texto or "médio uf" in texto:
        return "media_uf", "Média UF", NIVEIS_PRECO_PRIORIDADE["media_uf"]

    if "cepea" in texto or "esalq" in texto or "indicador" in texto:
        return "indicador_mercado", "Indicador de Mercado", NIVEIS_PRECO_PRIORIDADE["indicador_mercado"]

    if "b3" in texto or "futuro" in texto:
        return "mercado_futuro", "Mercado Futuro", NIVEIS_PRECO_PRIORIDADE["mercado_futuro"]

    return "nao_informado", "Não informado", NIVEIS_PRECO_PRIORIDADE["nao_informado"]


def nome_produto_com_nivel(produto_base: str, tipo_produto: str, nivel_label: str) -> str:
    """
    Mantém o layout da tabela sem criar coluna nova.
    Quando o tipo do preço é importante, ele aparece no nome do produto.
    """
    nome = montar_nome_produto(produto_base, tipo_produto)

    if nivel_label in {"Não informado", "Média UF"}:
        return nome

    if nivel_label in nome:
        return nome

    return f"{nome} — {nivel_label}"


def produto_eh_alvo(produto: Any) -> bool:
    """
    Evita falso positivo por substring.
    Exemplo que NÃO pode entrar:
    - INOCULANTE PARA MILHO
    - INOCULANTE PARA SOJA
    - BENEFICIAMENTO DE ALGODAO

    Entra quando o produto for a commodity principal ou uma variação comercial direta.
    """
    produto_norm = remover_acentos(produto).lower()
    produto_norm = re.sub(r"[^a-z0-9 ]+", " ", produto_norm)
    produto_norm = re.sub(r"\s+", " ", produto_norm).strip()

    # Produtos pecuários/comerciais diretos
    if produto_norm in {
        "boi",
        "boi gordo",
        "carne bovina",
        "leite",
        "leite de vaca",
    }:
        return True

    # Commodities agrícolas diretas
    commodities_diretas = {
        "soja",
        "milho",
        "sorgo",
        "arroz",
        "feijao",
        "feijao carioca",
        "feijao preto",
        "algodao",
        "algodao pluma",
        "algodao em pluma",
        "pluma de algodao",
    }

    if produto_norm in commodities_diretas:
        return True

    # Aceita variações comerciais claras, mas sem insumos/serviços/derivados.
    padroes_permitidos = [
        r"^soja( grao| em grao| disponivel| balcao)?$",
        r"^milho( grao| em grao| disponivel| balcao)?$",
        r"^sorgo( grao| em grao)?$",
        r"^arroz( em casca| casca| irrigado| sequeiro)?$",
        r"^feijao( carioca| preto| verde)?$",
        r"^algodao( em pluma| pluma| caroço| caroco)?$",
        r"^boi( gordo)?$",
        r"^leite( de vaca)?$",
        r"^carne bovina$",
    ]

    return any(re.match(padrao, produto_norm) for padrao in padroes_permitidos)


def produto_eh_derivado_ou_industrializado(produto: Any) -> bool:
    produto_norm = remover_acentos(produto).lower()

    for termo in TERMOS_EXCLUIR_DERIVADOS:
        termo_norm = remover_acentos(termo).lower()
        if termo_norm and termo_norm in produto_norm:
            return True

    return False


def produto_deve_entrar_na_base(produto: Any) -> bool:
    """
    Regras da base Nordeste Agro:
    - Entra: commodity agrícola/pecuária principal.
    - Não entra: derivado, processado, industrializado ou insumo.
    Exemplos excluídos: óleo de soja, fubá de milho, flocos de milho,
    semente de feijão, farelo, farinha, inoculante, beneficiamento e serviços.
    """
    if produto_eh_derivado_ou_industrializado(produto):
        return False

    return produto_eh_alvo(produto)


def normalizar_produto_base(produto: Any) -> str:
    p = limpar_texto(produto)
    p_norm = remover_acentos(p).lower()

    if "soja" in p_norm:
        return "Soja"
    if "milho" in p_norm:
        return "Milho"
    if "algodao" in p_norm:
        return "Algodão"
    if "sorgo" in p_norm:
        return "Sorgo"
    if "feijao" in p_norm:
        return "Feijão"
    if "arroz" in p_norm:
        return "Arroz"
    if "leite" in p_norm:
        return "Leite"
    if "boi" in p_norm:
        return "Boi Gordo"
    if "carne bovina" in p_norm:
        return "Carne Bovina"

    return p.title()


def identificar_tipo_produto(produto_original: Any) -> str:
    texto = remover_acentos(produto_original).lower()

    regras = [
        ("disponivel", "Disponível"),
        ("balcao", "Balcão"),
        ("futuro", "Futuro"),
        ("spot", "Spot"),
        ("pluma", "Pluma"),
        ("caroco", "Caroço"),
        ("carioca", "Carioca"),
        ("preto", "Preto"),
        ("verde", "Verde"),
        ("irrigado", "Irrigado"),
        ("sequeiro", "Sequeiro"),
        ("gordo", "Gordo"),
        ("vaca", "Vaca"),
    ]

    for chave, tipo in regras:
        if chave in texto:
            return tipo

    return "Padrão"


def montar_nome_produto(produto_base: str, tipo_produto: str) -> str:
    tipo = limpar_texto(tipo_produto)

    if not tipo or tipo.lower() == "padrão":
        return produto_base

    if remover_acentos(tipo).lower() in remover_acentos(produto_base).lower():
        return produto_base

    return f"{produto_base} — {tipo}"


def produto_usa_saca_60kg(produto_base: str) -> bool:
    return produto_base in PRODUTOS_SACA_60KG


def unidade_indica_kg(unidade: Any) -> bool:
    u = remover_acentos(unidade).lower().strip()
    return (
        u in {"kg", "quilo", "quilograma", "quilogramas"}
        or " kg" in f" {u} "
        or "quilo" in u
        or "quilograma" in u
    )


def unidade_indica_saca(unidade: Any) -> bool:
    u = remover_acentos(unidade).lower()
    return "saca" in u or "sc" == u.strip() or "sc " in f"{u} "


def inferir_unidade(produto_original: Any, produto_base: str, unidade: Any, fonte: str, preco: Optional[float] = None) -> str:
    unidade_limpa = limpar_texto(unidade)

    if unidade_limpa and unidade_limpa.lower() not in {
        "unidade informada pela fonte",
        "unidade",
        "nan",
        "none",
        "-",
    }:
        return unidade_limpa

    produto_norm = remover_acentos(produto_original).lower()
    base_norm = remover_acentos(produto_base).lower()
    fonte_norm = remover_acentos(fonte).lower()

    # CONAB usa a coluna valor_produto_kg.
    # Para grãos, isso será convertido para Saca 60 kg.
    # Para BOI/BOI GORDO, isso será convertido de Kg para Arroba (@), fator 15.
    # Para CARNE BOVINA, permanece Kg.
    if "conab" in fonte_norm:
        if "leite" in base_norm:
            return "Litro"

        if produto_base == "Boi Gordo" or produto_norm == "boi" or "boi gordo" in produto_norm:
            return "Kg"

        if produto_base == "Carne Bovina" or "carne bovina" in produto_norm:
            return "Kg"

        if produto_usa_saca_60kg(produto_base):
            return "Kg"

        if "algodao" in base_norm:
            # Os arquivos semanais da CONAB costumam trazer preço base em Kg.
            # O Nordeste Agro publica algodão em Arroba (@), então a conversão
            # segura é feita depois em aplicar_conversao_unidade_comercial.
            return "Kg"

    if "leite" in base_norm:
        return "Litro"

    if produto_base == "Boi Gordo":
        return "Arroba (@)"

    if produto_base == "Carne Bovina":
        return "Kg"

    if produto_usa_saca_60kg(produto_base):
        return "Saca 60 kg"

    if "algodao" in base_norm:
        return "@"

    return "Unidade"


def aplicar_conversao_unidade_comercial(
    *,
    produto_base: str,
    preco: float,
    unidade: str,
) -> tuple[float, str, float, bool]:
    """
    Conversões comerciais do Nordeste Agro.

    - Soja, milho, sorgo, arroz e feijão:
      Kg -> Saca 60 kg, fator 60.
    - BOI / BOI GORDO:
      Kg -> Arroba (@), fator 15.
    - CARNE BOVINA:
      Kg -> Arroba equivalente (@), fator 15.
      Observação: quando o dado for varejo, isso é uma equivalência matemática para padronizar a tabela,
      não necessariamente preço de boi gordo recebido pelo produtor.
    - Leite:
      mantém Litro.
    """
    unidade_limpa = limpar_texto(unidade)
    unidade_norm = remover_acentos(unidade_limpa).lower()

    if produto_usa_saca_60kg(produto_base):
        if unidade_indica_saca(unidade):
            return round(preco, 2), "Saca 60 kg", 1.0, False

        if unidade_indica_kg(unidade) or unidade_norm in {"unidade", "unidade informada pela fonte", ""}:
            return round(preco * 60, 2), "Saca 60 kg", 60.0, True

        return round(preco, 2), "Saca 60 kg", 1.0, False

    if produto_base == "Boi Gordo":
        if unidade_norm in {"@", "arroba", "arrobas"} or "arroba" in unidade_norm:
            return round(preco, 2), "Arroba (@)", 1.0, False

        if unidade_indica_kg(unidade) or unidade_norm in {"unidade", "unidade informada pela fonte", ""}:
            return round(preco * 15, 2), "Arroba (@)", 15.0, True

        return round(preco, 2), unidade_limpa or "Unidade informada pela fonte", 1.0, False

    if produto_base == "Carne Bovina":
        if unidade_norm in {"@", "arroba", "arrobas"} or "arroba" in unidade_norm:
            return round(preco, 2), "Arroba (@)", 1.0, False

        if unidade_indica_kg(unidade) or unidade_norm in {"unidade", "unidade informada pela fonte", ""}:
            return round(preco * 15, 2), "Arroba (@)", 15.0, True

        return round(preco, 2), unidade_limpa or "Unidade informada pela fonte", 1.0, False

    if produto_base == "Leite":
        if "litro" in unidade_norm or unidade_norm in {"l", "lt", "litros", "unidade", "unidade informada pela fonte", ""}:
            return round(preco, 2), "Litro", 1.0, False

        return round(preco, 2), unidade_limpa or "Litro", 1.0, False

    if produto_base == "Algodão":
        if unidade_norm in {"@", "arroba", "arrobas"} or "arroba" in unidade_norm:
            return round(preco, 2), "Arroba (@)", 1.0, False

        if unidade_indica_kg(unidade) or unidade_norm in {"unidade", "unidade informada pela fonte", ""}:
            return round(preco * 15, 2), "Arroba (@)", 15.0, True

        if "tonelada" in unidade_norm or unidade_norm in {"t", "ton"}:
            return round(preco, 2), "Tonelada", 1.0, False

    return round(preco, 2), unidade_limpa or "Unidade informada pela fonte", 1.0, False


def parse_preco(valor: Any) -> Optional[float]:
    texto = limpar_texto(valor)
    texto = texto.replace("R$", "").replace("r$", "")
    texto = re.sub(r"[^0-9,.\-]", "", texto)

    if not texto:
        return None

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return None


def parse_percentual(valor: Any) -> Optional[float]:
    texto = limpar_texto(valor)
    match = re.search(r"([+-]?\d+(?:[.,]\d+)?)\s*%", texto)

    if not match:
        return None

    return parse_preco(match.group(1))


def parse_data(valor: Any) -> str:
    texto = limpar_texto(valor)

    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", texto)
    if match:
        dia, mes, ano = match.groups()
        return f"{ano}-{mes}-{dia}"

    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", texto)
    if match:
        dia, mes, ano = match.groups()
        return f"{ano}-{mes}-{dia}"

    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", texto)
    if match:
        return match.group(0)

    return agora_local().date().isoformat()


def data_para_br(data_iso: Any) -> str:
    s = limpar_texto(data_iso)
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)

    if not match:
        return s or agora_local().strftime("%d/%m/%Y")

    ano, mes, dia = match.groups()
    return f"{dia}/{mes}/{ano}"


def data_ordenavel(data_iso: Any) -> str:
    return parse_data(data_iso)


def data_corte_cotacoes_ativas() -> str:
    return (agora_local().date() - timedelta(days=DIAS_MAXIMOS_COTACAO_ATIVA)).isoformat()


def data_dentro_do_limite(data_iso: Any, data_corte_iso: str) -> bool:
    data_item = parse_data(data_iso)
    return data_item >= data_corte_iso


def baixar_texto(url: str, timeout: int = 60) -> str:
    resposta = requests.get(url, headers=HEADERS, timeout=timeout)
    resposta.raise_for_status()

    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return resposta.content.decode(encoding)
        except UnicodeDecodeError:
            continue

    return resposta.content.decode("latin1", errors="ignore")


def normalizar_unidade_validacao(unidade: Any) -> str:
    u = limpar_texto(unidade)
    u_norm = remover_acentos(u).lower()

    if "saca" in u_norm or "60kg" in u_norm or "60 kg" in u_norm:
        return "Saca 60 kg"

    if "arroba" in u_norm or u_norm == "@":
        return "@"

    if "tonelada" in u_norm or u_norm in {"t", "ton"}:
        return "Tonelada"

    if "litro" in u_norm or u_norm == "l":
        return "Litro"

    if u_norm in {"kg", "quilo", "quilograma", "quilogramas"}:
        return "Kg"

    return u


def faixa_validacao_para(produto_base: Any, unidade: Any) -> Optional[tuple[float, float]]:
    produto = limpar_texto(produto_base)
    unidade_norm = normalizar_unidade_validacao(unidade)

    faixas_produto = FAIXAS_VALIDACAO_COMERCIAL.get(produto)

    if not faixas_produto:
        return None

    if unidade_norm in faixas_produto:
        return faixas_produto[unidade_norm]

    # Algodão pode vir como Arroba (@) ou @.
    if produto == "Algodão" and unidade_norm == "@":
        return faixas_produto.get("@") or faixas_produto.get("Arroba (@)")

    return None


def validar_item_publicavel(item: dict[str, Any]) -> tuple[bool, str]:
    """
    Valida se a cotação pode aparecer na tabela principal.

    A validação protege contra:
    - produto correto com unidade errada;
    - preço convertido de forma indevida;
    - valores absurdamente altos ou baixos;
    - categorias que não devem ir para a tabela principal.
    """
    produto_base = limpar_texto(item.get("produto_base"))
    produto_original = limpar_texto(item.get("produto_original"))
    unidade = limpar_texto(item.get("unidade"))
    preco = item.get("preco")
    categoria = limpar_texto(item.get("categoria"))
    fonte = limpar_texto(item.get("fonte"))

    if categoria and categoria != "commodity_agricola":
        return False, f"categoria_nao_publicavel:{categoria}"

    if produto_eh_derivado_ou_industrializado(produto_original):
        return False, "produto_derivado_insumo_servico"

    if produto_base not in PRODUTOS_COM_VALIDACAO_OBRIGATORIA:
        # Produto sem faixa obrigatória fica liberado, desde que seja numérico e positivo.
        try:
            preco_num = float(preco)
        except Exception:
            return False, "preco_nao_numerico"

        if preco_num <= 0:
            return False, "preco_menor_ou_igual_zero"

        return True, "ok_sem_faixa_especifica"

    faixa = faixa_validacao_para(produto_base, unidade)

    if not faixa:
        return False, f"unidade_insegura:{produto_base}:{unidade or 'vazio'}"

    try:
        preco_num = float(preco)
    except Exception:
        return False, "preco_nao_numerico"

    minimo, maximo = faixa

    if preco_num < minimo:
        return False, f"preco_abaixo_da_faixa:{produto_base}:{unidade}:min_{minimo}:valor_{preco_num}"

    if preco_num > maximo:
        return False, f"preco_acima_da_faixa:{produto_base}:{unidade}:max_{maximo}:valor_{preco_num}"

    return True, "ok"


def filtrar_cotacoes_publicaveis(cotacoes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    publicaveis = []
    descartadas = []

    for item in cotacoes:
        ok, motivo = validar_item_publicavel(item)

        if ok:
            item["validacao_publicacao"] = "ok"
            publicaveis.append(item)
            continue

        descartada = {
            "produto": item.get("produto"),
            "produto_base": item.get("produto_base"),
            "produto_original": item.get("produto_original"),
            "uf": item.get("uf"),
            "estado": item.get("estado"),
            "praca": item.get("praca"),
            "unidade": item.get("unidade"),
            "unidade_original": item.get("unidade_original"),
            "preco": item.get("preco"),
            "preco_original": item.get("preco_original"),
            "fonte": item.get("fonte"),
            "data_referencia": item.get("data_referencia"),
            "motivo": motivo,
        }
        descartadas.append(descartada)

    return publicaveis, descartadas


def resumir_descartes(descartadas: list[dict[str, Any]]) -> dict[str, int]:
    resumo: dict[str, int] = {}

    for item in descartadas:
        motivo = limpar_texto(item.get("motivo", "motivo_nao_informado"))
        chave = motivo.split(":")[0]
        resumo[chave] = resumo.get(chave, 0) + 1

    return dict(sorted(resumo.items(), key=lambda par: par[0]))


def nivel_publicavel_produtor(item: dict[str, Any]) -> tuple[bool, str]:
    """
    Política v1.1.7:
    - Publicar preço pago ao produtor quando a fonte informar claramente.
    - Publicar cotação regional produtiva, como AIBA.
    - Publicar referência oficial CONAB para soja, milho, algodão, feijão e sorgo
      quando a base não informar atacado nem varejo.
    - Não publicar varejo.
    - Não publicar atacado.
    - Não publicar indicador de mercado nem mercado futuro.
    """
    nivel = limpar_texto(item.get("nivel_comercializacao_chave"))
    fonte = limpar_texto(item.get("fonte"))

    if nivel == "preco_produtor":
        return True, "ok_preco_produtor"

    # AIBA é a principal referência regional de praça produtora do Oeste Baiano/MATOPIBA.
    # Mantemos como regional produtiva, pois não é varejo nem atacado.
    if nivel == "preco_regional" and "AIBA" in fonte:
        return True, "ok_preco_regional_aiba"

    if nivel == "preco_regional":
        return True, "ok_preco_regional"

    if nivel == "preco_referencia_conab":
        produto_base = limpar_texto(item.get("produto_base"))
        if "CONAB" in fonte and produto_base in PRODUTOS_CONAB_OFICIAIS:
            return True, "ok_referencia_oficial_conab"
        return False, "nivel_conab_referencia_fonte_invalida"

    if nivel == "preco_varejo":
        return False, "nivel_bloqueado_varejo"

    if nivel == "preco_atacado":
        return False, "nivel_bloqueado_atacado"

    if nivel == "media_uf":
        return False, "nivel_bloqueado_media_uf"

    if nivel == "indicador_mercado":
        return False, "nivel_bloqueado_indicador_mercado"

    if nivel == "mercado_futuro":
        return False, "nivel_bloqueado_mercado_futuro"

    return False, f"nivel_bloqueado_{nivel or 'nao_informado'}"


def filtrar_cotacoes_produtor_regional(
    cotacoes: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    publicaveis = []
    descartadas = []

    for item in cotacoes:
        ok, motivo = nivel_publicavel_produtor(item)

        if ok:
            item["politica_publicacao"] = "produtor_regional_ou_referencia_conab"
            publicaveis.append(item)
            continue

        descartadas.append(
            {
                "produto": item.get("produto"),
                "produto_base": item.get("produto_base"),
                "produto_original": item.get("produto_original"),
                "uf": item.get("uf"),
                "estado": item.get("estado"),
                "praca": item.get("praca"),
                "unidade": item.get("unidade"),
                "preco": item.get("preco"),
                "fonte": item.get("fonte"),
                "data_referencia": item.get("data_referencia"),
                "nivel_comercializacao": item.get("nivel_comercializacao"),
                "nivel_comercializacao_chave": item.get("nivel_comercializacao_chave"),
                "motivo": motivo,
            }
        )

    return publicaveis, descartadas



def formatar_preco(preco: Optional[float], unidade: str) -> str:
    if preco is None:
        return ""

    if unidade.lower() == "litro":
        return f"R$ {preco:.4f}".replace(".", ",")

    return f"R$ {preco:.2f}".replace(".", ",")


def criar_item(
    *,
    produto_original: str,
    uf: str,
    estado_nome: str,
    praca: str,
    unidade: str,
    preco: float,
    variacao_percentual: Optional[float],
    data_referencia: str,
    fonte: str,
    fonte_url: str,
    tipo_fonte: str,
    observacao: str,
    nivel_comercializacao: str = "Não informado",
    categoria: str = "commodity_agricola",
    converter_unidade: bool = True,
    preco_formatado_override: Optional[str] = None,
) -> dict[str, Any]:
    produto_base = normalizar_produto_base(produto_original)
    tipo_produto = identificar_tipo_produto(produto_original)

    nivel_chave, nivel_label, nivel_prioridade = normalizar_nivel_preco(nivel_comercializacao)

    if tipo_fonte == "regional" and nivel_chave == "nao_informado":
        nivel_chave, nivel_label, nivel_prioridade = normalizar_nivel_preco("regional")

    if tipo_fonte == "referencia_mercado" and nivel_chave == "nao_informado":
        nivel_chave, nivel_label, nivel_prioridade = normalizar_nivel_preco("indicador")

    produto_nome = nome_produto_com_nivel(produto_base, tipo_produto, nivel_label)

    unidade_original = inferir_unidade(produto_original, produto_base, unidade, fonte, preco)
    preco_original = preco

    if converter_unidade:
        preco_final, unidade_final, fator_conversao, conversao_aplicada = aplicar_conversao_unidade_comercial(
            produto_base=produto_base,
            preco=preco,
            unidade=unidade_original,
        )
    else:
        preco_final = preco
        unidade_final = limpar_texto(unidade_original) or limpar_texto(unidade) or "Unidade"
        fator_conversao = 1.0
        conversao_aplicada = False

    preco_formatado = preco_formatado_override or formatar_preco(preco_final, unidade_final)

    return {
        "produto": produto_nome,
        "produto_base": produto_base,
        "produto_original": limpar_texto(produto_original),
        "tipo_produto": tipo_produto,
        "produto_slug": slugify(produto_nome),
        "uf": uf,
        "estado": estado_nome,
        "praca": limpar_texto(praca),
        "unidade": unidade_final,
        "unidade_original": unidade_original,
        "preco": preco_final,
        "preco_original": preco_original,
        "preco_formatado": preco_formatado,
        "fator_conversao": fator_conversao,
        "conversao_aplicada": conversao_aplicada,
        "moeda": "BRL",
        "variacao_percentual": variacao_percentual,
        "data_referencia": parse_data(data_referencia),
        "fonte": fonte,
        "fonte_url": fonte_url,
        "tipo": tipo_fonte,
        "nivel_comercializacao": nivel_label,
        "nivel_comercializacao_chave": nivel_chave,
        "prioridade_nivel_preco": nivel_prioridade,
        "categoria": categoria,
        "observacao": observacao,
        "historico_30_dias": [],
    }


def cotacao_para_dado_html(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "estado": item.get("uf") or item.get("estado") or "",
        "cidade": item.get("praca") or "",
        "regiao": item.get("estado") or "",
        "produto": item.get("produto") or "",
        "produto_base": item.get("produto_base") or "",
        "produto_original": item.get("produto_original") or "",
        "tipo_produto": item.get("tipo_produto") or "",
        "nivel_comercializacao": item.get("nivel_comercializacao") or "",
        "nivel_comercializacao_chave": item.get("nivel_comercializacao_chave") or "",
        "prioridade_nivel_preco": item.get("prioridade_nivel_preco", 99),
        "categoria": item.get("categoria") or "",
        "valor": item.get("preco"),
        "preco": item.get("preco_formatado") or "",
        "preco_original": item.get("preco_original"),
        "unidade": item.get("unidade") or "",
        "unidade_original": item.get("unidade_original") or "",
        "fator_conversao": item.get("fator_conversao", 1),
        "conversao_aplicada": item.get("conversao_aplicada", False),
        "data": data_para_br(item.get("data_referencia")),
        "data_iso": item.get("data_referencia") or "",
        "fonte": item.get("fonte") or "",
        "fonte_url": item.get("fonte_url") or "",
        "historico_30d": [
            {
                "data": data_para_br(p.get("data")),
                "data_iso": p.get("data"),
                "valor": p.get("valor"),
                "preco": formatar_preco(p.get("valor"), item.get("unidade") or ""),
            }
            for p in item.get("historico_30_dias", [])
        ],
    }


def coletar_aiba(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes = []

    try:
        html = baixar_texto(AIBA_URL)
        soup = BeautifulSoup(html, "html.parser")

        linhas = [
            limpar_texto(linha)
            for linha in soup.get_text("\n", strip=True).splitlines()
            if limpar_texto(linha)
        ]

        for i in range(0, len(linhas) - 3):
            produto_original = linhas[i]
            unidade = linhas[i + 1]
            preco_texto = linhas[i + 2]
            detalhe = linhas[i + 3]

            if not preco_texto.startswith("R$"):
                continue

            if not produto_deve_entrar_na_base(produto_original):
                continue

            preco = parse_preco(preco_texto)

            if preco is None:
                continue

            cotacoes.append(
                criar_item(
                    produto_original=produto_original,
                    uf="BA",
                    estado_nome="Bahia",
                    praca="Oeste Baiano",
                    unidade=unidade,
                    preco=preco,
                    variacao_percentual=parse_percentual(detalhe),
                    data_referencia=parse_data(detalhe),
                    fonte="AIBA",
                    fonte_url=AIBA_URL,
                    tipo_fonte="regional",
                    observacao="Cotação regional do Oeste Baiano/MATOPIBA.",
                )
            )

        status_fontes.append(
            {
                "fonte": "AIBA",
                "url": AIBA_URL,
                "status": "ok",
                "total_registros": len(cotacoes),
            }
        )

    except Exception as erro:
        status_fontes.append(
            {
                "fonte": "AIBA",
                "url": AIBA_URL,
                "status": "erro",
                "erro": str(erro),
            }
        )

    return cotacoes


def detectar_delimitador(texto: str) -> str:
    primeira_linha = texto.splitlines()[0] if texto.splitlines() else ""
    candidatos = [";", "\t", "|", ","]
    contagens = {sep: primeira_linha.count(sep) for sep in candidatos}
    melhor = max(contagens, key=contagens.get)
    return melhor if contagens[melhor] > 0 else ";"


def normalizar_coluna(nome: Any) -> str:
    nome = remover_acentos(nome).lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    return nome.strip("_")


def encontrar_coluna(colunas: list[str], termos: list[str]) -> Optional[str]:
    for col in colunas:
        col_norm = normalizar_coluna(col)
        for termo in termos:
            if normalizar_coluna(termo) in col_norm:
                return col
    return None


def ler_registros_csv(texto: str) -> list[dict[str, str]]:
    delimitador = detectar_delimitador(texto)
    leitor = csv.DictReader(io.StringIO(texto), delimiter=delimitador)

    registros = []

    for linha in leitor:
        if not linha:
            continue

        registros.append(
            {
                limpar_texto(k): limpar_texto(v)
                for k, v in linha.items()
                if k is not None
            }
        )

    return registros


def identificar_colunas_preco_conab(colunas: list[str]) -> list[str]:
    """
    Identifica colunas que podem conter preço na base da CONAB.
    Em alguns arquivos há uma coluna única de preço; em outros, o tipo de preço
    pode aparecer no nome da coluna.
    """
    resultado = []

    for col in colunas:
        col_norm = normalizar_coluna(col)

        if any(chave in col_norm for chave in ["preco", "pre_o", "valor", "vlr"]):
            if not any(ignorar in col_norm for ignorar in ["data", "produto", "codigo", "cod_"]):
                resultado.append(col)

    # Evita duplicar e mantém ordem original.
    vistos = set()
    unicos = []
    for col in resultado:
        if col not in vistos:
            vistos.add(col)
            unicos.append(col)

    return unicos


def detectar_nivel_conab(
    *,
    coluna_preco: str,
    linha: dict[str, str],
    col_nivel: Optional[str],
    nome_fonte: str,
    tipo_fonte_conab: str,
    produto_base: str,
) -> str:
    candidatos = []

    if col_nivel:
        candidatos.append(linha.get(col_nivel, ""))

    candidatos.append(coluna_preco)
    candidatos.append(nome_fonte)

    texto = " ".join(limpar_texto(c) for c in candidatos if c)
    texto_norm = remover_acentos(texto).lower()

    if "atacado" in texto_norm or "varejo" in texto_norm or "consumidor" in texto_norm:
        return texto

    if "produtor" in texto_norm or "recebido" in texto_norm or "pago" in texto_norm:
        return texto

    if produto_base in PRODUTOS_CONAB_OFICIAIS and tipo_fonte_conab == "semanal_municipio":
        # Regra v1.1.7: quando a base semanal por município da CONAB não informa
        # explicitamente atacado, varejo ou produtor, ela entra como referência
        # oficial CONAB por praça. Não rotulamos como "Produtor" para não criar
        # uma informação que a própria linha não informou.
        return "referencia conab municipal"

    if produto_base in PRODUTOS_CONAB_OFICIAIS and tipo_fonte_conab == "semanal_uf":
        # Regra v1.1.7: quando a base semanal por UF da CONAB não informa nível
        # de comercialização, ela entra como referência oficial estadual CONAB.
        # Atacado e varejo continuam bloqueados acima.
        return "referencia conab estadual"

    return texto


def coletar_conab(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes = []

    for fonte in CONAB_URLS:
        nome = fonte["nome"]
        url = fonte["url"]
        total = 0

        try:
            texto = baixar_texto(url, timeout=120)
            registros = ler_registros_csv(texto)

            if not registros:
                raise RuntimeError("Arquivo CONAB lido, mas sem registros.")

            colunas = list(registros[0].keys())

            col_produto = encontrar_coluna(colunas, ["produto", "produto_descricao", "nome_produto"])
            col_uf = encontrar_coluna(colunas, ["uf", "sigla_uf", "estado"])
            col_praca = encontrar_coluna(colunas, ["municipio", "município", "cidade", "praca", "praça"])
            col_unidade = encontrar_coluna(colunas, ["unidade", "unid", "medida"])
            col_data = encontrar_coluna(colunas, ["data", "dt", "referencia", "referência", "semana"])
            col_nivel = encontrar_coluna(
                colunas,
                [
                    "nivel",
                    "nível",
                    "comercializacao",
                    "comercialização",
                    "mercado",
                    "tipo_preco",
                    "tipo preço",
                    "tipo",
                    "categoria_preco",
                ],
            )

            colunas_preco = identificar_colunas_preco_conab(colunas)

            # Fallback para manter compatibilidade com a versão anterior.
            if not colunas_preco:
                col_preco = encontrar_coluna(colunas, ["preco", "preço", "valor", "vlr"])
                if col_preco:
                    colunas_preco = [col_preco]

            if not col_produto or not col_uf or not colunas_preco:
                raise RuntimeError(f"Colunas principais não identificadas. Colunas: {colunas}")

            for linha in registros:
                uf = limpar_texto(linha.get(col_uf, "")).upper()

                if uf not in UFS_NORDESTE:
                    continue

                produto_original = limpar_texto(linha.get(col_produto, ""))

                if not produto_deve_entrar_na_base(produto_original):
                    continue

                produto_base_conab = normalizar_produto_base(produto_original)

                if produto_base_conab not in PRODUTOS_CONAB_OFICIAIS:
                    continue

                praca = "Média UF"

                if col_praca:
                    praca = limpar_texto(linha.get(col_praca, "")) or "Média UF"

                unidade = limpar_texto(linha.get(col_unidade, "")) if col_unidade else ""
                data_ref = parse_data(linha.get(col_data, "")) if col_data else agora_local().date().isoformat()

                for col_preco in colunas_preco:
                    preco = parse_preco(linha.get(col_preco, ""))

                    if preco is None:
                        continue

                    nivel_texto = detectar_nivel_conab(
                        coluna_preco=col_preco,
                        linha=linha,
                        col_nivel=col_nivel,
                        nome_fonte=nome,
                        tipo_fonte_conab=fonte.get("tipo", ""),
                        produto_base=produto_base_conab,
                    )

                    nivel_chave, nivel_label, _ = normalizar_nivel_preco(nivel_texto)

                    # Se não identificou nada, deixa pelo menos a origem explícita.
                    if nivel_chave == "nao_informado":
                        nivel_texto = "Não informado"

                    cotacoes.append(
                        criar_item(
                            produto_original=produto_original,
                            uf=uf,
                            estado_nome=UFS_NORDESTE[uf],
                            praca=praca,
                            unidade=unidade,
                            preco=preco,
                            variacao_percentual=None,
                            data_referencia=data_ref,
                            fonte=nome,
                            fonte_url=url,
                            tipo_fonte="oficial",
                            nivel_comercializacao=nivel_texto,
                            observacao=(
                                "Preço agropecuário oficial/compilado pela CONAB e parceiros. "
                                f"Fonte operacional: {FONTE_CONAB_POR_PRODUTO.get(produto_base_conab, 'CONAB Preços Agropecuários')}. "
                                f"Nível identificado: {nivel_label}."
                            ),
                        )
                    )

                    total += 1

            status_fontes.append(
                {
                    "fonte": nome,
                    "url": url,
                    "status": "ok",
                    "total_registros": total,
                    "colunas_preco_identificadas": colunas_preco,
                    "coluna_nivel_identificada": col_nivel,
                    "produtos_conab_publicaveis": sorted(PRODUTOS_CONAB_OFICIAIS),
                }
            )

        except Exception as erro:
            status_fontes.append(
                {
                    "fonte": nome,
                    "url": url,
                    "status": "erro",
                    "erro": str(erro),
                }
            )

    return cotacoes


def extrair_primeiro_indicador_cepea(texto_pagina: str) -> Optional[tuple[str, float, Optional[float]]]:
    texto_limpo = re.sub(r"\s+", " ", texto_pagina)

    padroes = [
        r"(\d{2}/\d{2}/\d{4})\s+([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]+)\s+([+-]?[0-9]+,[0-9]+%)",
        r"(\d{2}-\d{2}-\d{4})\s+[^0-9]{0,40}?([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]+)\s+([+-]?[0-9]+,[0-9]+%)",
    ]

    for padrao in padroes:
        match = re.search(padrao, texto_limpo)

        if match:
            data, valor, variacao = match.groups()
            preco = parse_preco(valor)

            if preco is None:
                continue

            return parse_data(data), preco, parse_percentual(variacao)

    return None


def decodificar_html_widget_cepea(js_texto: str) -> str:
    """
    O widget oficial da CEPEA retorna JavaScript com HTML.
    Esta função tenta transformar esse JavaScript em HTML legível para parser.
    """
    partes = []

    for chamada in re.findall(r"document\\.write\\((.*?)\\);", js_texto, flags=re.S):
        for trecho in re.findall(r"""['"]((?:\\\\.|[^'"\\\\])*)['"]""", chamada, flags=re.S):
            try:
                partes.append(bytes(trecho, "utf-8").decode("unicode_escape"))
            except Exception:
                partes.append(trecho)

    if partes:
        html = "".join(partes)
    else:
        html = js_texto

    html = (
        html.replace("\\/", "/")
        .replace('\\"', '"')
        .replace("\\'", "'")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
    )

    return html


def parse_preco_cepea(valor: Any) -> Optional[float]:
    return parse_preco(valor)


def extrair_linhas_widget_cepea(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    linhas = []

    for tr in soup.find_all("tr"):
        celulas = tr.find_all(["td", "th"])

        if len(celulas) < 3:
            continue

        data = limpar_texto(celulas[0].get_text(" ", strip=True))
        produto_bruto = limpar_texto(celulas[1].get_text("|", strip=True))
        valor = limpar_texto(celulas[2].get_text(" ", strip=True))

        if not re.search(r"\\d{2}/\\d{2}/\\d{4}|\\d{2}/\\d{4}|\\d{2}/\\d{2}", data):
            continue

        partes_produto = [limpar_texto(p) for p in produto_bruto.split("|") if limpar_texto(p)]

        produto = partes_produto[0] if partes_produto else produto_bruto
        unidade = partes_produto[1] if len(partes_produto) > 1 else "Indicador CEPEA"

        preco = parse_preco_cepea(valor)

        if preco is None:
            continue

        linhas.append(
            {
                "data": data,
                "produto": produto,
                "unidade": unidade,
                "valor_texto": valor,
                "preco": preco,
            }
        )

    # Fallback quando o HTML do widget vier como texto quebrado e não como tabela.
    if not linhas:
        texto = soup.get_text("\\n", strip=True)
        tokens = [limpar_texto(t) for t in texto.splitlines() if limpar_texto(t)]

        for i in range(0, len(tokens) - 2):
            if re.search(r"\\d{2}/\\d{2}/\\d{4}|\\d{2}/\\d{4}", tokens[i]) and parse_preco_cepea(tokens[i + 2]) is not None:
                linhas.append(
                    {
                        "data": tokens[i],
                        "produto": tokens[i + 1],
                        "unidade": "Indicador CEPEA",
                        "valor_texto": tokens[i + 2],
                        "preco": parse_preco_cepea(tokens[i + 2]) or 0,
                    }
                )

    return linhas


def coletar_cepea_widget(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    CEPEA/ESALQ:
    - O widget oficial funciona corretamente no navegador do visitante dentro do HTML.
    - No GitHub Actions, o servidor da CEPEA pode retornar 403 para a tentativa de coleta.
    - Portanto, o coletor não baixa o widget para o JSON.
    - A página mantém a seção visual CEPEA/ESALQ via script oficial no WordPress.
    """
    status_fontes.append(
        {
            "fonte": "CEPEA/ESALQ Widget",
            "url": CEPEA_WIDGET_URL,
            "status": "widget_frontend",
            "total_registros": 0,
            "observacao": (
                "Widget oficial mantido no HTML/WordPress. Não coletado pelo GitHub Actions "
                "para evitar erro 403 e duplicidade na tabela principal."
            ),
        }
    )

    return []


def coletar_cepea(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes = []

    for indicador in CEPEA_INDICADORES:
        try:
            html = baixar_texto(indicador["url"], timeout=90)
            soup = BeautifulSoup(html, "html.parser")
            texto_pagina = soup.get_text(" ", strip=True)

            extraido = extrair_primeiro_indicador_cepea(texto_pagina)

            if not extraido:
                raise RuntimeError("Não foi possível extrair a linha principal do indicador CEPEA.")

            data_ref, preco, variacao = extraido

            cotacoes.append(
                criar_item(
                    produto_original=indicador["produto"] + " — Referência CEPEA/ESALQ",
                    uf=indicador["uf"],
                    estado_nome=indicador["estado"],
                    praca=indicador["praca"],
                    unidade=indicador["unidade"],
                    preco=preco,
                    variacao_percentual=variacao,
                    data_referencia=data_ref,
                    fonte="CEPEA/ESALQ",
                    fonte_url=indicador["url"],
                    tipo_fonte="referencia_mercado",
                    observacao="Indicador CEPEA/ESALQ usado como referência nacional/de mercado, não como preço local de praça nordestina.",
                )
            )

            status_fontes.append(
                {
                    "fonte": f"CEPEA/ESALQ - {indicador['produto']}",
                    "url": indicador["url"],
                    "status": "ok",
                    "total_registros": 1,
                }
            )

        except Exception as erro:
            status_fontes.append(
                {
                    "fonte": f"CEPEA/ESALQ - {indicador['produto']}",
                    "url": indicador["url"],
                    "status": "erro",
                    "erro": str(erro),
                }
            )

    return cotacoes


def registrar_b3(status_fontes: list[dict[str, Any]]) -> None:
    status_fontes.append(
        {
            "fonte": "B3 - Commodities",
            "url": B3_COMMODITIES_URL,
            "status": "referencia",
            "total_registros": 0,
            "observacao": "Fonte de referência para mercado futuro. Não foram criados preços por praça/estado para evitar dados simulados.",
        }
    )


def chave_agrupamento(item: dict[str, Any]) -> tuple[str, ...]:
    """
    Define a linha única da tabela final.
    O histórico fica agrupado dentro desta chave.
    """
    return (
        chave_normalizada(item.get("fonte")),
        chave_normalizada(item.get("produto_base")),
        chave_normalizada(item.get("produto_original")),
        chave_normalizada(item.get("uf")),
        chave_normalizada(item.get("praca")),
        chave_normalizada(item.get("unidade")),
        chave_normalizada(item.get("nivel_comercializacao_chave")),
        chave_normalizada(item.get("categoria")),
    )


def consolidar_mais_recentes(
    cotacoes_brutas: list[dict[str, Any]],
    data_corte_iso: str,
) -> tuple[list[dict[str, Any]], int, int]:
    """
    Consolida a base para uso no site.

    Regra v1.1.7:
    - Agrupa todos os registros brutos por fonte + produto + UF + praça + unidade.
    - Dentro de cada grupo, mantém apenas o item mais recente.
    - Se o item mais recente do grupo for mais antigo que data_corte_iso, o grupo inteiro
      fica fora da tabela principal.
    - O histórico do gráfico também fica limitado ao período recente.
    - O histórico é deduplicado por data para evitar datas repetidas no gráfico.
    """
    grupos: dict[tuple[str, ...], list[dict[str, Any]]] = {}

    for item in cotacoes_brutas:
        grupos.setdefault(chave_agrupamento(item), []).append(item)

    consolidadas = []
    grupos_descartados_por_data = 0

    for _, itens in grupos.items():
        itens_ordenados = sorted(
            itens,
            key=lambda x: data_ordenavel(x.get("data_referencia")),
        )

        itens_recentes = [
            item
            for item in itens_ordenados
            if data_dentro_do_limite(item.get("data_referencia"), data_corte_iso)
        ]

        if not itens_recentes:
            grupos_descartados_por_data += 1
            continue

        historico_por_data: dict[str, float] = {}

        for p in itens_recentes:
            valor = p.get("preco")
            data_item = p.get("data_referencia")

            if valor is None or not data_item:
                continue

            # Mantém apenas um valor por data. Como a lista está ordenada,
            # se houver repetição da mesma data, o último valor substitui o anterior.
            historico_por_data[str(data_item)] = valor

        historico = [
            {
                "data": data,
                "valor": valor,
            }
            for data, valor in sorted(historico_por_data.items(), key=lambda par: par[0])
        ][-30:]

        mais_recente = dict(itens_recentes[-1])
        mais_recente["historico_30_dias"] = historico

        consolidadas.append(mais_recente)

    consolidadas.sort(
        key=lambda item: (
            item.get("prioridade_nivel_preco", 99),
            0 if "AIBA" in str(item.get("fonte", "")) else 1,
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("produto_base", ""),
            item.get("produto_original", ""),
            item.get("fonte", ""),
        )
    )

    return consolidadas, len(grupos), grupos_descartados_por_data


def salvar_csv(cotacoes: list[dict[str, Any]]) -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    campos = [
        "produto",
        "produto_base",
        "produto_original",
        "tipo_produto",
        "nivel_comercializacao",
        "nivel_comercializacao_chave",
        "prioridade_nivel_preco",
        "produto_slug",
        "uf",
        "estado",
        "praca",
        "unidade",
        "unidade_original",
        "preco",
        "preco_original",
        "preco_formatado",
        "fator_conversao",
        "conversao_aplicada",
        "moeda",
        "variacao_percentual",
        "data_referencia",
        "fonte",
        "fonte_url",
        "tipo",
        "categoria",
        "observacao",
    ]

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=campos, delimiter=";")
        escritor.writeheader()

        for cotacao in cotacoes:
            escritor.writerow({campo: cotacao.get(campo, "") for campo in campos})


def salvar_log(payload: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    log = {
        "ok": payload.get("ok"),
        "ultima_sincronizacao": payload.get("ultima_sincronizacao"),
        "total_cotacoes_tabela": payload.get("resumo", {}).get("total_cotacoes_tabela"),
        "total_cotacoes_brutas": payload.get("resumo", {}).get("total_cotacoes_brutas"),
        "total_dados_html": payload.get("resumo", {}).get("total_dados_html"),
        "total_cotacoes_descartadas_por_validacao": payload.get("resumo", {}).get("total_cotacoes_descartadas_por_validacao"),
        "total_cotacoes_descartadas_por_nivel": payload.get("resumo", {}).get("total_cotacoes_descartadas_por_nivel"),
        "resumo_descartes_validacao": payload.get("resumo", {}).get("resumo_descartes_validacao"),
        "resumo_descartes_nivel": payload.get("resumo", {}).get("resumo_descartes_nivel"),
        "data_limite_cotacoes_ativas": payload.get("data_limite_cotacoes_ativas"),
        "dias_maximos_cotacao_ativa": payload.get("dias_maximos_cotacao_ativa"),
        "grupos_descartados_por_data_antiga": payload.get("resumo", {}).get("grupos_descartados_por_data_antiga"),
        "fontes": payload.get("fontes"),
    }

    OUTPUT_LOG.write_text(
        json.dumps(log, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    inicio = agora_local()
    status_fontes: list[dict[str, Any]] = []

    cotacoes_brutas: list[dict[str, Any]] = []
    cotacoes_brutas.extend(coletar_aiba(status_fontes))
    cotacoes_brutas.extend(coletar_conab(status_fontes))
    cotacoes_brutas.extend(coletar_cepea_widget(status_fontes))
    registrar_b3(status_fontes)

    cotacoes_validas, cotacoes_descartadas_validacao = filtrar_cotacoes_publicaveis(cotacoes_brutas)
    cotacoes_produtor_regional, cotacoes_descartadas_nivel = filtrar_cotacoes_produtor_regional(cotacoes_validas)

    data_corte_iso = data_corte_cotacoes_ativas()
    cotacoes_tabela, total_grupos_brutos, grupos_descartados_por_data = consolidar_mais_recentes(
        cotacoes_produtor_regional,
        data_corte_iso,
    )
    dados_html = [cotacao_para_dado_html(item) for item in cotacoes_tabela]

    fontes_ok = [f["fonte"] for f in status_fontes if f.get("status") == "ok"]
    fontes_erro = [f for f in status_fontes if f.get("status") == "erro"]

    payload = {
        "ok": True,
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes",
        "repositorio": "idocandido-dotcom/cotacoes",
        "versao": "1.1.7",
        "ultima_sincronizacao": agora_local().strftime("%Y-%m-%d %H:%M:%S"),
        "ultima_sincronizacao_iso": agora_local().isoformat(),
        "gerado_em": agora_local().strftime("%d/%m/%Y %H:%M"),
        "fuso_horario": TIMEZONE,
        "frequencia_atualizacao": "diaria",
        "dias_maximos_cotacao_ativa": DIAS_MAXIMOS_COTACAO_ATIVA,
        "data_limite_cotacoes_ativas": data_corte_iso,
        "politica_atualidade": "A tabela principal exibe somente cotações com data dentro dos últimos 90 dias.",
        "fonte_principal": "AIBA/CONAB Produtos 360º e Preços Agropecuários",
        "fontes_complementares": ["CEPEA/ESALQ Widget no HTML", "B3 - referência de mercado futuro"],
        "politica_classificacao_preco": (
            "Política v1.1.7: a tabela principal publica preço pago ao produtor quando a fonte informar, "
            "cotação regional produtiva e referência oficial CONAB para soja, milho, algodão, feijão e sorgo. "
            "Varejo, atacado, indicador de mercado e mercado futuro ficam fora da tabela principal. "
            "Referência CONAB não é rotulada como preço ao produtor quando a linha não trouxer essa informação."
        ),
        "resumo": {
            "total_cotacoes_tabela": len(cotacoes_tabela),
            "total_cotacoes_brutas": len(cotacoes_brutas),
            "total_cotacoes_validas_apos_validacao": len(cotacoes_validas),
            "total_cotacoes_publicaveis_produtor_regional": len(cotacoes_produtor_regional),
            "total_cotacoes_descartadas_por_validacao": len(cotacoes_descartadas_validacao),
            "total_cotacoes_descartadas_por_nivel": len(cotacoes_descartadas_nivel),
            "resumo_descartes_validacao": resumir_descartes(cotacoes_descartadas_validacao),
            "resumo_descartes_nivel": resumir_descartes(cotacoes_descartadas_nivel),
            "total_dados_html": len(dados_html),
            "total_grupos_brutos": total_grupos_brutos,
            "grupos_descartados_por_data_antiga": grupos_descartados_por_data,
            "total_indicadores_cepea_widget": len([item for item in cotacoes_tabela if item.get("fonte") == "CEPEA/ESALQ Widget"]),
            "total_precos_produtor": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_produtor"]),
            "total_precos_regionais": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_regional"]),
            "total_referencias_conab": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_referencia_conab"]),
            "total_precos_atacado": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_atacado"]),
            "total_precos_media_uf": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "media_uf"]),
            "total_cotacoes_boi_gordo": len([item for item in cotacoes_tabela if item.get("produto_base") == "Boi Gordo"]),
            "total_cotacoes_carne_bovina": len([item for item in cotacoes_tabela if item.get("produto_base") == "Carne Bovina"]),
            "total_indicadores_mercado": len([item for item in cotacoes_tabela if item.get("categoria") == "indicador_mercado"]),
            "fontes_com_sucesso": fontes_ok,
            "total_fontes_com_erro": len(fontes_erro),
            "tempo_execucao_segundos": round((agora_local() - inicio).total_seconds(), 2),
        },
        "fontes": status_fontes,
        "cotacoes": cotacoes_tabela,
        "dados": dados_html,
        "historico_30_dias": {},
        "validacao_comercial": {
            "descricao": (
                "Validação automática para remover cotações com unidade insegura, "
                "preço absurdo ou produto incompatível com a tabela principal."
            ),
            "faixas": FAIXAS_VALIDACAO_COMERCIAL,
            "total_descartadas": len(cotacoes_descartadas_validacao),
            "resumo_descartes": resumir_descartes(cotacoes_descartadas_validacao),
            "amostra_descartadas": cotacoes_descartadas_validacao[:50],
        },
        "filtro_nivel_comercializacao": {
            "descricao": (
                "Filtro final de publicação. A tabela principal publica preço pago ao produtor, "
                "cotação regional produtiva e referência oficial CONAB para os produtos principais. "
                "Varejo, atacado, indicador de mercado e mercado futuro são removidos."
            ),
            "niveis_permitidos": ["preco_produtor", "preco_regional", "preco_referencia_conab"],
            "niveis_bloqueados": ["preco_varejo", "preco_atacado", "indicador_mercado", "mercado_futuro", "nao_informado"],
            "total_descartadas": len(cotacoes_descartadas_nivel),
            "resumo_descartes": resumir_descartes(cotacoes_descartadas_nivel),
            "amostra_descartadas": cotacoes_descartadas_nivel[:50],
        },
        "aviso_legal": (
            "As cotações apresentadas pelo Nordeste Agro são referenciais e compiladas "
            "a partir de fontes regionais, oficiais e indicadores de mercado. A partir da versão v1.1.7, "
            "a tabela principal publica preço pago ao produtor quando a fonte informar claramente, "
            "cotação regional produtiva e referência oficial CONAB para soja, milho, algodão, feijão e sorgo. "
            "Cotações de varejo e atacado são removidas para evitar confusão com preço recebido "
            "pelo produtor rural. Soja, milho, sorgo, arroz e feijão são padronizados em preço por Saca 60 kg "
            "quando a fonte vier em Kg. Produtos derivados, industrializados, insumos e serviços são removidos. "
            "CEPEA/ESALQ permanece em seção própria do HTML como indicador de mercado, e B3 permanece apenas como "
            "referência de mercado futuro. Os valores podem variar conforme praça, qualidade, volume, frete, "
            "forma de pagamento, logística e data de atualização."
        ),
    }

    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    OUTPUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    OUTPUT_JSON_REGIONAL.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    salvar_csv(cotacoes_tabela)
    salvar_log(payload)

    print("Coleta finalizada.")
    print(f"Total de cotações brutas: {len(cotacoes_brutas)}")
    print(f"Total de cotações válidas após validação: {len(cotacoes_validas)}")
    print(f"Total de cotações publicáveis produtor/regional: {len(cotacoes_produtor_regional)}")
    print(f"Total de cotações descartadas por validação: {len(cotacoes_descartadas_validacao)}")
    print(f"Total de cotações descartadas por nível: {len(cotacoes_descartadas_nivel)}")
    print(f"Resumo descartes validação: {resumir_descartes(cotacoes_descartadas_validacao)}")
    print(f"Resumo descartes nível: {resumir_descartes(cotacoes_descartadas_nivel)}")
    print(f"Total de grupos brutos: {total_grupos_brutos}")
    print(f"Grupos descartados por data antiga: {grupos_descartados_por_data}")
    print(f"Data limite para tabela: {data_corte_iso}")
    print(f"Total de cotações para tabela: {len(cotacoes_tabela)}")
    print(f"Total de dados HTML: {len(dados_html)}")
    print(f"Total CEPEA Widget: {len([item for item in cotacoes_tabela if item.get('fonte') == 'CEPEA/ESALQ Widget'])}")
    print(f"Total Preço Produtor: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'preco_produtor'])}")
    print(f"Total Preço Regional: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'preco_regional'])}")
    print(f"Total Preço Atacado: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'preco_atacado'])}")
    print(f"Total Média UF: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'media_uf'])}")
    print(f"Total Boi Gordo: {len([item for item in cotacoes_tabela if item.get('produto_base') == 'Boi Gordo'])}")
    print(f"Total Carne Bovina: {len([item for item in cotacoes_tabela if item.get('produto_base') == 'Carne Bovina'])}")
    print(f"JSON principal: {OUTPUT_JSON}")
    print(f"JSON regional: {OUTPUT_JSON_REGIONAL}")
    print(f"CSV: {OUTPUT_CSV}")
    print(f"LOG: {OUTPUT_LOG}")

    if fontes_erro:
        print("Atenção: algumas fontes apresentaram erro:")
        for fonte in fontes_erro:
            print(f"- {fonte.get('fonte')}: {fonte.get('erro')}")


if __name__ == "__main__":
    main()
