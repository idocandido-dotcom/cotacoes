#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nordeste Agro — Coletor Automático de Cotações v1.3.4

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
- Política de publicação v1.3.0:
  * publica preço ao produtor, cotação regional produtiva ou referência oficial CONAB.
  * usa CONAB Produtos 360º como fonte para Soja, Milho e Algodão.
  * usa arquivos semanais da CONAB apenas para Feijão e Sorgo.
  * remove Varejo, Atacado e Média UF da tabela principal.
  * CEPEA/ESALQ permanece como widget/indicador de mercado separado no HTML.
  * isso evita que valores de varejo sejam confundidos com preço pago ao produtor.
- Inclui Pará e Tocantins nas UFs monitoradas para complementar MATOPIBA/Norte.
- Revisa leite e carne: não converte carne bovina em kg para arroba; leite/carnes da CONAB só entram como preço ao produtor.
- v1.3.1: adiciona fallback CEPEA/ESALQ validado para Leite ao Produtor e Boi Gordo:
  * Boi Gordo CEPEA/ESALQ como referência de mercado em R$/arroba.
  * Leite ao Produtor CEPEA/ESALQ Brasil como referência de mercado em R$/litro.
  * Leite ao Produtor CEPEA/ESALQ Bahia como referência regional em R$/litro.
  * Esses registros só entram como referência; não são preço local de PI, MA ou PA.
- v1.3.3: corrige o filtro final de publicação para permitir Leite e Boi Gordo CEPEA/ESALQ:
  * CEPEA/ESALQ entra como Referência CEPEA/ESALQ, não como preço local.
  * A exceção de data vale apenas para referência CEPEA pecuária quando o período oficial é mensal/indicador.
  * PI, MA e PA continuam sem preço simulado quando não houver dado local validado.
- v1.3.3: adiciona correção operacional para CONAB Preços Agropecuários:
  * classifica "PREÇO RECEBIDO" e variações como preço recebido pelo produtor;
  * converte Boi/Boi Gordo informado em R$/kg para Arroba (@), usando fator 15;
  * mantém Leite ao Produtor em Litro;
  * marca Boi Gordo como pecuaria_corte e Leite como pecuaria_leite;
  * não altera soja, milho, algodão, feijão, sorgo e demais produtos já estabelecidos.
- v1.3.4: reforça a regra definida para pecuária CONAB:
  * quando Leite ou Boi Gordo vierem dos arquivos de Preços Agropecuários sem coluna explícita de nível,
    e a linha não indicar atacado, varejo ou consumidor final, o coletor trata como Preço Recebido pelo Produtor;
  * Boi Gordo continua convertido de Kg para Arroba (@), fator 15;
  * Leite continua sem conversão, em Litro;
  * mantém todos os produtos, caminhos, JSONs e rotinas já existentes.
- Gera:
  * cotacoes/public/cotacoes_nordeste.json
  * cotacoes/public/cotacoes_regionais.json
  * cotacoes/public/cotacoes_nordeste.csv
  * cotacoes/logs/status_ultima_execucao.json
  * cotacoes/logs/debug_leite_carne_conab.json
  * cotacoes/logs/debug_ceasas.json
"""

import asyncio
import csv
import io
import json
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse
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
OUTPUT_DEBUG_CONAB_360 = LOGS_DIR / "debug_conab_produtos_360.json"
OUTPUT_DEBUG_SORGO_CONAB = LOGS_DIR / "debug_sorgo_conab.json"
OUTPUT_DEBUG_LEITE_CARNE_CONAB = LOGS_DIR / "debug_leite_carne_conab.json"
OUTPUT_DEBUG_PECUARIA = LOGS_DIR / "debug_pecuaria_fontes.json"
OUTPUT_DEBUG_CEASAS = LOGS_DIR / "debug_ceasas.json"

AIBA_URL = "https://aiba.org.br/cotacoes/"
SEAGRI_BA_COTACOES_URL = "https://www.ba.gov.br/seagri/cotacao"
CEPEA_LEITE_URL = "https://cepea.org.br/br/indicador/leite.aspx"
CEPEA_BOI_GORDO_URL = "https://cepea.org.br/br/indicador/boi-gordo.aspx"

# Referências CEPEA/ESALQ validadas para fallback operacional.
# Uso: somente quando a coleta automática da página CEPEA falhar ou não retornar linhas válidas.
# Regra: são referências de mercado/regional, não preço local de PI, MA ou PA.
CEPEA_FALLBACK_REFERENCIAS = {
    "boi_gordo": {
        "produto_original": "Boi Gordo — Indicador CEPEA/ESALQ",
        "uf": "REF",
        "estado_nome": "Referência CEPEA",
        "praca": "Indicador CEPEA/ESALQ",
        "unidade": "Arroba (@)",
        "preco": 326.00,
        "variacao_percentual": 0.90,
        "data_referencia": "2026-01-28",
        "periodo_referencia": "28/01/2026",
        "fonte_url": CEPEA_BOI_GORDO_URL,
        "tipo_fonte": "oficial",
        "nivel_comercializacao": "referência de preço ao produtor cepea",
        "categoria": "pecuaria_corte",
        "observacao": (
            "Indicador do Boi Gordo CEPEA/ESALQ. Valor à vista por arroba de 15 kg, sem Funrural. "
            "Referência de mercado; não representa preço local da praça selecionada."
        ),
    },
    "leite_brasil": {
        "produto_original": "Leite — Preço ao Produtor CEPEA/ESALQ",
        "uf": "REF",
        "estado_nome": "Referência CEPEA",
        "praca": "Brasil",
        "unidade": "Litro",
        "preco": 2.1122,
        "variacao_percentual": None,
        "data_referencia": "2025-11-01",
        "periodo_referencia": "nov/25",
        "fonte_url": CEPEA_LEITE_URL,
        "tipo_fonte": "oficial",
        "nivel_comercializacao": "preço recebido pelo produtor cepea",
        "categoria": "pecuaria_leite",
        "observacao": (
            "Preço médio do leite ao produtor CEPEA/ESALQ para Brasil. "
            "Referência mensal em R$/litro; não é cotação local de PI, MA ou PA."
        ),
    },
    "leite_bahia": {
        "produto_original": "Leite — Preço ao Produtor CEPEA/ESALQ",
        "uf": "BA",
        "estado_nome": "Bahia",
        "praca": "Bahia",
        "unidade": "Litro",
        "preco": 2.2011,
        "variacao_percentual": None,
        "data_referencia": "2025-11-01",
        "periodo_referencia": "nov/25",
        "fonte_url": CEPEA_LEITE_URL,
        "tipo_fonte": "oficial",
        "nivel_comercializacao": "preço recebido pelo produtor cepea",
        "categoria": "pecuaria_leite",
        "observacao": (
            "Referência regional CEPEA/ESALQ para Bahia. "
            "Não deve ser exibida como preço local de PI, MA ou PA."
        ),
    },
}

ACRIOESTE_URL = "https://acrioeste.org.br/"
AGROLINK_MA_IMPERATRIZ_URL = "https://www.agrolink.com.br/regional/ma/imperatriz/cotacoes"
AGROLINK_PA_MARABA_URL = "https://www.agrolink.com.br/regional/pa/maraba/cotacoes"

PROHORT_PRECO_DIA_URL = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/api/repos/%3Ahome%3APROHORT%3AprecoDia.wcdf/generatedContent?password=password&userid=pentaho"
CEASA_CE_BOLETIM_URL = "https://files.ceasa-ce.com.br/unsima/boletim_diario/boletim.php"
CEASA_PE_COTACAO_URL = "https://www.ceasape.org.br/cotacao"

CONAB_PRODUTOS_360_URL = "https://portaldeinformacoes.conab.gov.br/produtos-360.html"
CONAB_PRODUTOS_360_PENTAHO_URL = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/api/repos/%3Ahome%3AProdutos%3Aprodutos360.wcdf/generatedContent?password=password&userid=pentaho"
CONAB_360_DOQUERY_URL = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery?"

CONAB_PRECOS_AGROPECUARIOS_URL = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios.html"

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

# UFs monitoradas pela página de Cotações.
# Mantemos o nome da constante por compatibilidade com o restante do código,
# mas a partir da v1.3.0 incluímos também Pará e Tocantins por relevância
# agrícola no MATOPIBA/Norte e para complementar as referências da CONAB 360º.
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
    "PA": "Pará",
    "TO": "Tocantins",
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

# Regra v1.3.0: na CONAB vamos publicar somente os 5 produtos definidos
# para esta etapa da página Cotações. Isso evita que leite/carne/boi entrem
# pela CONAB com unidade ou nível de comercialização inadequado.
PRODUTOS_CONAB_OFICIAIS = {"Soja", "Milho", "Algodão", "Feijão", "Sorgo"}

# v1.3.0: soja, milho e algodão não devem mais sair dos arquivos semanais,
# porque esses arquivos trouxeram datas antigas. Para esses três produtos,
# a fonte operacional passa a ser o painel CONAB Produtos 360º.
PRODUTOS_CONAB_360 = {"Soja", "Milho", "Algodão"}

# Feijão e sorgo continuam nos arquivos semanais/Preços Agropecuários.
# A partir da v1.3.0, leite e carnes também são lidos desses arquivos,
# mas com regra mais rígida: só entram na tabela se a linha vier claramente
# como preço ao produtor. Varejo, atacado e nível não informado continuam fora.
PRODUTOS_CONAB_TXT = {"Feijão", "Sorgo", "Leite", "Boi Gordo", "Carne Bovina"}

# Regra definida no projeto Nordeste Agro:
# Leite e Boi Gordo devem usar CONAB Preços Agropecuários como preço recebido pelo produtor
# quando a linha não indicar atacado, varejo, consumidor final ou outra categoria bloqueada.
# Essa exceção não altera os produtos agrícolas já estabelecidos.
PRODUTOS_CONAB_PECUARIA_PRECO_RECEBIDO = {"Leite", "Boi Gordo"}

FONTE_CONAB_POR_PRODUTO = {
    "Soja": "CONAB Produtos 360º / Preços Agropecuários",
    "Milho": "CONAB Produtos 360º / Preços Agropecuários",
    "Algodão": "CONAB Produtos 360º / Preços Agropecuários",
    "Feijão": "CONAB Preços Agropecuários / Preços de Mercado",
    "Sorgo": "CONAB Preços Agropecuários / Preços de Mercado",
    "Leite": "CONAB Preços Agropecuários - somente preço ao produtor",
    "Boi Gordo": "CONAB Preços Agropecuários - somente preço ao produtor",
    "Carne Bovina": "CONAB Preços Agropecuários - somente preço ao produtor",
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
    "preco_atacado_ceasa": 9,
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


# Produtos monitorados no módulo CEASAS/Hortifruti.
# Esta categoria é separada da tabela de commodities ao produtor.
# CEASA representa referência de atacado, não preço recebido pelo produtor.
PRODUTOS_CEASA_MONITORADOS = {
    "abacate", "abacaxi", "abobora", "abóbora", "alface", "alho", "banana",
    "batata", "batata doce", "beterraba", "cebola", "cenoura", "coentro",
    "coco", "goiaba", "inhame", "laranja", "limao", "limão", "maca", "maçã",
    "macaxeira", "mandioca", "mamao", "mamão", "manga", "maracuja", "maracujá",
    "melao", "melão", "milho verde", "pepino", "pimentao", "pimentão",
    "repolho", "tomate", "uva",
}

UNIDADES_CEASA_VALIDAS = [
    "kg", "quilo", "quilograma", "caixa", "cx", "saco", "saca", "sc", "unidade",
    "und", "cento", "duzia", "dúzia", "maco", "maço", "bandeja", "tonelada",
]

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
        "preco recebido p produtor",
        "preço recebido p produtor",
        "preco recebido p/ produtor",
        "preço recebido p/ produtor",
        "preco recebido",
        "preço recebido",
        "preco recebi",
        "preço recebi",
        "produtor",
    ]):
        # CONAB Preços Agropecuários usa no filtro o texto "PREÇO RECEBIDO".
        # No contexto da página, esse nível corresponde ao preço recebido pelo produtor.
        return "preco_produtor", "Produtor", NIVEIS_PRECO_PRIORIDADE["preco_produtor"]

    if any(chave in texto for chave in [
        "referencia conab",
        "referência conab",
        "oficial conab",
        "conab estadual",
        "conab municipal",
    ]):
        return "preco_referencia_conab", "Referência CONAB", NIVEIS_PRECO_PRIORIDADE["preco_referencia_conab"]

    if "seagri" in texto:
        return "preco_regional", "Referência SEAGRI-BA", NIVEIS_PRECO_PRIORIDADE["preco_regional"]

    if "aiba" in texto or "regional" in texto or "oeste baiano" in texto:
        return "preco_regional", "Regional", NIVEIS_PRECO_PRIORIDADE["preco_regional"]

    if "ceasa" in texto or "hortifruti" in texto or "atacado ceasa" in texto:
        return "preco_atacado_ceasa", "Atacado CEASA", NIVEIS_PRECO_PRIORIDADE["preco_atacado_ceasa"]

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
        "bovino",
        "bovinos",
        "carne bovina",
        "carne bovina dianteiro",
        "carne bovina traseiro",
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
        r"^bovino(s)?$",
        r"^leite( de vaca)?$",
        r"^carne bovina( dianteiro| traseiro)?$",
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
    if "carne bovina" in p_norm:
        return "Carne Bovina"
    if "boi" in p_norm or "bovino" in p_norm or "bovinos" in p_norm:
        return "Boi Gordo"

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
    u = u.replace(" ", "")
    return (
        u in {"kg", "kgs", "quilo", "quilos", "quilograma", "quilogramas"}
        or "kg" in u
        or "/kg" in u
        or "r$/kg" in u
        or "rs/kg" in u
        or "quilo" in u
        or "quilograma" in u
    )


def unidade_indica_saca(unidade: Any) -> bool:
    u = remover_acentos(unidade).lower()
    return "saca" in u or "sc" == u.strip() or "sc " in f"{u} "


def unidade_indica_litro(unidade: Any) -> bool:
    u = remover_acentos(unidade).lower().strip()
    u = u.replace(" ", "")
    return (
        u in {"l", "lt", "lts", "litro", "litros"}
        or "litro" in u
        or "/l" in u
        or "r$/l" in u
        or "rs/l" in u
    )


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
      mantém Kg quando a fonte vier em Kg.
      Não convertemos carne bovina para arroba para evitar confundir carne no atacado/varejo
      com boi gordo recebido pelo produtor.
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
            return round(preco, 2), "Kg", 1.0, False

        return round(preco, 2), unidade_limpa or "Kg", 1.0, False

    if produto_base == "Leite":
        if unidade_indica_litro(unidade) or unidade_norm in {"unidade", "unidade informada pela fonte", ""}:
            return round(preco, 4), "Litro", 1.0, False

        return round(preco, 4), unidade_limpa or "Litro", 1.0, False

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


def extrair_periodo_semanal(valor: Any) -> dict[str, str]:
    """
    Interpreta períodos semanais do CONAB Produtos 360º.

    Exemplo de entrada: "27-04-2026 - 01-05-2026".
    Saída: início ISO, fim ISO e texto para a tabela do site.

    A data de referência operacional passa a ser a data final da semana,
    pois o painel representa a última semana publicada, não preço diário.
    """
    texto = limpar_texto(valor)

    padroes = [
        r"(\d{2})-(\d{2})-(\d{4})\s*-\s*(\d{2})-(\d{2})-(\d{4})",
        r"(\d{2})/(\d{2})/(\d{4})\s*-\s*(\d{2})/(\d{2})/(\d{4})",
        r"(\d{2})/(\d{2})/(\d{4})\s*a\s*(\d{2})/(\d{2})/(\d{4})",
        r"(\d{2})-(\d{2})-(\d{4})\s*a\s*(\d{2})-(\d{2})-(\d{4})",
    ]

    for padrao in padroes:
        match = re.search(padrao, texto, flags=re.IGNORECASE)
        if not match:
            continue

        d1, m1, a1, d2, m2, a2 = match.groups()
        inicio_iso = f"{a1}-{m1}-{d1}"
        fim_iso = f"{a2}-{m2}-{d2}"
        return {
            "data_inicio": inicio_iso,
            "data_fim": fim_iso,
            "periodo_referencia": f"{d1}/{m1}/{a1} a {d2}/{m2}/{a2}",
            "periodo_original": texto,
        }

    data_iso = parse_data(texto)
    return {
        "data_inicio": data_iso,
        "data_fim": data_iso,
        "periodo_referencia": "",
        "periodo_original": texto,
    }


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
    u_norm = remover_acentos(u).lower().strip()
    u_compacta = u_norm.replace(" ", "")

    if "saca" in u_norm or "60kg" in u_compacta or "60 kg" in u_norm:
        return "Saca 60 kg"

    if "arroba" in u_norm or u_norm == "@":
        return "@"

    if "tonelada" in u_norm or u_norm in {"t", "ton"}:
        return "Tonelada"

    if unidade_indica_litro(unidade):
        return "Litro"

    if unidade_indica_kg(unidade):
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

    categorias_publicaveis = {"commodity_agricola", "hortifruti_ceasa", "pecuaria_leite", "pecuaria_corte"}
    if categoria and categoria not in categorias_publicaveis:
        return False, f"categoria_nao_publicavel:{categoria}"

    if categoria == "hortifruti_ceasa":
        try:
            preco_num = float(preco)
        except Exception:
            return False, "preco_nao_numerico"

        if preco_num <= 0:
            return False, "preco_menor_ou_igual_zero"

        # Faixa ampla para CEASA, pois unidade pode ser kg, caixa, saco, cento ou maço.
        # O objetivo aqui é bloquear erro grosseiro de parser, não validar mercado.
        if preco_num > 5000:
            return False, f"preco_ceasa_acima_da_faixa:{produto_base}:{unidade}:valor_{preco_num}"

        return True, "ok_hortifruti_ceasa"

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


def forcar_referencia_conab(item: dict[str, Any]) -> bool:
    """
    Força a classificação como Referência CONAB somente quando for seguro:
    - fonte CONAB;
    - produto dentro da lista oficial desta etapa;
    - nível ainda não confirmado como atacado, varejo, indicador ou mercado futuro.

    Isso resolve o caso em que a base da CONAB traz registros com nível vazio,
    média UF ou não informado. Esses registros entram como referência oficial,
    sem serem chamados de preço ao produtor.
    """
    fonte = limpar_texto(item.get("fonte"))
    produto_base = limpar_texto(item.get("produto_base"))
    nivel = limpar_texto(item.get("nivel_comercializacao_chave"))

    if "CONAB" not in fonte:
        return False

    if produto_base not in PRODUTOS_CONAB_OFICIAIS:
        return False

    if nivel in {"preco_varejo", "preco_atacado", "indicador_mercado", "mercado_futuro"}:
        return False

    if nivel in {"preco_referencia_conab", "nao_informado", "media_uf", ""}:
        item["nivel_comercializacao"] = "Referência CONAB"
        item["nivel_comercializacao_chave"] = "preco_referencia_conab"
        item["prioridade_nivel_preco"] = NIVEIS_PRECO_PRIORIDADE["preco_referencia_conab"]
        item["produto"] = nome_produto_com_nivel(
            item.get("produto_base", ""),
            item.get("tipo_produto", "Padrão"),
            "Referência CONAB",
        )
        item["produto_slug"] = slugify(item.get("produto", ""))
        item["politica_publicacao"] = "produtor_regional_ou_referencia_conab"
        item["observacao"] = (
            limpar_texto(item.get("observacao"))
            + " Referência CONAB publicada de forma transparente: não é rotulada como preço ao produtor quando a linha não informa esse nível."
        ).strip()
        return True

    return False


def nivel_publicavel_produtor(item: dict[str, Any]) -> tuple[bool, str]:
    """
    Política v1.3.3:
    - Publicar preço pago ao produtor quando a fonte informar claramente.
    - Publicar cotação regional produtiva, como AIBA/SEAGRI-BA.
    - Publicar referência oficial CONAB para soja, milho, algodão, feijão e sorgo
      quando a base não informar atacado nem varejo.
    - Publicar CEPEA/ESALQ para Leite e Boi Gordo como referência de mercado,
      sem tratar como preço local de praça/estado.
    - Não publicar varejo.
    - Não publicar atacado.
    - Não publicar mercado futuro.
    """
    nivel = limpar_texto(item.get("nivel_comercializacao_chave"))
    fonte = limpar_texto(item.get("fonte"))
    produto_base = limpar_texto(item.get("produto_base"))
    categoria = limpar_texto(item.get("categoria"))

    # Bloqueios absolutos: nunca entram na tabela principal.
    if nivel == "preco_varejo":
        return False, "nivel_bloqueado_varejo"

    if nivel == "preco_atacado":
        return False, "nivel_bloqueado_atacado"

    if nivel == "mercado_futuro":
        return False, "nivel_bloqueado_mercado_futuro"

    # CEASA só entra no módulo hortifruti/atacado, separado de leite e boi.
    if nivel == "preco_atacado_ceasa" and categoria == "hortifruti_ceasa":
        return True, "ok_atacado_ceasa_hortifruti"

    # CEPEA/ESALQ para leite e boi gordo entra como referência de mercado,
    # não como preço local. Isso corrige o caso em que o fallback CEPEA é coletado,
    # mas total_cotacoes_leite e total_cotacoes_boi_gordo ficavam zerados.
    if "CEPEA" in fonte or "ESALQ" in fonte:
        if produto_base in {"Leite", "Boi Gordo"} or categoria in {"pecuaria_leite", "pecuaria_corte"}:
            item["nivel_comercializacao"] = "Referência CEPEA/ESALQ"
            item["nivel_comercializacao_chave"] = "referencia_cepea_pecuaria"
            item["prioridade_nivel_preco"] = NIVEIS_PRECO_PRIORIDADE.get("indicador_mercado", 7)
            item["politica_publicacao"] = "referencia_cepea_pecuaria"
            item["observacao"] = (
                limpar_texto(item.get("observacao"))
                + " Referência CEPEA/ESALQ publicada de forma transparente: não é preço local da praça selecionada."
            ).strip()
            return True, "ok_referencia_cepea_pecuaria"

        return False, "nivel_bloqueado_indicador_mercado"

    # Indicadores de mercado genéricos continuam bloqueados.
    if nivel == "indicador_mercado":
        return False, "nivel_bloqueado_indicador_mercado"

    if nivel == "preco_produtor":
        return True, "ok_preco_produtor"

    # AIBA é a principal referência regional de praça produtora do Oeste Baiano/MATOPIBA.
    # Mantemos como regional produtiva, pois não é varejo nem atacado.
    if nivel == "preco_regional" and "AIBA" in fonte:
        return True, "ok_preco_regional_aiba"

    if nivel == "preco_regional":
        return True, "ok_preco_regional"

    # CONAB sem nível claro, média UF ou não informado entra como Referência CONAB,
    # desde que não seja atacado/varejo/indicador.
    if forcar_referencia_conab(item):
        return True, "ok_referencia_oficial_conab_forcada"

    if nivel == "preco_referencia_conab":
        if "CONAB" in fonte and produto_base in PRODUTOS_CONAB_OFICIAIS:
            return True, "ok_referencia_oficial_conab"
        return False, "nivel_conab_referencia_fonte_invalida"

    if nivel == "media_uf":
        return False, "nivel_bloqueado_media_uf"

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
    data_referencia_inicio: Optional[str] = None,
    data_referencia_fim: Optional[str] = None,
    periodo_referencia: Optional[str] = None,
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

    data_inicio_iso = parse_data(data_referencia_inicio or data_referencia)
    data_fim_iso = parse_data(data_referencia_fim or data_referencia)
    data_operacional_iso = data_fim_iso or data_inicio_iso
    periodo_referencia_limpo = limpar_texto(periodo_referencia)

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
        "data_referencia": data_operacional_iso,
        "data_referencia_inicio": data_inicio_iso,
        "data_referencia_fim": data_fim_iso,
        "periodo_referencia": periodo_referencia_limpo,
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
        "data": item.get("periodo_referencia") or data_para_br(item.get("data_referencia")),
        "data_iso": item.get("data_referencia") or "",
        "data_inicio_iso": item.get("data_referencia_inicio") or item.get("data_referencia") or "",
        "data_fim_iso": item.get("data_referencia_fim") or item.get("data_referencia") or "",
        "periodo_referencia": item.get("periodo_referencia") or "",
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

    if produto_base in PRODUTOS_CONAB_PECUARIA_PRECO_RECEBIDO and tipo_fonte_conab in {"semanal_uf", "semanal_municipio"}:
        # Regra v1.3.4:
        # Os arquivos da CONAB Preços Agropecuários podem não trazer uma coluna explícita
        # com o texto do filtro visual "Preço Recebido". Para Leite e Boi Gordo,
        # quando a linha não indicar atacado, varejo ou consumidor final, tratamos como
        # preço recebido pelo produtor, conforme definição operacional do Nordeste Agro.
        return "preço recebido pelo produtor conab"

    if produto_base in PRODUTOS_CONAB_OFICIAIS and tipo_fonte_conab == "semanal_municipio":
        # Regra v1.3.0: quando a base semanal por município da CONAB não informa
        # explicitamente atacado, varejo ou produtor, ela entra como referência
        # oficial CONAB por praça. Não rotulamos como "Produtor" para não criar
        # uma informação que a própria linha não informou.
        return "referencia conab municipal"

    if produto_base in PRODUTOS_CONAB_OFICIAIS and tipo_fonte_conab == "semanal_uf":
        # Regra v1.3.0: quando a base semanal por UF da CONAB não informa nível
        # de comercialização, ela entra como referência oficial estadual CONAB.
        # Atacado e varejo continuam bloqueados acima.
        return "referencia conab estadual"

    return texto





def limitar_texto_debug(valor: Any, limite: int = 1200) -> str:
    texto = limpar_texto(valor)
    if len(texto) <= limite:
        return texto
    return texto[:limite] + "... [cortado]"


def normalizar_parametros_query(valor: str) -> dict[str, Any]:
    """Transforma querystring ou post_data do Pentaho/CDA em dicionário resumido."""
    if not valor:
        return {}

    texto = valor
    if "?" in texto and texto.startswith("http"):
        texto = urlparse(texto).query

    try:
        params = parse_qs(texto, keep_blank_values=True)
    except Exception:
        return {"_bruto": limitar_texto_debug(texto, 2000)}

    saida: dict[str, Any] = {}
    for chave, valores in params.items():
        if not valores:
            saida[chave] = ""
        elif len(valores) == 1:
            saida[chave] = limitar_texto_debug(valores[0], 500)
        else:
            saida[chave] = [limitar_texto_debug(v, 300) for v in valores[:10]]
    return saida


def resumir_json_cda(texto: str) -> dict[str, Any]:
    """
    Resume as respostas JSON do Pentaho/CDA para descobrir qual chamada contém preço.
    Não altera a coleta principal; apenas gera diagnóstico no arquivo de debug.
    """
    resumo: dict[str, Any] = {
        "json_valido": False,
        "metadata_colunas": [],
        "total_linhas_resultset": 0,
        "preview_resultset": [],
        "tem_palavra_preco": False,
        "tem_palavra_produtor": False,
        "tem_uf": False,
        "tem_produto": False,
        "tem_data": False,
        "texto_colunas": "",
    }

    try:
        obj = json.loads(texto)
    except Exception as erro:
        resumo["erro_json"] = str(erro)
        resumo["preview_texto"] = limitar_texto_debug(texto, 1200)
        return resumo

    resumo["json_valido"] = True

    metadata = obj.get("metadata") or obj.get("metaData") or obj.get("columns") or []
    resultset = obj.get("resultset") or obj.get("resultSet") or obj.get("data") or []

    colunas = []
    if isinstance(metadata, list):
        for i, meta in enumerate(metadata):
            if isinstance(meta, dict):
                colunas.append(limpar_texto(meta.get("colName") or meta.get("name") or meta.get("label") or meta.get("id") or f"coluna_{i}"))
            else:
                colunas.append(limpar_texto(meta) or f"coluna_{i}")

    resumo["metadata_colunas"] = colunas
    texto_colunas = remover_acentos(" | ".join(colunas)).lower()
    resumo["texto_colunas"] = texto_colunas

    if isinstance(resultset, list):
        resumo["total_linhas_resultset"] = len(resultset)
        resumo["preview_resultset"] = resultset[:10]

    texto_busca = remover_acentos((texto_colunas + " " + limitar_texto_debug(texto, 5000))).lower()
    resumo["tem_palavra_preco"] = any(p in texto_busca for p in ["preco", "pre_o", "valor", "vlr", "cotacao"])
    resumo["tem_palavra_produtor"] = any(p in texto_busca for p in ["produtor", "recebido", "pago"])
    resumo["tem_uf"] = any(p in texto_busca for p in ["uf", "estado", "unidade federativa"])
    resumo["tem_produto"] = any(p in texto_busca for p in ["produto", "classificacao", "cultura"])
    resumo["tem_data"] = any(p in texto_busca for p in ["data", "mes", "ano", "semana", "periodo"])

    return resumo


def salvar_debug_conab_360(debug: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_CONAB_360.write_text(
        json.dumps(debug, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def salvar_debug_sorgo_conab(debug: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_SORGO_CONAB.write_text(
        json.dumps(debug, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def salvar_debug_leite_carne_conab(debug: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_LEITE_CARNE_CONAB.write_text(
        json.dumps(debug, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def extrair_registros_json_generico(obj: Any) -> list[dict[str, Any]]:
    """
    Extrai linhas de respostas JSON comuns em painéis Pentaho/CDA.
    Suporta formatos como:
    - {"metadata": [...], "resultset": [[...], [...]]}
    - lista de dicionários
    - dicionários aninhados
    """
    registros: list[dict[str, Any]] = []

    if isinstance(obj, dict):
        metadata = obj.get("metadata") or obj.get("metaData") or obj.get("columns")
        resultset = obj.get("resultset") or obj.get("resultSet") or obj.get("data")

        if isinstance(metadata, list) and isinstance(resultset, list):
            colunas = []
            for i, meta in enumerate(metadata):
                if isinstance(meta, dict):
                    colunas.append(
                        limpar_texto(
                            meta.get("colName")
                            or meta.get("name")
                            or meta.get("label")
                            or meta.get("id")
                            or f"coluna_{i}"
                        )
                    )
                else:
                    colunas.append(limpar_texto(meta) or f"coluna_{i}")

            for linha in resultset:
                if isinstance(linha, dict):
                    registros.append(linha)
                elif isinstance(linha, list):
                    registros.append({colunas[i] if i < len(colunas) else f"coluna_{i}": valor for i, valor in enumerate(linha)})

        for valor in obj.values():
            if isinstance(valor, (dict, list)):
                registros.extend(extrair_registros_json_generico(valor))

    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                registros.append(item)
                registros.extend(extrair_registros_json_generico(item))
            elif isinstance(item, list):
                registros.extend(extrair_registros_json_generico(item))

    return registros


def valor_por_termos(linha: dict[str, Any], termos: list[str]) -> Any:
    for chave, valor in linha.items():
        chave_norm = normalizar_coluna(chave)
        for termo in termos:
            if normalizar_coluna(termo) in chave_norm:
                return valor
    return None


def texto_linha_generico(linha: dict[str, Any]) -> str:
    return " | ".join(f"{k}: {v}" for k, v in linha.items() if limpar_texto(v))


def linha_para_item_conab_360(linha: dict[str, Any], origem_url: str) -> Optional[dict[str, Any]]:
    texto_linha = texto_linha_generico(linha)
    texto_norm = remover_acentos(texto_linha).lower()

    if any(bloqueado in texto_norm for bloqueado in ["atacado", "varejo", "consumidor", "ceasa", "prohort"]):
        return None

    produto_original = valor_por_termos(linha, ["produto", "classificacao", "classificação", "cultura"])
    if not produto_original:
        produto_original = texto_linha

    produto_base = normalizar_produto_base(produto_original)

    if produto_base not in PRODUTOS_CONAB_360:
        return None

    uf = limpar_texto(valor_por_termos(linha, ["uf", "sigla_uf", "estado"]) or "").upper()
    estado_nome = ""

    if uf not in UFS_NORDESTE:
        # tenta identificar pelo nome do estado dentro da linha
        for sigla, nome_estado in UFS_NORDESTE.items():
            if remover_acentos(nome_estado).lower() in texto_norm:
                uf = sigla
                estado_nome = nome_estado
                break

    if uf not in UFS_NORDESTE:
        return None

    if not estado_nome:
        estado_nome = UFS_NORDESTE[uf]

    praca = limpar_texto(
        valor_por_termos(linha, ["municipio", "município", "cidade", "praca", "praça", "localidade"])
        or estado_nome
    )

    data_ref = parse_data(
        valor_por_termos(linha, ["data", "dt", "referencia", "referência", "semana", "periodo", "período"])
        or texto_linha
    )

    preco_raw = valor_por_termos(
        linha,
        [
            "preco recebido",
            "preço recebido",
            "preco_produtor",
            "preço_produtor",
            "produtor",
            "valor",
            "preco",
            "preço",
            "vlr",
        ],
    )

    preco = parse_preco(preco_raw)

    if preco is None:
        # fallback: procura um valor monetário no texto inteiro
        candidatos = re.findall(r"\b\d{1,4}(?:\.\d{3})*,\d{2,4}\b|\b\d{1,4}\.\d{2,4}\b", texto_linha)
        precos = [parse_preco(c) for c in candidatos]
        precos = [p for p in precos if p is not None and p > 0]
        if not precos:
            return None
        preco = max(precos)

    unidade = limpar_texto(valor_por_termos(linha, ["unidade", "unid", "medida"]) or "")

    return criar_item(
        produto_original=produto_base,
        uf=uf,
        estado_nome=estado_nome,
        praca=praca,
        unidade=unidade,
        preco=preco,
        variacao_percentual=None,
        data_referencia=data_ref,
        fonte="CONAB - Produtos 360º",
        fonte_url=CONAB_PRODUTOS_360_URL,
        tipo_fonte="oficial",
        nivel_comercializacao="referencia conab 360",
        observacao=(
            "Referência oficial CONAB extraída do painel Produtos 360º. "
            "Usada para Soja, Milho e Algodão por apresentar a referência mais atualizada do painel. "
            "Não classificada como preço pago ao produtor quando a linha não trouxer essa informação explicitamente."
        ),
    )


def extrair_itens_conab_360_de_texto(texto: str, origem_url: str) -> list[dict[str, Any]]:
    itens: list[dict[str, Any]] = []

    if not texto:
        return itens

    # tenta JSON puro primeiro
    try:
        obj = json.loads(texto)
        registros = extrair_registros_json_generico(obj)
        for reg in registros:
            item = linha_para_item_conab_360(reg, origem_url)
            if item:
                itens.append(item)
    except Exception:
        pass

    # fallback HTML/tabelas
    try:
        soup = BeautifulSoup(texto, "html.parser")
        for tr in soup.find_all("tr"):
            celulas = [limpar_texto(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            celulas = [c for c in celulas if c]
            if len(celulas) < 3:
                continue
            linha = {f"coluna_{i}": valor for i, valor in enumerate(celulas)}
            item = linha_para_item_conab_360(linha, origem_url)
            if item:
                itens.append(item)
    except Exception:
        pass

    # remove duplicados
    unicos: dict[tuple[str, str, str, str, float], dict[str, Any]] = {}
    for item in itens:
        chave = (
            item.get("produto_base", ""),
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("data_referencia", ""),
            float(item.get("preco", 0)),
        )
        unicos[chave] = item

    return list(unicos.values())


async def coletar_conab_360_playwright_async() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    itens: list[dict[str, Any]] = []
    respostas_debug: list[dict[str, Any]] = []
    doquery_debug: list[dict[str, Any]] = []

    try:
        from playwright.async_api import async_playwright
    except Exception as erro:
        return [], [{"status": "playwright_indisponivel", "erro": str(erro)}], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1400},
            locale="pt-BR",
            timezone_id=TIMEZONE,
            user_agent=HEADERS["User-Agent"],
        )
        page = await context.new_page()

        async def capturar_resposta(response):
            try:
                url = response.url
                url_norm = remover_acentos(url).lower()
                content_type = response.headers.get("content-type", "")
                request = response.request
                metodo = request.method
                post_data = request.post_data or ""

                if not any(chave in url_norm for chave in ["cda", "doquery", "produto", "produtos", "preco", "precos", "360", "pentaho"]):
                    return

                texto = ""
                if any(tipo in content_type.lower() for tipo in ["json", "text", "html", "javascript", "xml"]):
                    texto = await response.text()
                else:
                    return

                novos = extrair_itens_conab_360_de_texto(texto, url)
                itens.extend(novos)

                item_debug = {
                    "url": url,
                    "metodo_http": metodo,
                    "status": response.status,
                    "content_type": content_type,
                    "itens_extraidos": len(novos),
                    "parametros_url": normalizar_parametros_query(url),
                    "parametros_post": normalizar_parametros_query(post_data),
                    "post_data_preview": limitar_texto_debug(post_data, 1200),
                    "preview": texto[:800],
                }
                respostas_debug.append(item_debug)

                if "doquery" in url_norm or "plugin/cda/api" in url_norm:
                    resumo_cda = resumir_json_cda(texto)
                    doquery_debug.append(
                        {
                            "url": url,
                            "metodo_http": metodo,
                            "status": response.status,
                            "content_type": content_type,
                            "parametros_url": item_debug["parametros_url"],
                            "parametros_post": item_debug["parametros_post"],
                            "post_data_preview": item_debug["post_data_preview"],
                            "itens_extraidos_pelo_parser_atual": len(novos),
                            "resumo_cda": resumo_cda,
                        }
                    )
            except Exception as erro:
                respostas_debug.append({"erro": str(erro)})

        page.on("response", lambda response: asyncio.create_task(capturar_resposta(response)))

        await page.goto(CONAB_PRODUTOS_360_PENTAHO_URL, wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_timeout(15000)

        # Clica nos produtos principais para forçar o painel a carregar as consultas internas.
        for nome_produto in ["Milho", "Soja", "Algodão em Pluma", "Algodão"]:
            try:
                await page.get_by_text(nome_produto, exact=False).first.click(timeout=8000)
                await page.wait_for_timeout(10000)
            except Exception as erro:
                respostas_debug.append({"acao": f"clicar {nome_produto}", "erro": str(erro)})

        # Tenta clicar em possíveis abas/legendas relacionadas a preço recebido pelo produtor.
        for termo in ["Preço", "Preços", "Recebido", "Produtor", "Preço Recebido", "Preço Mínimo"]:
            try:
                await page.get_by_text(termo, exact=False).first.click(timeout=5000)
                await page.wait_for_timeout(7000)
            except Exception as erro:
                respostas_debug.append({"acao": f"clicar termo {termo}", "erro": str(erro)})

        try:
            texto_dom = await page.locator("body").inner_text(timeout=15000)
            itens.extend(extrair_itens_conab_360_de_texto(texto_dom, CONAB_PRODUTOS_360_PENTAHO_URL))
            respostas_debug.append({"acao": "ler_dom", "preview_dom": texto_dom[:3000]})
        except Exception as erro:
            respostas_debug.append({"acao": "ler_dom", "erro": str(erro)})

        await context.close()
        await browser.close()

    unicos: dict[tuple[str, str, str, str, float], dict[str, Any]] = {}
    for item in itens:
        chave = (
            item.get("produto_base", ""),
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("data_referencia", ""),
            float(item.get("preco", 0)),
        )
        unicos[chave] = item

    return list(unicos.values()), respostas_debug, doquery_debug


def parametros_preco_produto_conab_360(produto_base: str) -> dict[str, str]:
    """
    Parâmetros CDA confirmados no debug do Produtos 360º.

    dataAccessId=precoProduto retorna preço médio recente por UF.
    Esse é o dado que alimenta a tela do Produtos 360º por estado,
    com colunas como Regionalizacao, UltimoPrecoMedio e DescricaoSemanal.
    """
    mapa_paramproduto = {
        "Soja": "[Produto].[SOJA]",
        "Milho": "[Produto].[MILHO]",
        "Algodão": "[Produto].[ALGODÃO EM PLUMA]",
    }

    return {
        "paramprodutoPreco": mapa_paramproduto[produto_base],
        "path": "/home/Produtos/produtos360.cda",
        "dataAccessId": "precoProduto",
        "outputIndexId": "1",
        "pageSize": "0",
        "pageStart": "0",
        "paramsearchBox": "",
    }


def parametros_historico_produto_conab_360(produto_base: str) -> dict[str, str]:
    """
    Consulta complementar nacional com coluna explícita
    'Preco Recebido Produtor'. Usada apenas no debug/validação,
    porque não retorna UF.
    """
    mapa_paramproduto = {
        "Soja": "[Produto].[SOJA]",
        "Milho": "[Produto].[MILHO]",
        "Algodão": "[Produto].[ALGODÃO EM PLUMA]",
    }

    return {
        "paramprodutoPreco": mapa_paramproduto[produto_base],
        "path": "/home/Produtos/produtos360.cda",
        "dataAccessId": "ultimaSemanaPrecoProduto_new",
        "outputIndexId": "1",
        "pageSize": "0",
        "pageStart": "0",
        "sortBy": "",
        "paramsearchBox": "",
    }


def extrair_metadata_resultset_cda(obj: dict[str, Any]) -> tuple[list[str], list[Any]]:
    metadata = obj.get("metadata") or obj.get("metaData") or []
    resultset = obj.get("resultset") or obj.get("resultSet") or []

    colunas: list[str] = []
    if isinstance(metadata, list):
        for i, meta in enumerate(metadata):
            if isinstance(meta, dict):
                colunas.append(
                    limpar_texto(
                        meta.get("colName")
                        or meta.get("name")
                        or meta.get("label")
                        or meta.get("id")
                        or f"coluna_{i}"
                    )
                )
            else:
                colunas.append(limpar_texto(meta) or f"coluna_{i}")

    if not isinstance(resultset, list):
        resultset = []

    return colunas, resultset


def linha_cda_para_dict(colunas: list[str], linha: Any) -> dict[str, Any]:
    if isinstance(linha, dict):
        return linha

    if isinstance(linha, list):
        return {
            colunas[i] if i < len(colunas) else f"coluna_{i}": valor
            for i, valor in enumerate(linha)
        }

    return {}


def valor_coluna_exata_ou_termos(linha: dict[str, Any], nomes_exatos: list[str], termos: list[str]) -> Any:
    for nome in nomes_exatos:
        if nome in linha:
            return linha.get(nome)

    return valor_por_termos(linha, termos)


def item_conab_360_preco_produto(produto_base: str, linha: dict[str, Any]) -> Optional[dict[str, Any]]:
    uf = limpar_texto(
        valor_coluna_exata_ou_termos(
            linha,
            ["Regionalizacao.Regionalizacao", "UF.UF", "UF", "Estado"],
            ["regionalizacao", "regionalização", "uf", "estado"],
        )
    ).upper()

    if uf not in UFS_NORDESTE:
        return None

    preco_raw = valor_coluna_exata_ou_termos(
        linha,
        ["UltimoPrecoMedio", "Último Preço Médio", "Preco Recebido Produtor", "Preço Recebido Produtor"],
        ["ultimoprecomedio", "ultimo preco medio", "preco recebido produtor", "preço recebido produtor", "preco", "preço"],
    )
    preco = parse_preco(preco_raw)

    if preco is None or preco <= 0:
        return None

    periodo_raw = valor_coluna_exata_ou_termos(
        linha,
        ["DescricaoSemanal", "Descrição Semanal", "Semana"],
        ["descricao semanal", "descrição semanal", "semana", "periodo", "período", "data"],
    )
    periodo = extrair_periodo_semanal(periodo_raw)
    data_ref = periodo["data_fim"]

    if produto_base == "Algodão":
        produto_original = "Algodão em Pluma"
        unidade = "Arroba (@)"
    else:
        produto_original = produto_base
        unidade = "Saca 60 kg"

    return criar_item(
        produto_original=produto_original,
        uf=uf,
        estado_nome=UFS_NORDESTE[uf],
        praca="Referência UF CONAB",
        unidade=unidade,
        preco=preco,
        variacao_percentual=parse_preco(
            valor_coluna_exata_ou_termos(
                linha,
                ["DifPercentualPrecoMedio"],
                ["difpercentualprecomedio", "variacao", "variação"],
            )
        ),
        data_referencia=data_ref,
        data_referencia_inicio=periodo["data_inicio"],
        data_referencia_fim=periodo["data_fim"],
        periodo_referencia=periodo["periodo_referencia"],
        fonte="CONAB - Produtos 360º",
        fonte_url=CONAB_PRODUTOS_360_URL,
        tipo_fonte="oficial",
        nivel_comercializacao="referencia conab 360",
        observacao=(
            "Referência oficial CONAB extraída diretamente do painel Produtos 360º. "
            "Consulta operacional: produtos360.cda / dataAccessId=precoProduto. "
            "Usada para Soja, Milho e Algodão por trazer preço médio semanal recente por UF. "
            "A data exibida no site representa o período semanal publicado no painel, não preço diário. "
            "Não é rotulada como preço pago ao produtor quando a linha não trouxer essa informação explicitamente."
        ),
    )


async def coletar_conab_360_cda_playwright_autenticado_async() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Fallback autenticado via Playwright para o CONAB Produtos 360º.

    Motivo:
    - Chamadas diretas ao doQuery podem retornar 401 quando não há sessão Pentaho.
    - Esta função abre o generatedContent com userid/password, herda a sessão no navegador
      e executa as chamadas CDA via fetch com credentials=include.
    """
    cotacoes: list[dict[str, Any]] = []
    debug_respostas: list[dict[str, Any]] = []
    debug_doquery: list[dict[str, Any]] = []

    try:
        from playwright.async_api import async_playwright
    except Exception as erro:
        return [], [{"status": "playwright_indisponivel", "erro": str(erro)}], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1400},
            locale="pt-BR",
            timezone_id=TIMEZONE,
            user_agent=HEADERS["User-Agent"],
        )
        page = await context.new_page()

        try:
            await page.goto(CONAB_PRODUTOS_360_PENTAHO_URL, wait_until="domcontentloaded", timeout=120000)
            await page.wait_for_timeout(8000)
        except Exception as erro:
            debug_respostas.append(
                {
                    "fonte": "CONAB - Produtos 360º",
                    "metodo": "playwright_login_generatedContent",
                    "status": "erro_ao_abrir_painel",
                    "erro": str(erro),
                }
            )

        async def post_cda_com_sessao(parametros: dict[str, str]) -> dict[str, Any]:
            return await page.evaluate(
                """
                async ({url, params}) => {
                  const body = new URLSearchParams(params);
                  const resp = await fetch(url, {
                    method: 'POST',
                    credentials: 'include',
                    headers: {
                      'Accept': 'application/json, text/javascript, */*; q=0.01',
                      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                      'X-Requested-With': 'XMLHttpRequest'
                    },
                    body
                  });
                  return {
                    status: resp.status,
                    content_type: resp.headers.get('content-type') || '',
                    texto: await resp.text()
                  };
                }
                """,
                {"url": CONAB_360_DOQUERY_URL, "params": parametros},
            )

        for produto_base in ["Soja", "Milho", "Algodão"]:
            parametros = parametros_preco_produto_conab_360(produto_base)

            try:
                resultado = await post_cda_com_sessao(parametros)
                texto = resultado.get("texto", "")
                status_http = resultado.get("status")
                content_type = resultado.get("content_type", "")

                item_debug = {
                    "fonte": "CONAB - Produtos 360º",
                    "produto_base": produto_base,
                    "url": CONAB_360_DOQUERY_URL,
                    "metodo_http": "POST",
                    "metodo_autenticacao": "playwright_fetch_credentials_include",
                    "status": status_http,
                    "content_type": content_type,
                    "parametros_post": parametros,
                    "preview": texto[:1600],
                }

                try:
                    obj = json.loads(texto)
                    colunas, resultset = extrair_metadata_resultset_cda(obj)
                    itens_produto: list[dict[str, Any]] = []

                    for linha_bruta in resultset:
                        linha = linha_cda_para_dict(colunas, linha_bruta)
                        item = item_conab_360_preco_produto(produto_base, linha)
                        if item:
                            itens_produto.append(item)

                    cotacoes.extend(itens_produto)
                    item_debug["metadata_colunas"] = colunas
                    item_debug["total_linhas_resultset"] = len(resultset)
                    item_debug["itens_extraidos"] = len(itens_produto)
                    item_debug["preview_resultset"] = resultset[:10]

                    debug_doquery.append(
                        {
                            "url": CONAB_360_DOQUERY_URL,
                            "metodo_http": "POST",
                            "metodo_autenticacao": "playwright_fetch_credentials_include",
                            "status": status_http,
                            "content_type": content_type,
                            "parametros_post": parametros,
                            "itens_extraidos_pelo_parser_atual": len(itens_produto),
                            "resumo_cda": resumir_json_cda(texto),
                        }
                    )

                except Exception as erro_json:
                    item_debug["erro_json"] = str(erro_json)
                    debug_doquery.append(
                        {
                            "url": CONAB_360_DOQUERY_URL,
                            "metodo_http": "POST",
                            "metodo_autenticacao": "playwright_fetch_credentials_include",
                            "status": status_http,
                            "content_type": content_type,
                            "parametros_post": parametros,
                            "itens_extraidos_pelo_parser_atual": 0,
                            "resumo_cda": resumir_json_cda(texto),
                        }
                    )

                debug_respostas.append(item_debug)

            except Exception as erro:
                debug_respostas.append(
                    {
                        "fonte": "CONAB - Produtos 360º",
                        "produto_base": produto_base,
                        "url": CONAB_360_DOQUERY_URL,
                        "metodo_http": "POST",
                        "metodo_autenticacao": "playwright_fetch_credentials_include",
                        "parametros_post": parametros,
                        "erro": str(erro),
                    }
                )

        await context.close()
        await browser.close()

    unicos: dict[tuple[str, str, str, str, float], dict[str, Any]] = {}
    for item in cotacoes:
        chave = (
            item.get("produto_base", ""),
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("data_referencia", ""),
            float(item.get("preco", 0)),
        )
        unicos[chave] = item

    return list(unicos.values()), debug_respostas, debug_doquery


def coletar_conab_360_direto_cda() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Coleta do CONAB Produtos 360º.

    v1.3.0:
    - primeiro abre o generatedContent para criar sessão Pentaho;
    - tenta POST direto com cookies da sessão;
    - se o endpoint responder 401 ou não retornar dados, usa fallback Playwright
      executando fetch dentro da página autenticada.
    """
    cotacoes: list[dict[str, Any]] = []
    debug_respostas: list[dict[str, Any]] = []
    debug_doquery: list[dict[str, Any]] = []

    sessao = requests.Session()
    headers = dict(HEADERS)
    headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://pentahoportaldeinformacoes.conab.gov.br",
            "Referer": CONAB_PRODUTOS_360_PENTAHO_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
    )

    try:
        auth = sessao.get(CONAB_PRODUTOS_360_PENTAHO_URL, headers=HEADERS, timeout=120)
        debug_respostas.append(
            {
                "fonte": "CONAB - Produtos 360º",
                "metodo": "requests_auth_generatedContent",
                "status": auth.status_code,
                "content_type": auth.headers.get("content-type", ""),
                "cookies": sorted(sessao.cookies.get_dict().keys()),
                "preview": auth.text[:600],
            }
        )
    except Exception as erro:
        debug_respostas.append(
            {
                "fonte": "CONAB - Produtos 360º",
                "metodo": "requests_auth_generatedContent",
                "status": "erro",
                "erro": str(erro),
            }
        )

    status_direct: list[int] = []

    for produto_base in ["Soja", "Milho", "Algodão"]:
        parametros = parametros_preco_produto_conab_360(produto_base)

        try:
            resposta = sessao.post(
                CONAB_360_DOQUERY_URL,
                headers=headers,
                data=parametros,
                timeout=90,
            )
            texto = resposta.text
            status_direct.append(resposta.status_code)

            item_debug = {
                "fonte": "CONAB - Produtos 360º",
                "produto_base": produto_base,
                "url": CONAB_360_DOQUERY_URL,
                "metodo_http": "POST",
                "metodo_autenticacao": "requests_session_after_generatedContent",
                "status": resposta.status_code,
                "content_type": resposta.headers.get("content-type", ""),
                "parametros_post": parametros,
                "preview": texto[:1600],
            }

            try:
                obj = resposta.json()
                colunas, resultset = extrair_metadata_resultset_cda(obj)
                itens_produto: list[dict[str, Any]] = []

                for linha_bruta in resultset:
                    linha = linha_cda_para_dict(colunas, linha_bruta)
                    item = item_conab_360_preco_produto(produto_base, linha)
                    if item:
                        itens_produto.append(item)

                cotacoes.extend(itens_produto)
                item_debug["metadata_colunas"] = colunas
                item_debug["total_linhas_resultset"] = len(resultset)
                item_debug["itens_extraidos"] = len(itens_produto)
                item_debug["preview_resultset"] = resultset[:10]

                debug_doquery.append(
                    {
                        "url": CONAB_360_DOQUERY_URL,
                        "metodo_http": "POST",
                        "metodo_autenticacao": "requests_session_after_generatedContent",
                        "status": resposta.status_code,
                        "content_type": resposta.headers.get("content-type", ""),
                        "parametros_post": parametros,
                        "itens_extraidos_pelo_parser_atual": len(itens_produto),
                        "resumo_cda": resumir_json_cda(texto),
                    }
                )

            except Exception as erro_json:
                item_debug["erro_json"] = str(erro_json)
                debug_doquery.append(
                    {
                        "url": CONAB_360_DOQUERY_URL,
                        "metodo_http": "POST",
                        "metodo_autenticacao": "requests_session_after_generatedContent",
                        "status": resposta.status_code,
                        "content_type": resposta.headers.get("content-type", ""),
                        "parametros_post": parametros,
                        "itens_extraidos_pelo_parser_atual": 0,
                        "resumo_cda": resumir_json_cda(texto),
                    }
                )

            debug_respostas.append(item_debug)

        except Exception as erro:
            debug_respostas.append(
                {
                    "fonte": "CONAB - Produtos 360º",
                    "produto_base": produto_base,
                    "url": CONAB_360_DOQUERY_URL,
                    "metodo_http": "POST",
                    "metodo_autenticacao": "requests_session_after_generatedContent",
                    "parametros_post": parametros,
                    "erro": str(erro),
                }
            )

    precisa_fallback = not cotacoes or any(status == 401 for status in status_direct)

    if precisa_fallback:
        try:
            novos_pw, debug_pw, debug_cda_pw = asyncio.run(coletar_conab_360_cda_playwright_autenticado_async())
            debug_respostas.append(
                {
                    "fonte": "CONAB - Produtos 360º",
                    "metodo": "fallback_playwright_autenticado",
                    "total_registros": len(novos_pw),
                }
            )
            cotacoes.extend(novos_pw)
            debug_respostas.extend(debug_pw)
            debug_doquery.extend(debug_cda_pw)
        except Exception as erro:
            debug_respostas.append(
                {
                    "fonte": "CONAB - Produtos 360º",
                    "metodo": "fallback_playwright_autenticado",
                    "status": "erro",
                    "erro": str(erro),
                }
            )

    unicos: dict[tuple[str, str, str, str, float], dict[str, Any]] = {}
    for item in cotacoes:
        chave = (
            item.get("produto_base", ""),
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("data_referencia", ""),
            float(item.get("preco", 0)),
        )
        unicos[chave] = item

    return list(unicos.values()), debug_respostas, debug_doquery

def coletar_conab_produtos_360(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Coleta preferencial para Soja, Milho e Algodão no painel CONAB Produtos 360º.

    v1.3.0:
    - usa chamada direta ao CDA identificada no debug;
    - publica dados por UF do Nordeste;
    - mantém debug para conferência.
    """
    cotacoes_finais: list[dict[str, Any]] = []
    debug_respostas: list[dict[str, Any]] = []
    debug_doquery: list[dict[str, Any]] = []

    try:
        novos, debug_direto, debug_cda = coletar_conab_360_direto_cda()
        cotacoes_finais.extend(novos)
        debug_respostas.extend(debug_direto)
        debug_doquery.extend(debug_cda)
    except Exception as erro:
        debug_respostas.append({"metodo": "cda_direto", "erro": str(erro)})

    chamadas_promissoras = []
    for item in debug_doquery:
        resumo = item.get("resumo_cda", {})
        score = 0
        if resumo.get("tem_palavra_preco"):
            score += 3
        if resumo.get("tem_palavra_produtor"):
            score += 3
        if resumo.get("tem_uf"):
            score += 1
        if resumo.get("tem_produto"):
            score += 1
        if resumo.get("tem_data"):
            score += 1
        if resumo.get("total_linhas_resultset", 0) > 0:
            score += 1
        if item.get("itens_extraidos_pelo_parser_atual", 0) > 0:
            score += 5
        if score > 0:
            chamadas_promissoras.append(
                {
                    "score": score,
                    "status": item.get("status"),
                    "metodo_http": item.get("metodo_http"),
                    "parametros_post": item.get("parametros_post"),
                    "itens_extraidos_pelo_parser_atual": item.get("itens_extraidos_pelo_parser_atual", 0),
                    "resumo_cda": item.get("resumo_cda"),
                }
            )

    chamadas_promissoras.sort(key=lambda x: x.get("score", 0), reverse=True)

    debug_payload = {
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes",
        "versao": "1.3.4",
        "gerado_em": agora_local().isoformat(),
        "objetivo": (
            "Coletar diretamente do CONAB Produtos 360º/Pentaho a consulta CDA "
            "dataAccessId=precoProduto para Soja, Milho e Algodão."
        ),
        "total_itens_extraidos_pelo_parser_atual": len(cotacoes_finais),
        "total_respostas_debug": len(debug_respostas),
        "total_chamadas_doquery": len(debug_doquery),
        "chamadas_promissoras": chamadas_promissoras[:50],
        "todas_chamadas_doquery": debug_doquery,
        "respostas_debug_resumidas": debug_respostas[:120],
        "amostra_cotacoes_extraidas": cotacoes_finais[:50],
    }
    salvar_debug_conab_360(debug_payload)

    produtos_extraidos = sorted({item.get("produto_base") for item in cotacoes_finais if item.get("produto_base")})
    ufs_extraidas = sorted({item.get("uf") for item in cotacoes_finais if item.get("uf")})

    status_fontes.append(
        {
            "fonte": "CONAB - Produtos 360º",
            "url": CONAB_PRODUTOS_360_URL,
            "status": "ok" if cotacoes_finais else "sem_registros_extraidos",
            "total_registros": len(cotacoes_finais),
            "produtos_conab_360": sorted(PRODUTOS_CONAB_360),
            "produtos_extraidos": produtos_extraidos,
            "ufs_extraidas": ufs_extraidas,
            "observacao": (
                "Fonte preferencial para Soja, Milho e Algodão. v1.3.0 usa chamada direta "
                "ao Pentaho/CDA: produtos360.cda / dataAccessId=precoProduto."
            ),
            "arquivo_debug_conab_360": str(OUTPUT_DEBUG_CONAB_360),
            "total_chamadas_doquery": len(debug_doquery),
            "chamadas_promissoras_preview": chamadas_promissoras[:10],
            "debug_respostas": debug_respostas[:30],
        }
    )

    return cotacoes_finais


def produto_seagri_ba_interessa(produto: Any) -> bool:
    produto_base = normalizar_produto_base(produto)
    return produto_base in {"Feijão", "Sorgo", "Leite", "Boi Gordo"}


def montar_url_debug_seagri_ba(params: dict[str, Any]) -> str:
    pares = []
    for chave, valor in params.items():
        pares.append(f"{chave}={valor}")
    return SEAGRI_BA_COTACOES_URL + "?" + "&".join(pares)


def extrair_linhas_seagri_ba_html(html: str) -> list[dict[str, str]]:
    """
    Extrai linhas da página pública de cotações da SEAGRI-BA.

    Estrutura esperada da tabela:
    Data | Produto | Praça | Tipo | Unidade | Preço

    A função mantém fallback por texto porque o portal pode alterar pequenas
    marcações HTML sem alterar o conteúdo visível.
    """
    linhas_extraidas: list[dict[str, str]] = []
    soup = BeautifulSoup(html, "html.parser")

    # Caminho principal: tabela HTML.
    for tr in soup.find_all("tr"):
        celulas = [limpar_texto(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        celulas = [c for c in celulas if c]

        if len(celulas) < 6:
            continue

        data = celulas[0]
        if not re.search(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}", data):
            continue

        produto = celulas[1]
        if not produto_seagri_ba_interessa(produto):
            continue

        linhas_extraidas.append(
            {
                "data": data,
                "produto": produto,
                "praca": celulas[2],
                "tipo": celulas[3],
                "unidade": celulas[4],
                "preco": celulas[5],
                "origem_parser": "html_table",
            }
        )

    if linhas_extraidas:
        return linhas_extraidas

    # Fallback: texto bruto. Funciona quando a página vem sem tags de tabela
    # preservadas no parser, mas com linhas visíveis no formato da busca.
    texto = soup.get_text(" ", strip=True)
    texto = re.sub(r"\s+", " ", texto)

    padrao = re.compile(
        r"(\d{2}/\d{2}/\d{4})\s+"
        r"(Feij[aã]o|Sorgo|Leite|Boi\s+Gordo)\s+"
        r"([A-ZÁÀÂÃÉÊÍÓÔÕÚÇ0-9' ./-]{2,80})\s+"
        r"([A-Za-zÀ-ÿ0-9ªº()/ .-]{2,50})\s+"
        r"(sc\s*60\s*kg|saca\s*60\s*kg|saca 60 kg|quilo|kg|litro|arroba|@)\s+"
        r"(Sem cotação|sem cotação|\d{1,4}(?:\.\d{3})*,\d{2,4}|\d{1,4}\.\d{2,4})",
        flags=re.I,
    )

    for match in padrao.finditer(texto):
        data, produto, praca, tipo, unidade, preco = match.groups()
        if not produto_seagri_ba_interessa(produto):
            continue

        linhas_extraidas.append(
            {
                "data": data,
                "produto": produto,
                "praca": limpar_texto(praca),
                "tipo": limpar_texto(tipo),
                "unidade": limpar_texto(unidade),
                "preco": limpar_texto(preco),
                "origem_parser": "texto_regex",
            }
        )

    return linhas_extraidas


def coletar_seagri_ba(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    SEAGRI-BA — fonte complementar para Feijão, Sorgo, Leite e Boi Gordo.

    Uso na página:
    - entra como Referência SEAGRI-BA / Regional;
    - ignora linhas com "Sem cotação";
    - não substitui CONAB/AIBA; complementa quando houver preço regional válido;
    - consulta o período recente usado pela política de atualidade do site.
    """
    cotacoes: list[dict[str, Any]] = []
    paginas_lidas = 0
    linhas_encontradas = 0
    linhas_sem_cotacao = 0
    linhas_preco_invalido = 0
    paginas_sem_linhas = 0
    erros_paginas: list[str] = []

    data_fim = agora_local().date()
    data_inicio = data_fim - timedelta(days=DIAS_MAXIMOS_COTACAO_ATIVA)

    # A página é paginada. O limite evita execução longa no GitHub Actions.
    max_paginas = 35
    assinaturas_paginas: set[str] = set()

    for pagina in range(max_paginas):
        params = {
            "data[min][date]": data_inicio.isoformat(),
            "data[max][date]": data_fim.isoformat(),
            "produto": "All",
            "praca": "All",
            "tipo": "All",
            "order": "data",
            "sort": "desc",
            "page": str(pagina),
        }

        try:
            resposta = requests.get(
                SEAGRI_BA_COTACOES_URL,
                headers=HEADERS,
                params=params,
                timeout=90,
            )
            resposta.raise_for_status()
            html = resposta.text
            paginas_lidas += 1

            assinatura = re.sub(r"\s+", " ", BeautifulSoup(html, "html.parser").get_text(" ", strip=True))[:1500]
            if assinatura in assinaturas_paginas and pagina > 0:
                break
            assinaturas_paginas.add(assinatura)

            linhas = extrair_linhas_seagri_ba_html(html)

            if not linhas:
                paginas_sem_linhas += 1
                # Depois de várias páginas seguidas sem feijão/sorgo, evita varrer o portal inteiro.
                if paginas_sem_linhas >= 8 and cotacoes:
                    break
                continue

            paginas_sem_linhas = 0
            linhas_encontradas += len(linhas)

            for linha in linhas:
                preco_texto = limpar_texto(linha.get("preco"))
                if not preco_texto or "sem cot" in remover_acentos(preco_texto).lower():
                    linhas_sem_cotacao += 1
                    continue

                preco = parse_preco(preco_texto)
                if preco is None or preco <= 0:
                    linhas_preco_invalido += 1
                    continue

                produto = limpar_texto(linha.get("produto"))
                tipo = limpar_texto(linha.get("tipo"))
                tipo_norm = remover_acentos(tipo).lower()

                if tipo and tipo_norm not in {"(comum)", "comum", "nao informado", "não informado"}:
                    produto_original = f"{produto} {tipo}"
                else:
                    produto_original = produto

                produto_base = normalizar_produto_base(produto_original)
                if produto_base not in {"Feijão", "Sorgo", "Leite", "Boi Gordo"}:
                    continue

                praca = limpar_texto(linha.get("praca")) or "Bahia"
                unidade = limpar_texto(linha.get("unidade")) or "Saca 60 kg"
                data_ref = parse_data(linha.get("data"))

                cotacoes.append(
                    criar_item(
                        produto_original=produto_original,
                        uf="BA",
                        estado_nome="Bahia",
                        praca=praca,
                        unidade=unidade,
                        preco=preco,
                        variacao_percentual=None,
                        data_referencia=data_ref,
                        fonte="SEAGRI-BA",
                        fonte_url=montar_url_debug_seagri_ba(params),
                        tipo_fonte="regional",
                        nivel_comercializacao="referencia seagri ba",
                        observacao=(
                            "Cotação regional publicada pela SEAGRI-BA. "
                            "Fonte complementar para Feijão, Sorgo, Leite e Boi Gordo; linhas 'Sem cotação' são ignoradas."
                        ),
                    )
                )

        except Exception as erro:
            erros_paginas.append(f"page={pagina}: {erro}")
            if pagina == 0:
                break

    # Deduplicação: mantém a ocorrência mais recente/preço por praça/produto/tipo/unidade/data.
    unicos: dict[tuple[str, str, str, str, str, float], dict[str, Any]] = {}
    for item in cotacoes:
        chave = (
            item.get("produto_base", ""),
            item.get("produto_original", ""),
            item.get("praca", ""),
            item.get("unidade", ""),
            item.get("data_referencia", ""),
            float(item.get("preco", 0)),
        )
        unicos[chave] = item

    cotacoes_finais = list(unicos.values())

    status = "ok" if cotacoes_finais else "sem_registros_extraidos"
    if erros_paginas and not cotacoes_finais:
        status = "erro"

    status_fontes.append(
        {
            "fonte": "SEAGRI-BA",
            "url": SEAGRI_BA_COTACOES_URL,
            "status": status,
            "total_registros": len(cotacoes_finais),
            "produtos_monitorados": ["Feijão", "Sorgo", "Leite", "Boi Gordo"],
            "periodo_consultado": f"{data_inicio.isoformat()} a {data_fim.isoformat()}",
            "paginas_lidas": paginas_lidas,
            "linhas_encontradas_feijao_sorgo": linhas_encontradas,
            "linhas_sem_cotacao_ignoradas": linhas_sem_cotacao,
            "linhas_preco_invalido_ignoradas": linhas_preco_invalido,
            "erros_paginas": erros_paginas[:10],
            "observacao": (
                "Fonte complementar estadual para Feijão, Sorgo, Leite e Boi Gordo. "
                "Publicada como Referência SEAGRI-BA/Regional, sem misturar com CONAB 360."
            ),
        }
    )

    return cotacoes_finais



def salvar_debug_pecuaria(debug: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_PECUARIA.write_text(
        json.dumps(debug, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def extrair_data_mensal_ou_diaria(texto: str) -> str:
    """Extrai data diária ou referência mensal de textos de indicadores."""
    texto_limpo = limpar_texto(texto)

    match = re.search(r"(\d{2}/\d{2}/\d{4}|\d{2}-\d{2}-\d{4}|\d{4}-\d{2}-\d{2})", texto_limpo)
    if match:
        return parse_data(match.group(1))

    meses = {
        "jan": "01", "janeiro": "01",
        "fev": "02", "fevereiro": "02",
        "mar": "03", "marco": "03", "março": "03",
        "abr": "04", "abril": "04",
        "mai": "05", "maio": "05",
        "jun": "06", "junho": "06",
        "jul": "07", "julho": "07",
        "ago": "08", "agosto": "08",
        "set": "09", "setembro": "09",
        "out": "10", "outubro": "10",
        "nov": "11", "novembro": "11",
        "dez": "12", "dezembro": "12",
    }

    texto_norm = remover_acentos(texto_limpo).lower()
    match = re.search(r"\b(jan(?:eiro)?|fev(?:ereiro)?|mar(?:co)?|abr(?:il)?|mai(?:o)?|jun(?:ho)?|jul(?:ho)?|ago(?:sto)?|set(?:embro)?|out(?:ubro)?|nov(?:embro)?|dez(?:embro)?)[./ -]*(\d{2}|\d{4})\b", texto_norm)
    if match:
        mes_raw, ano_raw = match.groups()
        mes = meses.get(mes_raw[:3], "01")
        ano = int(ano_raw)
        if ano < 100:
            ano += 2000
        return f"{ano:04d}-{mes}-01"

    return agora_local().date().isoformat()


def extrair_precos_por_intervalo(texto: str, minimo: float, maximo: float) -> list[float]:
    candidatos = []
    for bruto in re.findall(r"\b\d{1,4}(?:\.\d{3})*,\d{2,4}\b|\b\d{1,4}\.\d{2,4}\b", texto):
        valor = parse_preco(bruto)
        if valor is not None and minimo <= valor <= maximo:
            candidatos.append(valor)
    return candidatos


def extrair_cepea_leite(html: str) -> list[dict[str, Any]]:
    """
    Extrai o indicador CEPEA/ESALQ de leite ao produtor.
    Publica com segurança apenas linhas de Brasil/BA quando forem encontradas.
    """
    soup = BeautifulSoup(html, "html.parser")
    texto_total = soup.get_text(" ", strip=True)
    data_ref = extrair_data_mensal_ou_diaria(texto_total)
    periodo = "Referência mensal CEPEA/ESALQ"
    itens: list[dict[str, Any]] = []

    linhas_candidatas: list[list[str]] = []
    for tr in soup.find_all("tr"):
        celulas = [limpar_texto(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        celulas = [c for c in celulas if c]
        if celulas:
            linhas_candidatas.append(celulas)

    # Fallback: quebra texto em linhas visíveis.
    if not linhas_candidatas:
        linhas_candidatas = [[linha] for linha in soup.get_text("\n", strip=True).splitlines() if limpar_texto(linha)]

    alvos = {
        "BA": ("BA", "Bahia", "Bahia"),
        "BR": ("REF", "Referência CEPEA", "Brasil"),
    }

    vistos: set[str] = set()
    for celulas in linhas_candidatas:
        linha = " | ".join(celulas)
        linha_norm = remover_acentos(linha).lower()

        alvo_key = None
        if re.search(r"\bba\b", linha_norm) or "bahia" in linha_norm:
            alvo_key = "BA"
        elif "brasil" in linha_norm or re.search(r"\bmedia brasil\b", linha_norm):
            alvo_key = "BR"

        if not alvo_key or alvo_key in vistos:
            continue

        precos = extrair_precos_por_intervalo(linha, 0.5, 8.0)
        if not precos:
            continue

        preco = precos[0]
        uf, estado, praca = alvos[alvo_key]
        item = criar_item(
            produto_original="Leite — Preço ao Produtor CEPEA/ESALQ",
            uf=uf,
            estado_nome=estado,
            praca=praca,
            unidade="Litro",
            preco=preco,
            variacao_percentual=None,
            data_referencia=data_ref,
            fonte="CEPEA/ESALQ",
            fonte_url=CEPEA_LEITE_URL,
            tipo_fonte="oficial",
            nivel_comercializacao="preço recebido pelo produtor cepea",
            categoria="pecuaria_leite",
            observacao=(
                "Indicador CEPEA/ESALQ de leite ao produtor. "
                "Referência mensal em R$/litro; não é cotação diária."
            ),
        )
        item["periodo_referencia"] = periodo
        itens.append(item)
        vistos.add(alvo_key)

    # Fallback final: quando a página trouxer apenas um valor geral do indicador.
    if not itens:
        precos = extrair_precos_por_intervalo(texto_total, 0.5, 8.0)
        if precos:
            item = criar_item(
                produto_original="Leite — Preço ao Produtor CEPEA/ESALQ",
                uf="REF",
                estado_nome="Referência CEPEA",
                praca="Brasil",
                unidade="Litro",
                preco=precos[0],
                variacao_percentual=None,
                data_referencia=data_ref,
                fonte="CEPEA/ESALQ",
                fonte_url=CEPEA_LEITE_URL,
                tipo_fonte="oficial",
                nivel_comercializacao="preço recebido pelo produtor cepea",
                categoria="pecuaria_leite",
                observacao=(
                    "Indicador CEPEA/ESALQ de leite ao produtor. "
                    "Referência mensal em R$/litro; não é cotação diária."
                ),
            )
            item["periodo_referencia"] = periodo
            itens.append(item)

    return itens


def extrair_cepea_boi_gordo(html: str) -> Optional[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    texto = soup.get_text(" ", strip=True)
    extraido = extrair_primeiro_indicador_cepea(texto)

    if extraido:
        data_ref, preco, variacao = extraido
    else:
        precos = extrair_precos_por_intervalo(texto, 100.0, 500.0)
        if not precos:
            return None
        data_ref = extrair_data_mensal_ou_diaria(texto)
        preco = precos[0]
        variacao = None

    item = criar_item(
        produto_original="Boi Gordo — Indicador CEPEA/ESALQ",
        uf="REF",
        estado_nome="Referência CEPEA",
        praca="Indicador CEPEA/ESALQ",
        unidade="Arroba (@)",
        preco=preco,
        variacao_percentual=variacao,
        data_referencia=data_ref,
        fonte="CEPEA/ESALQ",
        fonte_url=CEPEA_BOI_GORDO_URL,
        tipo_fonte="oficial",
        nivel_comercializacao="referência de preço ao produtor cepea",
        categoria="pecuaria_corte",
        observacao=(
            "Indicador CEPEA/ESALQ do Boi Gordo. "
            "Referência institucional de mercado físico em R$/arroba."
        ),
    )
    return item


def diagnosticar_acrioeste() -> dict[str, Any]:
    debug: dict[str, Any] = {
        "fonte": "ACRIOESTE",
        "url": ACRIOESTE_URL,
        "status": "nao_testado",
        "publicacao_automatica": False,
        "observacao": "Diagnóstico de fonte regional do Oeste Baiano para leite e boi; não publica até parser ficar estável.",
    }
    try:
        html = baixar_texto(ACRIOESTE_URL, timeout=90)
        texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        debug.update(
            {
                "status": "ok" if texto else "sem_conteudo",
                "tem_boi": "boi" in remover_acentos(texto).lower(),
                "tem_leite": "leite" in remover_acentos(texto).lower(),
                "precos_boi_candidatos": extrair_precos_por_intervalo(texto, 100.0, 500.0)[:20],
                "precos_leite_candidatos": extrair_precos_por_intervalo(texto, 0.5, 8.0)[:20],
                "preview": limitar_texto_debug(texto, 2500),
            }
        )
    except Exception as erro:
        debug.update({"status": "erro", "erro": str(erro)})
    return debug


def diagnosticar_agrolink_pecuaria() -> list[dict[str, Any]]:
    resultados = []
    for nome, url in [
        ("Agrolink MA - Imperatriz", AGROLINK_MA_IMPERATRIZ_URL),
        ("Agrolink PA - Marabá", AGROLINK_PA_MARABA_URL),
    ]:
        debug: dict[str, Any] = {
            "fonte": nome,
            "url": url,
            "status": "nao_testado",
            "publicacao_automatica": False,
            "observacao": "Diagnóstico de referência de mercado para boi gordo por praça; não publica automaticamente nesta versão.",
        }
        try:
            html = baixar_texto(url, timeout=90)
            texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            debug.update(
                {
                    "status": "ok" if texto else "sem_conteudo",
                    "tem_boi_gordo": "boi gordo" in remover_acentos(texto).lower(),
                    "precos_boi_candidatos": extrair_precos_por_intervalo(texto, 100.0, 500.0)[:20],
                    "preview": limitar_texto_debug(texto, 2500),
                }
            )
        except Exception as erro:
            debug.update({"status": "erro", "erro": str(erro)})
        resultados.append(debug)
    return resultados


def criar_item_cepea_fallback(chave: str, motivo: str) -> dict[str, Any]:
    """Cria item de referência CEPEA/ESALQ a partir da tabela fixa de fallback.

    O fallback não substitui a coleta automática: ele só evita que leite e boi
    desapareçam da página quando o GitHub Actions receber 403, HTML quebrado
    ou página sem tabela legível.
    """
    ref = CEPEA_FALLBACK_REFERENCIAS[chave]

    item = criar_item(
        produto_original=ref["produto_original"],
        uf=ref["uf"],
        estado_nome=ref["estado_nome"],
        praca=ref["praca"],
        unidade=ref["unidade"],
        preco=ref["preco"],
        variacao_percentual=ref.get("variacao_percentual"),
        data_referencia=ref["data_referencia"],
        fonte="CEPEA/ESALQ",
        fonte_url=ref["fonte_url"],
        tipo_fonte=ref["tipo_fonte"],
        nivel_comercializacao=ref["nivel_comercializacao"],
        categoria=ref["categoria"],
        observacao=f"{ref['observacao']} Fallback operacional usado porque: {motivo}",
    )
    item["periodo_referencia"] = ref["periodo_referencia"]
    item["fallback_operacional"] = True
    item["motivo_fallback"] = motivo
    item["politica_publicacao"] = "referencia_cepea_sem_preco_local_simulado"
    return item


def criar_fallback_cepea_leite(motivo: str) -> list[dict[str, Any]]:
    return [
        criar_item_cepea_fallback("leite_brasil", motivo),
        criar_item_cepea_fallback("leite_bahia", motivo),
    ]


def criar_fallback_cepea_boi_gordo(motivo: str) -> list[dict[str, Any]]:
    return [criar_item_cepea_fallback("boi_gordo", motivo)]


def remover_duplicados_cepea_pecuaria(itens: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Evita duplicidade entre coleta automática e fallback CEPEA."""
    unicos: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in itens:
        chave = (
            limpar_texto(item.get("produto_base")),
            limpar_texto(item.get("uf")),
            limpar_texto(item.get("praca")),
            limpar_texto(item.get("fonte")),
        )
        # Mantém item automático quando existir; fallback só entra se não houver automático.
        if chave not in unicos:
            unicos[chave] = item
            continue
        if unicos[chave].get("fallback_operacional") and not item.get("fallback_operacional"):
            unicos[chave] = item
    return list(unicos.values())


def coletar_pecuaria_leite_boi(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Pecuária v1.3.0:
    - publica CEPEA/ESALQ para Leite ao Produtor e Boi Gordo;
    - mantém ACRIOESTE e Agrolink como diagnóstico, sem publicação automática;
    - não publica Carne Bovina, pois costuma ser varejo/atacado em kg.
    """
    cotacoes: list[dict[str, Any]] = []
    debug: dict[str, Any] = {
        "projeto": "Nordeste Agro",
        "modulo": "pecuaria_leite_boi",
        "versao": "1.3.4",
        "gerado_em": agora_local().isoformat(),
        "politica": "Leite e Boi Gordo entram por CEPEA/ESALQ como referência segura e por CONAB apenas quando a linha for preço ao produtor. Carne bovina em kg não é publicada na tabela principal.",
        "fontes": [],
    }

    # CEPEA Leite ao Produtor.
    try:
        html = baixar_texto(CEPEA_LEITE_URL, timeout=90)
        itens_leite = extrair_cepea_leite(html)
        fallback_usado = False
        if not itens_leite:
            itens_leite = criar_fallback_cepea_leite("pagina_cepea_sem_registros_extraidos")
            fallback_usado = True
        cotacoes.extend(itens_leite)
        status_fontes.append(
            {
                "fonte": "CEPEA/ESALQ - Leite ao Produtor",
                "url": CEPEA_LEITE_URL,
                "status": "ok_fallback" if fallback_usado else "ok",
                "total_registros": len(itens_leite),
                "categoria": "pecuaria_leite",
                "fallback_operacional": fallback_usado,
                "observacao": "Indicador institucional de leite ao produtor em R$/litro. Referência mensal; não é preço local de PI, MA ou PA.",
            }
        )
        debug["fontes"].append(
            {
                "fonte": "CEPEA/ESALQ - Leite ao Produtor",
                "url": CEPEA_LEITE_URL,
                "status": "ok_fallback" if fallback_usado else "ok",
                "total_registros": len(itens_leite),
                "fallback_operacional": fallback_usado,
                "amostra": itens_leite[:5],
            }
        )
    except Exception as erro:
        itens_leite = criar_fallback_cepea_leite(f"erro_coleta_cepea_leite:{erro}")
        cotacoes.extend(itens_leite)
        status_fontes.append(
            {
                "fonte": "CEPEA/ESALQ - Leite ao Produtor",
                "url": CEPEA_LEITE_URL,
                "status": "ok_fallback_pos_erro",
                "erro": str(erro),
                "total_registros": len(itens_leite),
                "categoria": "pecuaria_leite",
                "fallback_operacional": True,
            }
        )
        debug["fontes"].append({"fonte": "CEPEA/ESALQ - Leite ao Produtor", "url": CEPEA_LEITE_URL, "status": "ok_fallback_pos_erro", "erro": str(erro), "amostra": itens_leite[:5]})

    # CEPEA Boi Gordo.
    try:
        html = baixar_texto(CEPEA_BOI_GORDO_URL, timeout=90)
        item_boi = extrair_cepea_boi_gordo(html)
        itens_boi = [item_boi] if item_boi else []
        fallback_usado = False
        if not itens_boi:
            itens_boi = criar_fallback_cepea_boi_gordo("pagina_cepea_sem_registros_extraidos")
            fallback_usado = True
        cotacoes.extend(itens_boi)
        status_fontes.append(
            {
                "fonte": "CEPEA/ESALQ - Boi Gordo",
                "url": CEPEA_BOI_GORDO_URL,
                "status": "ok_fallback" if fallback_usado else "ok",
                "total_registros": len(itens_boi),
                "categoria": "pecuaria_corte",
                "fallback_operacional": fallback_usado,
                "observacao": "Indicador institucional do Boi Gordo em R$/arroba. Referência de mercado; não é preço local da praça selecionada.",
            }
        )
        debug["fontes"].append(
            {
                "fonte": "CEPEA/ESALQ - Boi Gordo",
                "url": CEPEA_BOI_GORDO_URL,
                "status": "ok_fallback" if fallback_usado else "ok",
                "total_registros": len(itens_boi),
                "fallback_operacional": fallback_usado,
                "amostra": itens_boi[:5],
            }
        )
    except Exception as erro:
        itens_boi = criar_fallback_cepea_boi_gordo(f"erro_coleta_cepea_boi:{erro}")
        cotacoes.extend(itens_boi)
        status_fontes.append(
            {
                "fonte": "CEPEA/ESALQ - Boi Gordo",
                "url": CEPEA_BOI_GORDO_URL,
                "status": "ok_fallback_pos_erro",
                "erro": str(erro),
                "total_registros": len(itens_boi),
                "categoria": "pecuaria_corte",
                "fallback_operacional": True,
            }
        )
        debug["fontes"].append({"fonte": "CEPEA/ESALQ - Boi Gordo", "url": CEPEA_BOI_GORDO_URL, "status": "ok_fallback_pos_erro", "erro": str(erro), "amostra": itens_boi[:5]})

    # Diagnósticos regionais sem publicação automática.
    acrioeste_debug = diagnosticar_acrioeste()
    debug["fontes"].append(acrioeste_debug)
    status_fontes.append(
        {
            "fonte": "ACRIOESTE",
            "url": ACRIOESTE_URL,
            "status": acrioeste_debug.get("status"),
            "total_registros": 0,
            "categoria": "diagnostico_pecuaria",
            "observacao": "Diagnóstico regional do Oeste Baiano para leite/boi. Não publicado automaticamente nesta versão.",
        }
    )

    for agrolink_debug in diagnosticar_agrolink_pecuaria():
        debug["fontes"].append(agrolink_debug)
        status_fontes.append(
            {
                "fonte": agrolink_debug.get("fonte"),
                "url": agrolink_debug.get("url"),
                "status": agrolink_debug.get("status"),
                "total_registros": 0,
                "categoria": "diagnostico_pecuaria",
                "observacao": "Diagnóstico de referência de mercado para boi por praça. Não publicado automaticamente nesta versão.",
            }
        )

    cotacoes = remover_duplicados_cepea_pecuaria(cotacoes)

    debug["total_cotacoes_publicaveis_extraidas"] = len(cotacoes)
    debug["produtos_extraidos"] = sorted({item.get("produto_base") for item in cotacoes if item.get("produto_base")})
    debug["ufs_extraidas"] = sorted({item.get("uf") for item in cotacoes if item.get("uf")})
    salvar_debug_pecuaria(debug)
    return cotacoes


def salvar_debug_ceasas(debug: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_CEASAS.write_text(
        json.dumps(debug, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def produto_ceasa_interessa(produto: Any) -> bool:
    texto = remover_acentos(produto).lower()
    texto = re.sub(r"[^a-z0-9 ]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    if not texto:
        return False

    for alvo in PRODUTOS_CEASA_MONITORADOS:
        alvo_norm = remover_acentos(alvo).lower()
        alvo_norm = re.sub(r"[^a-z0-9 ]+", " ", alvo_norm).strip()
        if alvo_norm and re.search(rf"(^|\s){re.escape(alvo_norm)}(\s|$)", texto):
            return True

    return False


def normalizar_produto_ceasa(produto: Any) -> str:
    texto = limpar_texto(produto)
    texto = re.sub(r"\s+", " ", texto).strip(" -–—")

    # Remove marcas comuns de classificação/tipo que costumam vir grudadas no nome.
    texto = re.sub(r"\b(extra|especial|comum|primeira|segunda|tipo\s*1|tipo\s*2)\b", "", texto, flags=re.I)
    texto = re.sub(r"\s+", " ", texto).strip(" -–—")

    if not texto:
        return "Produto CEASA"

    return texto.title()


def unidade_ceasa_valida(unidade: Any) -> bool:
    texto = remover_acentos(unidade).lower()
    return any(u in texto for u in [remover_acentos(x).lower() for x in UNIDADES_CEASA_VALIDAS])


def inferir_unidade_ceasa(celulas: list[str]) -> str:
    for celula in celulas:
        if unidade_ceasa_valida(celula):
            return limpar_texto(celula)
    return "Unidade informada pela fonte"


def parse_preco_ceasa_celula(valor: Any) -> Optional[float]:
    texto = limpar_texto(valor)
    if not texto or "sem cot" in remover_acentos(texto).lower():
        return None
    if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", texto):
        return None
    if "%" in texto:
        return None

    return parse_preco(texto)


def escolher_preco_ceasa(celulas: list[str], cabecalhos: list[str]) -> Optional[float]:
    # Preferência: coluna com preço médio/médio/valor.
    candidatos_prioritarios: list[float] = []
    candidatos_gerais: list[float] = []

    for i, celula in enumerate(celulas):
        preco = parse_preco_ceasa_celula(celula)
        if preco is None or preco <= 0:
            continue

        cab = remover_acentos(cabecalhos[i]).lower() if i < len(cabecalhos) else ""
        if any(chave in cab for chave in ["medio", "media", "preco medio", "valor medio", "preco", "valor"]):
            candidatos_prioritarios.append(preco)
        else:
            candidatos_gerais.append(preco)

    if candidatos_prioritarios:
        return candidatos_prioritarios[0]

    if candidatos_gerais:
        return candidatos_gerais[0]

    return None


def extrair_data_ceasa(html: str) -> str:
    texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    match = re.search(r"(\d{2}/\d{2}/\d{4})", texto)
    if match:
        return parse_data(match.group(1))
    return agora_local().date().isoformat()


def extrair_linhas_ceasa_html_generico(
    html: str,
    *,
    fonte: str,
    uf: str,
    estado_nome: str,
    praca_padrao: str,
    fonte_url: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Parser defensivo para boletins de CEASA.

    Ele não assume um único layout, porque CEASA-CE, CEASA-PE e outras fontes
    mudam marcações de tabela com frequência. A regra é publicar somente linhas
    que contenham produto monitorado, preço numérico e unidade plausível.
    """
    itens: list[dict[str, Any]] = []
    soup = BeautifulSoup(html, "html.parser")
    data_ref = extrair_data_ceasa(html)

    debug = {
        "fonte": fonte,
        "url": fonte_url,
        "linhas_tabela_lidas": 0,
        "linhas_com_produto_monitorado": 0,
        "linhas_preco_invalido": 0,
        "linhas_publicadas": 0,
        "amostra_linhas": [],
    }

    for tabela in soup.find_all("table"):
        linhas = tabela.find_all("tr")
        cabecalhos: list[str] = []

        for tr in linhas:
            celulas = [limpar_texto(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
            celulas = [c for c in celulas if c]
            if len(celulas) < 2:
                continue

            debug["linhas_tabela_lidas"] += 1

            if tr.find_all("th") or not cabecalhos:
                possivel_cab = [remover_acentos(c).lower() for c in celulas]
                if any("produto" in c or "preco" in c or "valor" in c or "medio" in c for c in possivel_cab):
                    cabecalhos = celulas
                    continue

            produto_candidato = ""
            for celula in celulas:
                if produto_ceasa_interessa(celula):
                    produto_candidato = normalizar_produto_ceasa(celula)
                    break

            if not produto_candidato:
                continue

            debug["linhas_com_produto_monitorado"] += 1
            if len(debug["amostra_linhas"]) < 20:
                debug["amostra_linhas"].append(celulas)

            preco = escolher_preco_ceasa(celulas, cabecalhos)
            if preco is None:
                debug["linhas_preco_invalido"] += 1
                continue

            unidade = inferir_unidade_ceasa(celulas)
            praca = praca_padrao

            # Alguns boletins trazem o entreposto/praça na primeira coluna.
            for celula in celulas[:3]:
                celula_norm = remover_acentos(celula).lower()
                if any(chave in celula_norm for chave in ["maracanau", "tiangua", "cariri", "recife", "ceasa", "entreposto"]):
                    praca = limpar_texto(celula)
                    break

            itens.append(
                criar_item(
                    produto_original=produto_candidato,
                    uf=uf,
                    estado_nome=estado_nome,
                    praca=praca,
                    unidade=unidade,
                    preco=preco,
                    variacao_percentual=None,
                    data_referencia=data_ref,
                    fonte=fonte,
                    fonte_url=fonte_url,
                    tipo_fonte="ceasa",
                    nivel_comercializacao="atacado ceasa hortifruti",
                    categoria="hortifruti_ceasa",
                    converter_unidade=False,
                    observacao=(
                        "Cotação de hortifruti em CEASA/mercado atacadista. "
                        "Não representa preço recebido pelo produtor e fica separada das commodities agrícolas."
                    ),
                )
            )

    debug["linhas_publicadas"] = len(itens)

    # Remove duplicados do mesmo produto/praça/data/fonte.
    unicos: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in itens:
        chave = (
            item.get("produto_base", ""),
            item.get("uf", ""),
            item.get("praca", ""),
            item.get("data_referencia", ""),
        )
        unicos[chave] = item

    itens_unicos = list(unicos.values())
    debug["linhas_publicadas_unicas"] = len(itens_unicos)
    return itens_unicos, debug


def coletar_ceasa_ce(status_fontes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        html = baixar_texto(CEASA_CE_BOLETIM_URL, timeout=90)
        itens, debug = extrair_linhas_ceasa_html_generico(
            html,
            fonte="CEASA-CE",
            uf="CE",
            estado_nome="Ceará",
            praca_padrao="CEASA Ceará",
            fonte_url=CEASA_CE_BOLETIM_URL,
        )
        status_fontes.append(
            {
                "fonte": "CEASA-CE",
                "url": CEASA_CE_BOLETIM_URL,
                "status": "ok" if itens else "sem_registros_extraidos",
                "total_registros": len(itens),
                "categoria": "hortifruti_ceasa",
                "observacao": "Fonte diária de hortifruti/atacado. Publicada como Atacado CEASA, separada de preço ao produtor.",
                "debug": debug,
            }
        )
        return itens, debug
    except Exception as erro:
        debug = {"fonte": "CEASA-CE", "url": CEASA_CE_BOLETIM_URL, "erro": str(erro)}
        status_fontes.append(
            {
                "fonte": "CEASA-CE",
                "url": CEASA_CE_BOLETIM_URL,
                "status": "erro",
                "erro": str(erro),
                "total_registros": 0,
                "categoria": "hortifruti_ceasa",
            }
        )
        return [], debug


def coletar_ceasa_pe(status_fontes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        html = baixar_texto(CEASA_PE_COTACAO_URL, timeout=90)
        itens, debug = extrair_linhas_ceasa_html_generico(
            html,
            fonte="CEASA-PE",
            uf="PE",
            estado_nome="Pernambuco",
            praca_padrao="CEASA Pernambuco",
            fonte_url=CEASA_PE_COTACAO_URL,
        )
        status_fontes.append(
            {
                "fonte": "CEASA-PE",
                "url": CEASA_PE_COTACAO_URL,
                "status": "ok" if itens else "sem_registros_extraidos",
                "total_registros": len(itens),
                "categoria": "hortifruti_ceasa",
                "observacao": "Fonte diária de hortifruti/atacado. Publicada como Atacado CEASA, separada de preço ao produtor.",
                "debug": debug,
            }
        )
        return itens, debug
    except Exception as erro:
        debug = {"fonte": "CEASA-PE", "url": CEASA_PE_COTACAO_URL, "erro": str(erro)}
        status_fontes.append(
            {
                "fonte": "CEASA-PE",
                "url": CEASA_PE_COTACAO_URL,
                "status": "erro",
                "erro": str(erro),
                "total_registros": 0,
                "categoria": "hortifruti_ceasa",
            }
        )
        return [], debug


def diagnosticar_prohort(status_fontes: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Nesta versão, o PROHORT entra como diagnóstico de disponibilidade.
    A coleta via CDA será habilitada depois de mapear os dataAccessId/params
    com segurança, sem criar preço errado na tabela.
    """
    debug = {
        "fonte": "PROHORT/CONAB",
        "url": PROHORT_PRECO_DIA_URL,
        "status": "nao_testado",
        "total_registros": 0,
        "observacao": "Diagnóstico de acesso ao painel PROHORT. Não publica preço até mapear a chamada CDA correta.",
    }

    try:
        html = baixar_texto(PROHORT_PRECO_DIA_URL, timeout=90)
        texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        debug.update(
            {
                "status": "ok" if texto else "sem_conteudo",
                "preview": limitar_texto_debug(texto, 1500),
                "tem_produto": "PRODUTO" in texto.upper() or "produto" in texto.lower(),
                "tem_ceasa": "CEASA" in texto.upper(),
            }
        )
    except Exception as erro:
        debug.update({"status": "erro", "erro": str(erro)})

    status_fontes.append(
        {
            "fonte": "PROHORT/CONAB",
            "url": PROHORT_PRECO_DIA_URL,
            "status": debug.get("status"),
            "total_registros": 0,
            "categoria": "hortifruti_ceasa",
            "observacao": "Fonte nacional prioritária para CEASAS. v1.3.0 mantém diagnóstico; publicação será ativada após mapear a CDA do painel.",
        }
    )
    return debug


def coletar_ceasas(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Módulo inicial de CEASAS/Hortifruti.

    Publica CEASA-CE e CEASA-PE quando o parser encontra linhas claras.
    Mantém PROHORT/CONAB como diagnóstico para a próxima etapa.
    """
    cotacoes: list[dict[str, Any]] = []
    debug = {
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes_ceasas",
        "versao": "1.3.4",
        "gerado_em": agora_local().isoformat(),
        "politica": "CEASA/Hortifruti é preço de atacado. Não misturar com preço ao produtor.",
        "fontes": [],
    }

    prohort_debug = diagnosticar_prohort(status_fontes)
    debug["fontes"].append(prohort_debug)

    itens_ce, debug_ce = coletar_ceasa_ce(status_fontes)
    cotacoes.extend(itens_ce)
    debug["fontes"].append(debug_ce)

    itens_pe, debug_pe = coletar_ceasa_pe(status_fontes)
    cotacoes.extend(itens_pe)
    debug["fontes"].append(debug_pe)

    debug["total_cotacoes_extraidas"] = len(cotacoes)
    debug["produtos_extraidos"] = sorted({item.get("produto_base") for item in cotacoes if item.get("produto_base")})
    debug["ufs_extraidas"] = sorted({item.get("uf") for item in cotacoes if item.get("uf")})
    salvar_debug_ceasas(debug)

    return cotacoes


def categoria_conab_produto(produto_base: Any) -> str:
    produto = limpar_texto(produto_base)

    if produto == "Leite":
        return "pecuaria_leite"

    if produto == "Boi Gordo":
        return "pecuaria_corte"

    if produto == "Carne Bovina":
        return "pecuaria_corte"

    return "commodity_agricola"


def observacao_conab_produto(produto_base: Any, nivel_label: str) -> str:
    produto = limpar_texto(produto_base)
    base = (
        "Preço agropecuário oficial/compilado pela CONAB e parceiros. "
        f"Fonte operacional: {FONTE_CONAB_POR_PRODUTO.get(produto, 'CONAB Preços Agropecuários')}. "
        f"Nível identificado: {nivel_label}."
    )

    if produto == "Boi Gordo":
        return (
            base
            + " Regra Nordeste Agro: quando a CONAB informar BOI/BOI GORDO em R$/kg, "
            + "o coletor converte para Arroba (@) usando 1 @ = 15 kg, mantendo o preço original no JSON."
        )

    if produto == "Leite":
        return base + " Regra Nordeste Agro: leite ao produtor permanece em R$/litro."

    return base


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

                if produto_base_conab not in PRODUTOS_CONAB_TXT:
                    continue

                praca = "Média UF"

                if col_praca:
                    praca = limpar_texto(linha.get(col_praca, "")) or "Média UF"

                unidade = limpar_texto(linha.get(col_unidade, "")) if col_unidade else ""

                # v1.3.0: os arquivos CONAB Preços Agropecuários são semanais.
                # Quando a fonte trouxer apenas uma data, mostramos como "Semana de DD/MM/AAAA"
                # para não dar a impressão de preço diário.
                data_original_conab = linha.get(col_data, "") if col_data else agora_local().date().isoformat()
                periodo_conab = extrair_periodo_semanal(data_original_conab)
                data_ref = periodo_conab.get("data_fim") or parse_data(data_original_conab)
                data_inicio_conab = periodo_conab.get("data_inicio") or data_ref
                data_fim_conab = periodo_conab.get("data_fim") or data_ref
                periodo_referencia_conab = periodo_conab.get("periodo_referencia") or f"Semana de {data_para_br(data_ref)}"

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

                    # Correção v1.3.0: quando a CONAB não informar claramente
                    # o nível, publicamos como Referência CONAB, sem chamar de
                    # preço ao produtor. Atacado e varejo continuam bloqueados.
                    if nivel_chave in {"nao_informado", "media_uf"}:
                        texto_nivel_norm = remover_acentos(nivel_texto).lower()
                        if "atacado" not in texto_nivel_norm and "varejo" not in texto_nivel_norm and "consumidor" not in texto_nivel_norm:
                            if produto_base_conab in PRODUTOS_CONAB_PECUARIA_PRECO_RECEBIDO:
                                nivel_texto = "preço recebido pelo produtor conab"
                            elif fonte.get("tipo", "") == "semanal_municipio":
                                nivel_texto = "referencia conab municipal"
                            else:
                                nivel_texto = "referencia conab estadual"
                            nivel_chave, nivel_label, _ = normalizar_nivel_preco(nivel_texto)

                    fonte_url_item = (
                        CONAB_PRECOS_AGROPECUARIOS_URL
                        if produto_base_conab in PRODUTOS_CONAB_PECUARIA_PRECO_RECEBIDO
                        else url
                    )

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
                            data_referencia_inicio=data_inicio_conab,
                            data_referencia_fim=data_fim_conab,
                            periodo_referencia=periodo_referencia_conab,
                            fonte=nome,
                            fonte_url=fonte_url_item,
                            tipo_fonte="oficial",
                            nivel_comercializacao=nivel_texto,
                            categoria=categoria_conab_produto(produto_base_conab),
                            observacao=observacao_conab_produto(produto_base_conab, nivel_label),
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
                    "produtos_conab_publicaveis": sorted(PRODUTOS_CONAB_TXT),
                    "observacao": "v1.3.4: arquivos semanais usados para Feijão, Sorgo, Leite, Boi Gordo e Carne Bovina. Leite e Boi Gordo entram quando o nível vier como PREÇO RECEBIDO/Produtor. Boi em R$/kg é convertido para Arroba (@) com fator 15; Leite permanece em Litro. Varejo, atacado e nível não informado seguem bloqueados. Soja, Milho e Algodão saem do Produtos 360º.",
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

    Regra v1.3.0:
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

        # Exceção controlada v1.3.3:
        # Leite e Boi Gordo CEPEA/ESALQ são referências institucionais/mensais.
        # Quando a página do CEPEA bloqueia o GitHub Actions ou retorna fallback,
        # o dado pode ter período oficial anterior ao limite de 90 dias.
        # Nesse caso, mantemos apenas a referência CEPEA mais recente do grupo,
        # sempre rotulada como referência de mercado, nunca como preço local.
        if not itens_recentes:
            referencias_cepea_pecuaria = [
                item
                for item in itens_ordenados
                if (
                    ("CEPEA" in limpar_texto(item.get("fonte")) or "ESALQ" in limpar_texto(item.get("fonte")))
                    and item.get("categoria") in {"pecuaria_leite", "pecuaria_corte"}
                    and item.get("produto_base") in {"Leite", "Boi Gordo"}
                    and item.get("preco") is not None
                )
            ]

            if referencias_cepea_pecuaria:
                item_cepea = dict(referencias_cepea_pecuaria[-1])
                item_cepea["referencia_fora_janela_90d"] = True
                item_cepea["observacao"] = (
                    limpar_texto(item_cepea.get("observacao"))
                    + " Exceção de atualidade: referência CEPEA/ESALQ mantida na tabela mesmo fora da janela de 90 dias, por ser indicador institucional/mensal e não preço local."
                ).strip()
                itens_recentes = [item_cepea]
            else:
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
        "data_referencia_inicio",
        "data_referencia_fim",
        "periodo_referencia",
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




def diagnosticar_produtos_pecuarios_conab(
    *,
    cotacoes_brutas: list[dict[str, Any]],
    cotacoes_validas: list[dict[str, Any]],
    cotacoes_descartadas_validacao: list[dict[str, Any]],
    cotacoes_produtor_regional: list[dict[str, Any]],
    cotacoes_descartadas_nivel: list[dict[str, Any]],
    cotacoes_tabela: list[dict[str, Any]],
    data_corte_iso: str,
) -> dict[str, Any]:
    """
    Diagnóstico específico para Leite, Boi Gordo e Carne Bovina na CONAB semanal.

    Objetivo:
    - confirmar se a CONAB trouxe registros brutos;
    - descobrir se caíram por validação de preço/unidade;
    - descobrir se foram bloqueados por nível de comercialização;
    - verificar se ficaram fora por data antiga ou consolidação.
    """
    produtos_alvo = ["Leite", "Boi Gordo", "Carne Bovina"]

    def eh_produto_conab(item: dict[str, Any], produto: str) -> bool:
        return limpar_texto(item.get("produto_base")) == produto and "CONAB" in limpar_texto(item.get("fonte"))

    def amostra(lista: list[dict[str, Any]], limite: int = 80) -> list[dict[str, Any]]:
        campos = [
            "produto", "produto_base", "produto_original", "uf", "estado", "praca", "unidade",
            "unidade_original", "preco", "preco_original", "preco_formatado", "fator_conversao",
            "conversao_aplicada", "data_referencia", "periodo_referencia", "fonte",
            "nivel_comercializacao", "nivel_comercializacao_chave", "prioridade_nivel_preco",
            "motivo", "validacao_publicacao", "observacao",
        ]
        return [{campo: item.get(campo) for campo in campos if campo in item} for item in lista[:limite]]

    diagnosticos: dict[str, Any] = {}
    totais_gerais = {
        "brutas_conab": 0,
        "validas_apos_validacao": 0,
        "descartadas_por_validacao": 0,
        "publicaveis_apos_filtro_nivel": 0,
        "descartadas_por_nivel": 0,
        "grupos_publicaveis": 0,
        "grupos_antigos": 0,
        "grupos_recentes": 0,
        "na_tabela_final": 0,
    }

    for produto in produtos_alvo:
        brutas = [item for item in cotacoes_brutas if eh_produto_conab(item, produto)]
        validas = [item for item in cotacoes_validas if eh_produto_conab(item, produto)]
        descartadas_validacao = [item for item in cotacoes_descartadas_validacao if eh_produto_conab(item, produto)]
        nivel_ok = [item for item in cotacoes_produtor_regional if eh_produto_conab(item, produto)]
        descartadas_nivel = [item for item in cotacoes_descartadas_nivel if eh_produto_conab(item, produto)]
        tabela = [item for item in cotacoes_tabela if eh_produto_conab(item, produto)]

        grupos: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for item in nivel_ok:
            grupos.setdefault(chave_agrupamento(item), []).append(item)

        grupos_antigos = []
        grupos_recentes = []
        for chave, itens in grupos.items():
            itens_ordenados = sorted(itens, key=lambda x: data_ordenavel(x.get("data_referencia")))
            mais_recente = itens_ordenados[-1] if itens_ordenados else {}
            entrada = {
                "chave": list(chave),
                "total_itens": len(itens_ordenados),
                "mais_recente": {
                    "produto": mais_recente.get("produto"),
                    "uf": mais_recente.get("uf"),
                    "estado": mais_recente.get("estado"),
                    "praca": mais_recente.get("praca"),
                    "unidade": mais_recente.get("unidade"),
                    "unidade_original": mais_recente.get("unidade_original"),
                    "preco": mais_recente.get("preco"),
                    "preco_original": mais_recente.get("preco_original"),
                    "data_referencia": mais_recente.get("data_referencia"),
                    "periodo_referencia": mais_recente.get("periodo_referencia"),
                    "fonte": mais_recente.get("fonte"),
                    "nivel_comercializacao": mais_recente.get("nivel_comercializacao"),
                    "nivel_comercializacao_chave": mais_recente.get("nivel_comercializacao_chave"),
                    "observacao": mais_recente.get("observacao"),
                },
            }
            if mais_recente and not data_dentro_do_limite(mais_recente.get("data_referencia"), data_corte_iso):
                grupos_antigos.append(entrada)
            else:
                grupos_recentes.append(entrada)

        motivos_validacao: dict[str, int] = {}
        for item in descartadas_validacao:
            motivo = limpar_texto(item.get("motivo", "motivo_nao_informado"))
            motivos_validacao[motivo] = motivos_validacao.get(motivo, 0) + 1

        motivos_nivel: dict[str, int] = {}
        for item in descartadas_nivel:
            motivo = limpar_texto(item.get("motivo", "motivo_nao_informado"))
            motivos_nivel[motivo] = motivos_nivel.get(motivo, 0) + 1

        if tabela:
            conclusao = f"{produto} CONAB entrou na tabela final."
        elif not brutas:
            conclusao = f"Nenhum registro bruto de {produto} foi encontrado nos arquivos semanais da CONAB para os estados monitorados."
        elif descartadas_validacao and not validas:
            conclusao = f"{produto} CONAB foi coletado, mas caiu na validação comercial/unidade/preço. Verificar unidade e faixa de validação."
        elif descartadas_nivel and not nivel_ok:
            conclusao = f"{produto} CONAB foi coletado, mas foi bloqueado pelo nível de comercialização. Provável atacado, varejo ou nível não permitido para tabela principal."
        elif grupos_antigos and not grupos_recentes:
            conclusao = f"{produto} CONAB foi coletado e aprovado, mas o grupo mais recente ficou fora por data antiga."
        elif nivel_ok and not tabela:
            conclusao = f"{produto} CONAB foi aprovado antes da consolidação, mas não apareceu na tabela final; verificar agrupamento/consolidação."
        else:
            conclusao = f"{produto} CONAB requer revisão manual: há registros, mas não foi possível classificar o ponto de bloqueio automaticamente."

        totais = {
            "brutas_conab": len(brutas),
            "validas_apos_validacao": len(validas),
            "descartadas_por_validacao": len(descartadas_validacao),
            "publicaveis_apos_filtro_nivel": len(nivel_ok),
            "descartadas_por_nivel": len(descartadas_nivel),
            "grupos_publicaveis": len(grupos),
            "grupos_antigos": len(grupos_antigos),
            "grupos_recentes": len(grupos_recentes),
            "na_tabela_final": len(tabela),
        }

        for chave, valor in totais.items():
            totais_gerais[chave] += valor

        diagnosticos[produto] = {
            "produto": produto,
            "fonte": "CONAB - Preços Agropecuários Semanal UF/Município",
            "data_corte_cotacoes_ativas": data_corte_iso,
            "conclusao": conclusao,
            "totais": totais,
            "motivos_descartes_validacao": motivos_validacao,
            "motivos_descartes_nivel": motivos_nivel,
            "grupos_recentes": grupos_recentes[:80],
            "grupos_antigos": grupos_antigos[:80],
            "amostra_brutas": amostra(brutas),
            "amostra_validas": amostra(validas),
            "amostra_descartadas_validacao": amostra(descartadas_validacao),
            "amostra_descartadas_nivel": amostra(descartadas_nivel),
            "amostra_tabela_final": amostra(tabela),
        }

    return {
        "produtos": produtos_alvo,
        "fonte": "CONAB - Preços Agropecuários Semanal UF/Município",
        "data_corte_cotacoes_ativas": data_corte_iso,
        "politica_publicacao": "Leite, Boi Gordo e Carne Bovina só entram na tabela principal quando a CONAB informa preço ao produtor. Varejo, atacado e nível não informado continuam bloqueados.",
        "totais_gerais": totais_gerais,
        "diagnosticos": diagnosticos,
    }


def diagnosticar_sorgo_conab(
    *,
    cotacoes_brutas: list[dict[str, Any]],
    cotacoes_validas: list[dict[str, Any]],
    cotacoes_descartadas_validacao: list[dict[str, Any]],
    cotacoes_produtor_regional: list[dict[str, Any]],
    cotacoes_descartadas_nivel: list[dict[str, Any]],
    cotacoes_tabela: list[dict[str, Any]],
    data_corte_iso: str,
) -> dict[str, Any]:
    """Diagnóstico específico para entender por que o sorgo CONAB aparece ou não aparece no site."""

    def eh_sorgo_conab(item: dict[str, Any]) -> bool:
        return limpar_texto(item.get("produto_base")) == "Sorgo" and "CONAB" in limpar_texto(item.get("fonte"))

    brutas = [item for item in cotacoes_brutas if eh_sorgo_conab(item)]
    validas = [item for item in cotacoes_validas if eh_sorgo_conab(item)]
    descartadas_validacao = [item for item in cotacoes_descartadas_validacao if eh_sorgo_conab(item)]
    nivel_ok = [item for item in cotacoes_produtor_regional if eh_sorgo_conab(item)]
    descartadas_nivel = [item for item in cotacoes_descartadas_nivel if eh_sorgo_conab(item)]
    tabela = [item for item in cotacoes_tabela if eh_sorgo_conab(item)]

    grupos: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for item in nivel_ok:
        grupos.setdefault(chave_agrupamento(item), []).append(item)

    grupos_antigos = []
    grupos_recentes = []
    for chave, itens in grupos.items():
        itens_ordenados = sorted(itens, key=lambda x: data_ordenavel(x.get("data_referencia")))
        mais_recente = itens_ordenados[-1] if itens_ordenados else {}
        entrada = {
            "chave": list(chave),
            "total_itens": len(itens_ordenados),
            "mais_recente": {
                "produto": mais_recente.get("produto"),
                "uf": mais_recente.get("uf"),
                "estado": mais_recente.get("estado"),
                "praca": mais_recente.get("praca"),
                "unidade": mais_recente.get("unidade"),
                "preco": mais_recente.get("preco"),
                "preco_original": mais_recente.get("preco_original"),
                "data_referencia": mais_recente.get("data_referencia"),
                "periodo_referencia": mais_recente.get("periodo_referencia"),
                "fonte": mais_recente.get("fonte"),
                "nivel_comercializacao": mais_recente.get("nivel_comercializacao"),
                "nivel_comercializacao_chave": mais_recente.get("nivel_comercializacao_chave"),
                "observacao": mais_recente.get("observacao"),
            },
        }
        if mais_recente and not data_dentro_do_limite(mais_recente.get("data_referencia"), data_corte_iso):
            grupos_antigos.append(entrada)
        else:
            grupos_recentes.append(entrada)

    def amostra(lista: list[dict[str, Any]], limite: int = 80) -> list[dict[str, Any]]:
        campos = [
            "produto", "produto_base", "produto_original", "uf", "estado", "praca", "unidade",
            "unidade_original", "preco", "preco_original", "preco_formatado", "data_referencia",
            "periodo_referencia", "fonte", "nivel_comercializacao", "nivel_comercializacao_chave",
            "motivo", "validacao_publicacao", "observacao",
        ]
        return [{campo: item.get(campo) for campo in campos if campo in item} for item in lista[:limite]]

    motivos_validacao: dict[str, int] = {}
    for item in descartadas_validacao:
        motivo = limpar_texto(item.get("motivo", "motivo_nao_informado"))
        motivos_validacao[motivo] = motivos_validacao.get(motivo, 0) + 1

    motivos_nivel: dict[str, int] = {}
    for item in descartadas_nivel:
        motivo = limpar_texto(item.get("motivo", "motivo_nao_informado"))
        motivos_nivel[motivo] = motivos_nivel.get(motivo, 0) + 1

    conclusao = ""
    if tabela:
        conclusao = "Sorgo CONAB entrou na tabela final."
    elif not brutas:
        conclusao = "Nenhum registro bruto de Sorgo foi encontrado nos arquivos semanais da CONAB para os estados monitorados."
    elif descartadas_validacao and not validas:
        conclusao = "Sorgo CONAB foi coletado, mas caiu na validação comercial/unidade/preço."
    elif descartadas_nivel and not nivel_ok:
        conclusao = "Sorgo CONAB foi coletado, mas foi bloqueado pelo nível de comercialização, provavelmente atacado/varejo/nível não permitido."
    elif grupos_antigos and not grupos_recentes:
        conclusao = "Sorgo CONAB foi coletado e aprovado, mas o grupo mais recente ficou fora por data antiga."
    elif nivel_ok and not tabela:
        conclusao = "Sorgo CONAB foi aprovado antes da consolidação, mas não apareceu na tabela final; verificar agrupamento/consolidação."
    else:
        conclusao = "Sorgo CONAB requer revisão manual: há registros, mas não foi possível classificar o ponto de bloqueio automaticamente."

    return {
        "produto": "Sorgo",
        "fonte": "CONAB - Preços Agropecuários Semanal UF/Município",
        "data_corte_cotacoes_ativas": data_corte_iso,
        "conclusao": conclusao,
        "totais": {
            "brutas_sorgo_conab": len(brutas),
            "validas_apos_validacao": len(validas),
            "descartadas_por_validacao": len(descartadas_validacao),
            "publicaveis_apos_filtro_nivel": len(nivel_ok),
            "descartadas_por_nivel": len(descartadas_nivel),
            "grupos_publicaveis": len(grupos),
            "grupos_antigos": len(grupos_antigos),
            "grupos_recentes": len(grupos_recentes),
            "na_tabela_final": len(tabela),
        },
        "motivos_descartes_validacao": motivos_validacao,
        "motivos_descartes_nivel": motivos_nivel,
        "grupos_recentes": grupos_recentes[:80],
        "grupos_antigos": grupos_antigos[:80],
        "amostra_brutas": amostra(brutas),
        "amostra_descartadas_validacao": amostra(descartadas_validacao),
        "amostra_descartadas_nivel": amostra(descartadas_nivel),
        "amostra_tabela_final": amostra(tabela),
    }

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
        "debug_conab_produtos_360": str(OUTPUT_DEBUG_CONAB_360),
        "debug_sorgo_conab": str(OUTPUT_DEBUG_SORGO_CONAB),
        "debug_leite_carne_conab": str(OUTPUT_DEBUG_LEITE_CARNE_CONAB),
        "debug_pecuaria": str(OUTPUT_DEBUG_PECUARIA),
        "diagnostico_sorgo_conab": payload.get("diagnostico_sorgo_conab", {}),
        "diagnostico_leite_carne_conab": payload.get("diagnostico_leite_carne_conab", {}),
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
    cotacoes_brutas.extend(coletar_seagri_ba(status_fontes))
    cotacoes_brutas.extend(coletar_pecuaria_leite_boi(status_fontes))
    cotacoes_brutas.extend(coletar_conab_produtos_360(status_fontes))
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

    diagnostico_sorgo_conab = diagnosticar_sorgo_conab(
        cotacoes_brutas=cotacoes_brutas,
        cotacoes_validas=cotacoes_validas,
        cotacoes_descartadas_validacao=cotacoes_descartadas_validacao,
        cotacoes_produtor_regional=cotacoes_produtor_regional,
        cotacoes_descartadas_nivel=cotacoes_descartadas_nivel,
        cotacoes_tabela=cotacoes_tabela,
        data_corte_iso=data_corte_iso,
    )
    salvar_debug_sorgo_conab(diagnostico_sorgo_conab)

    diagnostico_leite_carne_conab = diagnosticar_produtos_pecuarios_conab(
        cotacoes_brutas=cotacoes_brutas,
        cotacoes_validas=cotacoes_validas,
        cotacoes_descartadas_validacao=cotacoes_descartadas_validacao,
        cotacoes_produtor_regional=cotacoes_produtor_regional,
        cotacoes_descartadas_nivel=cotacoes_descartadas_nivel,
        cotacoes_tabela=cotacoes_tabela,
        data_corte_iso=data_corte_iso,
    )
    salvar_debug_leite_carne_conab(diagnostico_leite_carne_conab)

    fontes_ok = [f["fonte"] for f in status_fontes if f.get("status") == "ok"]
    fontes_erro = [f for f in status_fontes if f.get("status") == "erro"]

    payload = {
        "ok": True,
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes",
        "repositorio": "idocandido-dotcom/cotacoes",
        "versao": "1.3.4",
        "ultima_sincronizacao": agora_local().strftime("%Y-%m-%d %H:%M:%S"),
        "ultima_sincronizacao_iso": agora_local().isoformat(),
        "gerado_em": agora_local().strftime("%d/%m/%Y %H:%M"),
        "fuso_horario": TIMEZONE,
        "frequencia_atualizacao": "diaria",
        "dias_maximos_cotacao_ativa": DIAS_MAXIMOS_COTACAO_ATIVA,
        "data_limite_cotacoes_ativas": data_corte_iso,
        "politica_atualidade": "A tabela principal exibe somente cotações com data dentro dos últimos 90 dias. No CONAB Produtos 360º, a data exibida é o período semanal publicado, não preço diário.",
        "fonte_principal": "AIBA/CONAB Produtos 360º para soja, milho e algodão; AIBA/SEAGRI-BA/CONAB semanal para feijão e sorgo; CONAB Preços Agropecuários para leite e boi gordo quando o nível for PREÇO RECEBIDO/Produtor; CEPEA/ESALQ e SEAGRI-BA como referências complementares",
        "fontes_complementares": ["CEPEA/ESALQ para Leite e Boi Gordo", "SEAGRI-BA como referência estadual", "ACRIOESTE e Agrolink em diagnóstico"],
        "politica_classificacao_preco": (
            "Política v1.3.3: a tabela principal publica preço pago ao produtor quando a fonte informar, "
            "cotação regional produtiva, referência SEAGRI-BA e referência oficial CONAB para soja, milho, algodão, feijão e sorgo. "
            "Leite e Boi Gordo CEPEA/ESALQ entram como referência de mercado, sem serem tratados como preço local de praça/estado. "
            "Leite e Boi Gordo da CONAB entram quando a fonte informar PREÇO RECEBIDO/Produtor; Boi em R$/kg é convertido para Arroba (@) com fator 15. Carne Bovina permanece bloqueada salvo preço ao produtor claramente identificado. "
            "Varejo, atacado comum, indicadores genéricos e mercado futuro ficam fora da tabela principal. "
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
            "total_referencias_seagri_ba": len([item for item in cotacoes_tabela if item.get("fonte") == "SEAGRI-BA"]),
            "total_pecuaria_leite": len([item for item in cotacoes_tabela if item.get("categoria") == "pecuaria_leite"]),
            "total_pecuaria_corte": len([item for item in cotacoes_tabela if item.get("categoria") == "pecuaria_corte"]),
            "total_cepea_pecuaria": len([item for item in cotacoes_tabela if item.get("fonte") == "CEPEA/ESALQ" and item.get("categoria") in {"pecuaria_leite", "pecuaria_corte"}]),
            "total_referencias_conab": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_referencia_conab"]),
            "total_referencias_conab_360": len([item for item in cotacoes_tabela if item.get("fonte") == "CONAB - Produtos 360º"]),
            "total_precos_atacado": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "preco_atacado"]),
            "total_precos_media_uf": len([item for item in cotacoes_tabela if item.get("nivel_comercializacao_chave") == "media_uf"]),
            "total_cotacoes_leite": len([item for item in cotacoes_tabela if item.get("produto_base") == "Leite"]),
            "total_cotacoes_boi_gordo": len([item for item in cotacoes_tabela if item.get("produto_base") == "Boi Gordo"]),
            "total_cotacoes_carne_bovina": len([item for item in cotacoes_tabela if item.get("produto_base") == "Carne Bovina"]),
            "total_leite_conab_bruto": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Leite", {}).get("totais", {}).get("brutas_conab", 0),
            "total_leite_conab_tabela": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Leite", {}).get("totais", {}).get("na_tabela_final", 0),
            "total_boi_gordo_conab_bruto": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Boi Gordo", {}).get("totais", {}).get("brutas_conab", 0),
            "total_boi_gordo_conab_tabela": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Boi Gordo", {}).get("totais", {}).get("na_tabela_final", 0),
            "total_carne_bovina_conab_bruto": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Carne Bovina", {}).get("totais", {}).get("brutas_conab", 0),
            "total_carne_bovina_conab_tabela": diagnostico_leite_carne_conab.get("diagnosticos", {}).get("Carne Bovina", {}).get("totais", {}).get("na_tabela_final", 0),
            "total_sorgo_conab_tabela": diagnostico_sorgo_conab.get("totais", {}).get("na_tabela_final", 0),
            "total_sorgo_conab_bruto": diagnostico_sorgo_conab.get("totais", {}).get("brutas_sorgo_conab", 0),
            "total_sorgo_conab_descartado_nivel": diagnostico_sorgo_conab.get("totais", {}).get("descartadas_por_nivel", 0),
            "total_sorgo_conab_descartado_validacao": diagnostico_sorgo_conab.get("totais", {}).get("descartadas_por_validacao", 0),
            "total_sorgo_conab_grupos_antigos": diagnostico_sorgo_conab.get("totais", {}).get("grupos_antigos", 0),
            "total_indicadores_mercado": len([item for item in cotacoes_tabela if item.get("categoria") == "indicador_mercado"]),
            "fontes_com_sucesso": fontes_ok,
            "total_fontes_com_erro": len(fontes_erro),
            "tempo_execucao_segundos": round((agora_local() - inicio).total_seconds(), 2),
        },
        "fontes": status_fontes,
        "diagnostico_sorgo_conab": diagnostico_sorgo_conab,
        "diagnostico_leite_carne_conab": diagnostico_leite_carne_conab,
        "debug_sorgo_conab": str(OUTPUT_DEBUG_SORGO_CONAB),
        "debug_leite_carne_conab": str(OUTPUT_DEBUG_LEITE_CARNE_CONAB),
        "debug_pecuaria": str(OUTPUT_DEBUG_PECUARIA),
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
                "cotação regional produtiva, referência oficial CONAB para os produtos principais "
                "e referência CEPEA/ESALQ para Leite e Boi Gordo. "
                "Varejo, atacado, indicadores genéricos e mercado futuro são removidos."
            ),
            "niveis_permitidos": ["preco_produtor", "preco_regional", "preco_referencia_conab", "referencia_cepea_pecuaria"],
            "niveis_bloqueados": ["preco_varejo", "preco_atacado", "indicador_mercado", "mercado_futuro", "nao_informado", "media_uf"],
            "total_descartadas": len(cotacoes_descartadas_nivel),
            "resumo_descartes": resumir_descartes(cotacoes_descartadas_nivel),
            "amostra_descartadas": cotacoes_descartadas_nivel[:50],
        },
        "aviso_legal": (
            "As cotações apresentadas pelo Nordeste Agro são referenciais e compiladas "
            "a partir de fontes regionais, oficiais e indicadores de mercado. A partir da versão v1.3.3, "
            "a tabela principal publica preço pago ao produtor quando a fonte informar claramente, "
            "cotação regional produtiva, referência SEAGRI-BA, referência oficial CONAB para soja, milho, algodão, feijão e sorgo, "
            "e referência CEPEA/ESALQ para Leite e Boi Gordo. "
            "Leite, Boi Gordo e Carne Bovina da CONAB só são publicados quando a fonte indicar preço ao produtor. "
            "Cotações de varejo e atacado são removidas para evitar confusão com preço recebido "
            "pelo produtor rural. Soja, milho, sorgo, arroz e feijão são padronizados em preço por Saca 60 kg "
            "quando a fonte vier em Kg. Produtos derivados, industrializados, insumos e serviços são removidos. "
            "CEPEA/ESALQ pode ser publicado como referência institucional para leite e boi gordo, e B3 permanece apenas como "
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
    print(f"Total SEAGRI-BA: {len([item for item in cotacoes_tabela if item.get('fonte') == 'SEAGRI-BA'])}")
    print(f"Total Preço Atacado: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'preco_atacado'])}")
    print(f"Total Média UF: {len([item for item in cotacoes_tabela if item.get('nivel_comercializacao_chave') == 'media_uf'])}")
    print(f"Total Leite: {len([item for item in cotacoes_tabela if item.get('produto_base') == 'Leite'])}")
    print(f"Total Boi Gordo: {len([item for item in cotacoes_tabela if item.get('produto_base') == 'Boi Gordo'])}")
    print(f"Total Carne Bovina: {len([item for item in cotacoes_tabela if item.get('produto_base') == 'Carne Bovina'])}")
    print(f"JSON principal: {OUTPUT_JSON}")
    print(f"JSON regional: {OUTPUT_JSON_REGIONAL}")
    print(f"CSV: {OUTPUT_CSV}")
    print(f"LOG: {OUTPUT_LOG}")
    print(f"DEBUG CONAB 360: {OUTPUT_DEBUG_CONAB_360}")
    print(f"DEBUG SORGO CONAB: {OUTPUT_DEBUG_SORGO_CONAB}")
    print(f"DEBUG LEITE/CARNE CONAB: {OUTPUT_DEBUG_LEITE_CARNE_CONAB}")
    print(f"Diagnóstico Sorgo CONAB: {diagnostico_sorgo_conab.get('conclusao')}")
    for produto_diag, diag in diagnostico_leite_carne_conab.get("diagnosticos", {}).items():
        print(f"Diagnóstico {produto_diag} CONAB: {diag.get('conclusao')}")

    if fontes_erro:
        print("Atenção: algumas fontes apresentaram erro:")
        for fonte in fontes_erro:
            print(f"- {fonte.get('fonte')}: {fonte.get('erro')}")


if __name__ == "__main__":
    main()
