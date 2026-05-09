#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Nordeste Agro — Coletor Automático de Cotações v1.5.1 enxuto

Objetivo:
- Manter o mesmo nome do arquivo principal do projeto:
  cotacoes/scripts/coletor_cotacoes_nordeste.py

Fontes principais:
- CONAB Produtos 360º:
  Soja, Milho e Algodão.
- CONAB SIAGRO / Preço Médio UF:
  Sorgo, Arroz, Feijão, Boi Gordo e Leite.
- CONAB Preços Agropecuários Semanal UF/Município:
  fallback somente quando o SIAGRO não retornar dado válido.
- AIBA:
  referência regional complementar.
- Regionais anteriores:
  preservados quando a fonte regional falhar.

Conversões:
- Soja/Milho já vêm do Produtos 360º em Saca 60 kg.
- Algodão vem do Produtos 360º em Arroba (@).
- Sorgo/Arroz/Feijão no SIAGRO: R$/kg x 60 = R$/Saca 60 kg.
- Boi Gordo no SIAGRO: R$/kg x 15 = R$/@.
- Leite no SIAGRO: R$/litro, sem conversão.

Não publica CEPEA na tabela principal.
"""

import csv
import io
import json
import math
import os
import re
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


# =============================================================================
# Configuração geral
# =============================================================================

VERSAO = "1.5.1"
PROJETO = "Nordeste Agro"
MODULO = "cotacoes"
TZ = ZoneInfo("America/Fortaleza")

ROOT_DIR = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path(".")
PUBLIC_DIR = ROOT_DIR / "public"
LOGS_DIR = ROOT_DIR / "logs"

OUTPUT_JSON = PUBLIC_DIR / "cotacoes_nordeste.json"
OUTPUT_JSON_REGIONAL = PUBLIC_DIR / "cotacoes_regionais.json"
OUTPUT_CSV = PUBLIC_DIR / "cotacoes_nordeste.csv"
OUTPUT_STATUS = LOGS_DIR / "status_ultima_execucao.json"
OUTPUT_DEBUG_CONAB360 = LOGS_DIR / "debug_conab_produtos_360.json"
OUTPUT_DEBUG_SIAGRO = LOGS_DIR / "debug_sorgo_conab.json"
OUTPUT_DEBUG_PRIORIDADE = LOGS_DIR / "debug_prioridade_siagro.json"

DIAS_MAXIMOS_COTACAO_ATIVA = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
}

CONAB_PRODUTOS_360_URL = "https://portaldeinformacoes.conab.gov.br/produtos-360.html"
CONAB_PRODUTOS_360_WCDF_URL = (
    "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/api/repos/"
    "%3Ahome%3AProdutos%3Aprodutos360.wcdf/generatedContent?userid=pentaho&password=password"
)
CONAB_360_DOQUERY_URL = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery?"
CONAB_360_CDA_PATH = "/home/Produtos/produtos360.cda"

CONAB_PRECOS_AGROPECUARIOS_URL = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios.html"
CONAB_SIAGRO_WCDF_URL = (
    "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/api/repos/"
    "%3Ahome%3ASIAGRO%3APrecoMedio.wcdf/generatedContent?userid=pentaho&password=password"
)
CONAB_SIAGRO_DOQUERY_URL = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery?"
CONAB_SIAGRO_CDA_PATH = "/home/SIAGRO/PrecoMedio.cda"

CONAB_SEMANAL_UF_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt"
CONAB_SEMANAL_MUNICIPIO_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalMunicipio.txt"

AIBA_URL = "https://aiba.org.br/cotacoes/"

UFS_BRASIL = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas", "BA": "Bahia",
    "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco", "PI": "Piauí",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul",
    "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina", "SP": "São Paulo",
    "SE": "Sergipe", "TO": "Tocantins",
}
UFS_NORDESTE_AMPLIADO = {"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "PA", "TO"}

PRODUTOS_360 = {
    "Soja": {
        "param": "[Produto].[SOJA]",
        "produto_original": "Soja",
        "unidade": "Saca 60 kg",
        "categoria": "commodity_agricola",
    },
    "Milho": {
        "param": "[Produto].[MILHO]",
        "produto_original": "Milho",
        "unidade": "Saca 60 kg",
        "categoria": "commodity_agricola",
    },
    "Algodão": {
        "param": "[Produto].[ALGODÃO EM PLUMA]",
        "produto_original": "Algodão em Pluma",
        "unidade": "Arroba (@)",
        "categoria": "commodity_agricola",
    },
}

PRODUTOS_SIAGRO = ["Sorgo", "Arroz", "Feijão", "Boi Gordo", "Leite"]


# =============================================================================
# Utilitários de texto, data e preço
# =============================================================================

def agora_local() -> datetime:
    return datetime.now(TZ).replace(microsecond=0)


def limpar_texto(valor: Any) -> str:
    if valor is None:
        return ""
    return re.sub(r"\s+", " ", str(valor)).strip()


def remover_acentos(valor: Any) -> str:
    texto = unicodedata.normalize("NFD", limpar_texto(valor))
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def slugify(valor: Any) -> str:
    texto = remover_acentos(valor).lower()
    texto = re.sub(r"[^a-z0-9]+", "-", texto)
    return texto.strip("-")


def data_para_br(data_iso: Any) -> str:
    texto = limpar_texto(data_iso)
    if not texto:
        return ""
    try:
        return datetime.strptime(texto[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return texto


def parse_data_qualquer(valor: Any) -> Optional[str]:
    texto = limpar_texto(valor)
    if not texto:
        return None

    texto = texto.replace(".", "/").replace("-", "/")
    formatos = ["%Y/%m/%d", "%d/%m/%Y", "%d/%m/%y"]

    for formato in formatos:
        try:
            return datetime.strptime(texto[:10], formato).date().isoformat()
        except Exception:
            pass

    return None


def parse_preco(valor: Any) -> Optional[float]:
    if valor is None:
        return None
    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        if math.isfinite(float(valor)):
            return float(valor)
        return None

    texto = limpar_texto(valor)
    if not texto:
        return None

    texto = texto.replace("R$", "").replace("\xa0", " ").strip()
    texto = re.sub(r"[^0-9,.\-]", "", texto)

    if not texto or texto in {"-", ",", "."}:
        return None

    # pt-BR: 1.234,56
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")

    try:
        numero = float(texto)
    except Exception:
        return None

    if not math.isfinite(numero):
        return None
    return numero


def extrair_periodo_semanal(texto: Any) -> dict[str, Optional[str]]:
    """
    Extrai períodos como:
    - 27-04-2026 - 01-05-2026
    - 27/04/2026 a 01/05/2026
    """
    bruto = limpar_texto(texto)
    if not bruto:
        return {"data_inicio": None, "data_fim": None, "periodo_referencia": None}

    padrao = re.search(
        r"(\d{2})[/-](\d{2})[/-](\d{4})\s*(?:-|a|até|ate)\s*(\d{2})[/-](\d{2})[/-](\d{4})",
        bruto,
        flags=re.I,
    )
    if not padrao:
        return {"data_inicio": None, "data_fim": None, "periodo_referencia": None}

    d1, m1, a1, d2, m2, a2 = padrao.groups()
    inicio = f"{a1}-{m1}-{d1}"
    fim = f"{a2}-{m2}-{d2}"
    periodo = f"{d1}/{m1}/{a1} a {d2}/{m2}/{a2}"
    return {"data_inicio": inicio, "data_fim": fim, "periodo_referencia": periodo}


def semana_monday_friday(base_date: date) -> tuple[date, date]:
    segunda = base_date - timedelta(days=base_date.weekday())
    sexta = segunda + timedelta(days=4)
    return segunda, sexta


def contextos_semanais_siagro(total: int = 6) -> list[dict[str, str]]:
    """
    Gera semanas completas no padrão usado pelo painel SIAGRO.

    Ordem:
    - semana anterior completa;
    - semana atual;
    - semanas anteriores.
    """
    hoje = agora_local().date()
    semana_atual_ini, semana_atual_fim = semana_monday_friday(hoje)
    primeira_ini = semana_atual_ini - timedelta(days=7)

    contextos = []

    for i in range(total):
        ini = primeira_ini - timedelta(days=7 * i)
        fim = ini + timedelta(days=4)
        periodo = f"{ini.strftime('%d-%m-%Y')} - {fim.strftime('%d-%m-%Y')}"
        periodo_br = f"{ini.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"
        membro = f"[Semana].[Data Inicial Final].[{periodo}]"
        expr = f"Exists({{[Semana].[Data Inicial Final].Members}}, {{[Semana].[{periodo}]: [Semana].[{periodo}]}})"

        contextos.append(
            {
                "periodo": periodo,
                "periodo_br": periodo_br,
                "data_inicio_iso": ini.isoformat(),
                "data_fim_iso": fim.isoformat(),
                "data_ref_iso": fim.isoformat(),
                "data_ref_br": fim.strftime("%d/%m/%Y"),
                "mesAnoUF": membro,
                "linhaRegiao": expr,
                "colunaMunicipioSemanal": expr,
                "semana_data_inicial": periodo,
                "semana_data_final": periodo,
            }
        )

    # Inclui semana atual como segunda tentativa, se diferente.
    periodo_atual = f"{semana_atual_ini.strftime('%d-%m-%Y')} - {semana_atual_fim.strftime('%d-%m-%Y')}"
    contexto_atual = {
        "periodo": periodo_atual,
        "periodo_br": f"{semana_atual_ini.strftime('%d/%m/%Y')} a {semana_atual_fim.strftime('%d/%m/%Y')}",
        "data_inicio_iso": semana_atual_ini.isoformat(),
        "data_fim_iso": semana_atual_fim.isoformat(),
        "data_ref_iso": semana_atual_fim.isoformat(),
        "data_ref_br": semana_atual_fim.strftime("%d/%m/%Y"),
        "mesAnoUF": f"[Semana].[Data Inicial Final].[{periodo_atual}]",
        "linhaRegiao": f"Exists({{[Semana].[Data Inicial Final].Members}}, {{[Semana].[{periodo_atual}]: [Semana].[{periodo_atual}]}})",
        "colunaMunicipioSemanal": f"Exists({{[Semana].[Data Inicial Final].Members}}, {{[Semana].[{periodo_atual}]: [Semana].[{periodo_atual}]}})",
        "semana_data_inicial": periodo_atual,
        "semana_data_final": periodo_atual,
    }
    contextos.insert(1, contexto_atual)

    vistos = set()
    unicos = []
    for c in contextos:
        if c["periodo"] not in vistos:
            unicos.append(c)
            vistos.add(c["periodo"])

    return unicos[:total]


def periodo_e_datas_de_linha(row: list[Any], contexto: Optional[dict[str, Any]] = None) -> tuple[str, str, str]:
    texto_linha = " ".join(limpar_texto(x) for x in row)
    periodo = extrair_periodo_semanal(texto_linha)

    if not periodo.get("periodo_referencia") and contexto:
        periodo = {
            "data_inicio": contexto.get("data_inicio_iso"),
            "data_fim": contexto.get("data_fim_iso"),
            "periodo_referencia": contexto.get("periodo_br"),
        }

    data_inicio = periodo.get("data_inicio") or agora_local().date().isoformat()
    data_fim = periodo.get("data_fim") or data_inicio
    periodo_ref = periodo.get("periodo_referencia") or f"Semana de {data_para_br(data_fim)}"
    return data_inicio, data_fim, periodo_ref


# =============================================================================
# Produto, unidade e item final
# =============================================================================

def normalizar_produto_base(valor: Any) -> str:
    t = remover_acentos(valor).lower()

    if "soja" in t:
        return "Soja"
    if "milho" in t:
        return "Milho"
    if "algod" in t:
        return "Algodão"
    if "sorgo" in t:
        return "Sorgo"
    if "arroz" in t:
        return "Arroz"
    if "feij" in t:
        return "Feijão"
    if "boi" in t or "bovino" in t:
        return "Boi Gordo"
    if "leite" in t:
        return "Leite"
    return limpar_texto(valor).title()


def categoria_produto(produto_base: str) -> str:
    if produto_base == "Leite":
        return "pecuaria_leite"
    if produto_base == "Boi Gordo":
        return "pecuaria_corte"
    return "commodity_agricola"


def nome_produto(produto_base: str, nivel: str = "Produtor") -> str:
    if produto_base in {"Leite", "Boi Gordo", "Sorgo", "Arroz", "Feijão"}:
        return f"{produto_base} — {nivel}"
    return produto_base


def formatar_preco(valor: Optional[float], unidade: str) -> str:
    if valor is None:
        return ""
    return f"R$ {valor:,.2f}/{unidade}".replace(",", "X").replace(".", ",").replace("X", ".")


def converter_preco(produto_base: str, preco_original: float, unidade_original: str) -> tuple[float, str, float, bool]:
    """
    Converte apenas quando o produto vem em R$/kg no SIAGRO/CONAB semanal.
    """
    unidade_norm = remover_acentos(unidade_original).lower()

    if produto_base in {"Sorgo", "Arroz", "Feijão"} and ("kg" in unidade_norm or unidade_norm in {"", "quilo"}):
        return round(preco_original * 60, 2), "Saca 60 kg", 60.0, True

    if produto_base == "Boi Gordo" and ("kg" in unidade_norm or unidade_norm in {"", "quilo"}):
        return round(preco_original * 15, 2), "Arroba (@)", 15.0, True

    if produto_base == "Leite":
        return round(preco_original, 2), "Litro", 1.0, False

    return round(preco_original, 2), limpar_texto(unidade_original) or "Unidade", 1.0, False


def criar_item(
    *,
    produto_original: str,
    produto_base: str,
    uf: str,
    estado: str,
    praca: str,
    unidade_original: str,
    preco_original: float,
    data_referencia: str,
    data_inicio: Optional[str],
    data_fim: Optional[str],
    periodo_referencia: Optional[str],
    fonte: str,
    fonte_url: str,
    tipo_fonte: str,
    nivel: str = "Produtor",
    variacao_percentual: Optional[float] = None,
    categoria: Optional[str] = None,
    converter: bool = True,
    observacao: str = "",
) -> dict[str, Any]:
    if converter:
        preco, unidade, fator, convertida = converter_preco(produto_base, preco_original, unidade_original)
    else:
        preco = round(preco_original, 2)
        unidade = limpar_texto(unidade_original) or "Unidade"
        fator = 1.0
        convertida = False

    nivel_chave = "preco_produtor" if "produtor" in remover_acentos(nivel).lower() else slugify(nivel)

    return {
        "produto": nome_produto(produto_base, nivel) if produto_base in PRODUTOS_SIAGRO else produto_base,
        "produto_base": produto_base,
        "produto_original": limpar_texto(produto_original),
        "tipo_produto": "principal",
        "produto_slug": slugify(f"{produto_base}-{nivel}"),
        "uf": uf,
        "estado": estado,
        "praca": limpar_texto(praca),
        "unidade": unidade,
        "unidade_original": limpar_texto(unidade_original) or unidade,
        "preco": preco,
        "preco_original": round(preco_original, 6),
        "preco_formatado": formatar_preco(preco, unidade),
        "fator_conversao": fator,
        "conversao_aplicada": convertida,
        "moeda": "BRL",
        "variacao_percentual": variacao_percentual,
        "data_referencia": data_referencia,
        "data_referencia_inicio": data_inicio or data_referencia,
        "data_referencia_fim": data_fim or data_referencia,
        "periodo_referencia": periodo_referencia or data_para_br(data_referencia),
        "fonte": fonte,
        "fonte_url": fonte_url,
        "tipo": tipo_fonte,
        "nivel_comercializacao": nivel,
        "nivel_comercializacao_chave": nivel_chave,
        "prioridade_nivel_preco": 1 if nivel_chave == "preco_produtor" else 5,
        "categoria": categoria or categoria_produto(produto_base),
        "observacao": limpar_texto(observacao),
        "historico_30_dias": [],
    }


def item_para_html(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "estado": item.get("uf") or "",
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


# =============================================================================
# HTTP e CDA
# =============================================================================

def resposta_json(resp: requests.Response) -> Optional[dict[str, Any]]:
    try:
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, dict) and isinstance(data.get("resultset"), list):
            return data
    except Exception:
        return None
    return None


def post_cda(session: requests.Session, url: str, params: dict[str, str], timeout: int = 30) -> Optional[dict[str, Any]]:
    try:
        resp = session.post(url, data=params, timeout=timeout)
    except Exception:
        return None
    return resposta_json(resp)


def abrir_sessao(session: requests.Session, url: str, timeout: int = 30) -> str:
    try:
        resp = session.get(url, timeout=timeout)
        return f"http_{resp.status_code}"
    except Exception as erro:
        return f"erro: {repr(erro)}"


# =============================================================================
# CONAB Produtos 360º
# =============================================================================

def coletar_conab_360(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    itens: list[dict[str, Any]] = []
    debug: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update(HEADERS)

    html_status = abrir_sessao(session, CONAB_PRODUTOS_360_WCDF_URL)

    for produto_base, cfg in PRODUTOS_360.items():
        params = {
            "path": CONAB_360_CDA_PATH,
            "dataAccessId": "precoProduto",
            "outputIndexId": "1",
            "pageSize": "0",
            "pageStart": "0",
            "paramsearchBox": "",
            "paramprodutoPreco": cfg["param"],
        }

        data = post_cda(session, CONAB_360_DOQUERY_URL, params)
        resultset = data.get("resultset", []) if data else []
        extraidos = 0

        for row in resultset:
            if not isinstance(row, list) or len(row) < 2:
                continue

            uf = limpar_texto(row[0]).upper()
            if uf not in UFS_NORDESTE_AMPLIADO:
                continue

            preco = parse_preco(row[1])
            if preco is None:
                continue

            variacao = parse_preco(row[2]) if len(row) > 2 else None
            periodo_texto = limpar_texto(row[-1]) if row else ""
            periodo = extrair_periodo_semanal(periodo_texto)
            data_fim = periodo.get("data_fim") or agora_local().date().isoformat()
            data_inicio = periodo.get("data_inicio") or data_fim
            periodo_ref = periodo.get("periodo_referencia") or periodo_texto or data_para_br(data_fim)

            itens.append(
                criar_item(
                    produto_original=cfg["produto_original"],
                    produto_base=produto_base,
                    uf=uf,
                    estado=UFS_BRASIL.get(uf, uf),
                    praca=f"{UFS_BRASIL.get(uf, uf)} - Média estadual CONAB",
                    unidade_original=cfg["unidade"],
                    preco_original=preco,
                    data_referencia=data_fim,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                    periodo_referencia=periodo_ref,
                    fonte="CONAB - Produtos 360º",
                    fonte_url=CONAB_PRODUTOS_360_URL,
                    tipo_fonte="oficial",
                    nivel="Produtor",
                    variacao_percentual=variacao,
                    categoria=cfg["categoria"],
                    converter=False,
                    observacao=(
                        f"{produto_base} coletado do CONAB Produtos 360º. "
                        "Fonte principal para Soja, Milho e Algodão."
                    ),
                )
            )
            extraidos += 1

        debug.append(
            {
                "produto": produto_base,
                "param": cfg["param"],
                "resultset": len(resultset),
                "extraidos": extraidos,
            }
        )

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_CONAB360.write_text(
        json.dumps(
            {
                "fonte": "CONAB - Produtos 360º",
                "versao": VERSAO,
                "html_status": html_status,
                "total_itens": len(itens),
                "produtos": debug,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    status_fontes.append(
        {
            "fonte": "CONAB - Produtos 360º",
            "url": CONAB_PRODUTOS_360_URL,
            "status": "ok" if itens else "sem_registros_extraidos",
            "total_registros": len(itens),
            "produtos_extraidos": sorted({i["produto_base"] for i in itens}),
            "observacao": "Fonte principal para Soja, Milho e Algodão.",
        }
    )

    return itens


# =============================================================================
# CONAB SIAGRO / Preço Médio UF
# =============================================================================

def valores_produto_siagro(produto_base: str) -> list[str]:
    mapa = {
        "Sorgo": ["SORGO GRANIFERO", "[Produto].[SORGO GRANIFERO]"],
        "Arroz": ["ARROZ", "ARROZ EM CASCA", "[Produto].[ARROZ]"],
        "Feijão": ["FEIJAO", "FEIJÃO", "FEIJAO CARIOCA"],
        "Boi Gordo": ["BOI GORDO", "BOVINO"],
        "Leite": ["LEITE", "LEITE DE VACA"],
    }
    return mapa.get(produto_base, [produto_base])


def classificacoes_siagro(produto_base: str) -> list[str]:
    if produto_base in {"Sorgo", "Arroz", "Feijão"}:
        return ["EM GRÃOS"]
    if produto_base == "Boi Gordo":
        return ["BOI GORDO", ""]
    if produto_base == "Leite":
        return ["LEITE", ""]
    return [""]


def montar_params_siagro(produto_valor: str, classificacao: str, contexto: dict[str, str]) -> dict[str, str]:
    produto_sem_mdx = re.sub(r"^\[Produto\]\.\[|\]$", "", limpar_texto(produto_valor)).strip()
    nivel = "PREÇO RECEBIDO P/ PRODUTOR"

    return {
        "path": CONAB_SIAGRO_CDA_PATH,
        "dataAccessId": "RankingPrecoMedioUF",
        "outputIndexId": "1",
        "pageSize": "0",
        "pageStart": "0",

        "paramproduto": produto_sem_mdx,
        "paramprodutoFormatado": produto_sem_mdx,
        "paramnivelComercializacao": nivel,
        "paramnivelComercializacaoFormatado": nivel,
        "paramclassificacao": classificacao,
        "paramclassificacaoData": classificacao,
        "paramtipoVisao": "SEMANAL",
        "parammesAnoUF": contexto["mesAnoUF"],
        "paramlinhaRegiao": contexto["linhaRegiao"],
        "paramcolunaRegiao": "{{[UF].[All UFs]}, {[UF].[Regiao].Members}}",
        "paramufMunicipio": "[UF].[All UFs]",
        "paramregiaoUf": "All UFs",
        "paramcolunaMunicipioSemanal": contexto["colunaMunicipioSemanal"],
        "paramdata_semana": contexto["data_ref_br"],
        "paramsemana_data_inicial": contexto["semana_data_inicial"],
        "paramsemana_data_final": contexto["semana_data_final"],

        "produto": produto_sem_mdx,
        "nivelComercializacao": nivel,
        "classificacao": classificacao,
        "tipoVisao": "SEMANAL",
        "mesAnoUF": contexto["mesAnoUF"],
    }


def uf_da_linha(row: list[Any]) -> Optional[str]:
    for valor in row:
        texto = limpar_texto(valor).upper()
        if texto in UFS_BRASIL:
            return texto
    return None


def preco_siagro_da_linha(row: list[Any], produto_base: str) -> Optional[float]:
    faixas = {
        "Sorgo": (0.05, 10.0),
        "Arroz": (0.05, 30.0),
        "Feijão": (0.05, 30.0),
        "Boi Gordo": (1.0, 80.0),
        "Leite": (0.1, 10.0),
    }
    minimo, maximo = faixas.get(produto_base, (0.01, 999999.0))

    for valor in row:
        preco = parse_preco(valor)
        if preco is not None and minimo <= preco <= maximo:
            return preco
    return None


def produto_original_siagro(produto_base: str) -> str:
    return {
        "Sorgo": "Sorgo Granífero - Em Grãos",
        "Arroz": "Arroz - Em Grãos",
        "Feijão": "Feijão - Em Grãos",
        "Boi Gordo": "Boi Gordo",
        "Leite": "Leite",
    }.get(produto_base, produto_base)


def unidade_original_siagro(produto_base: str) -> str:
    if produto_base == "Leite":
        return "Litro"
    return "Kg"


def extrair_itens_siagro(data: dict[str, Any], produto_base: str, contexto: dict[str, str]) -> list[dict[str, Any]]:
    itens: list[dict[str, Any]] = []

    for row in data.get("resultset", []):
        if not isinstance(row, list):
            continue

        uf = uf_da_linha(row)
        if not uf:
            continue

        preco_original = preco_siagro_da_linha(row, produto_base)
        if preco_original is None:
            continue

        data_inicio, data_fim, periodo_ref = periodo_e_datas_de_linha(row, contexto)

        if produto_base == "Boi Gordo":
            obs_unidade = "Valor original em R$/kg convertido para Arroba (@), com fator de 15 kg."
        elif produto_base == "Leite":
            obs_unidade = "Valor publicado em R$/litro."
        else:
            obs_unidade = "Valor original em R$/kg convertido para Saca 60 kg."

        itens.append(
            criar_item(
                produto_original=produto_original_siagro(produto_base),
                produto_base=produto_base,
                uf=uf,
                estado=UFS_BRASIL.get(uf, uf),
                praca=f"{UFS_BRASIL.get(uf, uf)} - Média estadual CONAB",
                unidade_original=unidade_original_siagro(produto_base),
                preco_original=preco_original,
                data_referencia=data_fim,
                data_inicio=data_inicio,
                data_fim=data_fim,
                periodo_referencia=periodo_ref,
                fonte="CONAB - Preços Agropecuários Painel SIAGRO",
                fonte_url=CONAB_PRECOS_AGROPECUARIOS_URL,
                tipo_fonte="oficial",
                nivel="Produtor",
                converter=True,
                observacao=(
                    f"{produto_original_siagro(produto_base)} coletado no painel CONAB Preços Agropecuários "
                    f"via CDA /home/SIAGRO/PrecoMedio.cda / RankingPrecoMedioUF. {obs_unidade}"
                ),
            )
        )

    return itens


def coletar_siagro(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    itens: list[dict[str, Any]] = []
    debug: list[dict[str, Any]] = []

    session = requests.Session()
    session.headers.update(HEADERS)
    html_status = abrir_sessao(session, CONAB_SIAGRO_WCDF_URL, timeout=30)

    for produto_base in PRODUTOS_SIAGRO:
        encontrados: list[dict[str, Any]] = []

        for contexto in contextos_semanais_siagro(total=6):
            if encontrados:
                break

            for produto_valor in valores_produto_siagro(produto_base):
                if encontrados:
                    break

                for classificacao in classificacoes_siagro(produto_base):
                    params = montar_params_siagro(produto_valor, classificacao, contexto)
                    data = post_cda(session, CONAB_SIAGRO_DOQUERY_URL, params, timeout=30)

                    if data is None:
                        if len(debug) < 120:
                            debug.append(
                                {
                                    "produto": produto_base,
                                    "produto_valor": produto_valor,
                                    "classificacao": classificacao,
                                    "periodo": contexto["periodo_br"],
                                    "status": "sem_json_resultset",
                                }
                            )
                        continue

                    candidatos = extrair_itens_siagro(data, produto_base, contexto)
                    debug.append(
                        {
                            "produto": produto_base,
                            "produto_valor": produto_valor,
                            "classificacao": classificacao,
                            "periodo": contexto["periodo_br"],
                            "resultset": len(data.get("resultset", [])),
                            "itens_extraidos": len(candidatos),
                            "metadata": [
                                m.get("colName")
                                for m in data.get("metadata", [])
                                if isinstance(m, dict)
                            ][:12],
                        }
                    )

                    if candidatos:
                        encontrados.extend(candidatos)
                        break

        itens.extend(encontrados)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DEBUG_SIAGRO.write_text(
        json.dumps(
            {
                "fonte": "CONAB - Preços Agropecuários Painel SIAGRO",
                "versao": VERSAO,
                "modo": "enxuto",
                "html_status": html_status,
                "total_itens_extraidos": len(itens),
                "total_sorgo_extraido": len([x for x in itens if x.get("produto_base") == "Sorgo"]),
                "total_arroz_extraido": len([x for x in itens if x.get("produto_base") == "Arroz"]),
                "total_feijao_extraido": len([x for x in itens if x.get("produto_base") == "Feijão"]),
                "total_boi_gordo_extraido": len([x for x in itens if x.get("produto_base") == "Boi Gordo"]),
                "total_leite_extraido": len([x for x in itens if x.get("produto_base") == "Leite"]),
                "tentativas": debug[:120],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    status_fontes.append(
        {
            "fonte": "CONAB - Preços Agropecuários Painel SIAGRO",
            "url": CONAB_SIAGRO_WCDF_URL,
            "status": "ok" if itens else "sem_registros_extraidos",
            "total_registros": len(itens),
            "produtos_monitorados": ["Sorgo Granífero", "Arroz", "Feijão", "Boi Gordo", "Leite"],
            "observacao": (
                "v1.5.1 enxuta: usa RankingPrecoMedioUF por POST. "
                "Grãos em Saca 60 kg, Boi em @, Leite em litro."
            ),
        }
    )

    return itens


# =============================================================================
# CONAB semanal fallback
# =============================================================================

def produto_deve_entrar(produto: str) -> bool:
    base = normalizar_produto_base(produto)
    return base in {"Sorgo", "Arroz", "Feijão", "Boi Gordo", "Leite"}


def nivel_produtor(texto: str) -> bool:
    t = remover_acentos(texto).lower()
    if "atacado" in t or "varejo" in t:
        return False
    return "produtor" in t or "recebido" in t or "preco recebido" in t


def detectar_coluna(colunas: list[str], termos: list[str]) -> Optional[str]:
    for col in colunas:
        cn = remover_acentos(col).lower()
        if all(t in cn for t in termos):
            return col
    return None


def ler_csv_conab_texto(texto: str) -> list[dict[str, str]]:
    primeira = texto.splitlines()[0] if texto.splitlines() else ""
    sep = ";" if primeira.count(";") >= primeira.count("\t") else "\t"
    return list(csv.DictReader(io.StringIO(texto), delimiter=sep))


def baixar_texto(url: str, timeout: int = 30) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    # CONAB pode vir como latin-1 ou utf-8
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = "utf-8"
    return resp.text


def coletar_conab_semanal_fallback(
    produtos_necessarios: set[str],
    status_fontes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not produtos_necessarios:
        return []

    itens: list[dict[str, Any]] = []

    fontes = [
        ("CONAB - Preços Agropecuários Semanal UF", CONAB_SEMANAL_UF_URL, "Média UF"),
        ("CONAB - Preços Agropecuários Semanal Município", CONAB_SEMANAL_MUNICIPIO_URL, None),
    ]

    for nome, url, praca_padrao in fontes:
        try:
            texto = baixar_texto(url, timeout=30)
            registros = ler_csv_conab_texto(texto)
            if not registros:
                raise RuntimeError("arquivo sem registros")

            colunas = list(registros[0].keys())
            col_produto = detectar_coluna(colunas, ["produto"]) or "produto"
            col_uf = detectar_coluna(colunas, ["uf"]) or detectar_coluna(colunas, ["sigla"])
            col_preco = (
                detectar_coluna(colunas, ["valor", "kg"])
                or detectar_coluna(colunas, ["preco"])
                or detectar_coluna(colunas, ["valor"])
            )
            col_nivel = detectar_coluna(colunas, ["nivel"]) or detectar_coluna(colunas, ["comercializacao"])
            col_data = detectar_coluna(colunas, ["data"]) or detectar_coluna(colunas, ["dt"])
            col_praca = detectar_coluna(colunas, ["municipio"]) or detectar_coluna(colunas, ["praca"])

            if not col_produto or not col_uf or not col_preco:
                raise RuntimeError(f"colunas essenciais não encontradas: {colunas[:20]}")

            total = 0
            for row in registros:
                produto_original = limpar_texto(row.get(col_produto, ""))
                produto_base = normalizar_produto_base(produto_original)

                if produto_base not in produtos_necessarios:
                    continue

                uf = limpar_texto(row.get(col_uf, "")).upper()
                if uf not in UFS_BRASIL:
                    continue

                nivel = limpar_texto(row.get(col_nivel, "")) if col_nivel else "Preço Recebido pelo Produtor"
                if not nivel_produtor(nivel):
                    continue

                preco = parse_preco(row.get(col_preco))
                if preco is None:
                    continue

                data_iso = parse_data_qualquer(row.get(col_data)) if col_data else None
                data_iso = data_iso or agora_local().date().isoformat()
                praca = limpar_texto(row.get(col_praca, "")) if col_praca else ""
                praca = praca or praca_padrao or f"{UFS_BRASIL.get(uf, uf)} - Média estadual CONAB"

                itens.append(
                    criar_item(
                        produto_original=produto_original,
                        produto_base=produto_base,
                        uf=uf,
                        estado=UFS_BRASIL.get(uf, uf),
                        praca=praca,
                        unidade_original="Kg" if produto_base != "Leite" else "Litro",
                        preco_original=preco,
                        data_referencia=data_iso,
                        data_inicio=data_iso,
                        data_fim=data_iso,
                        periodo_referencia=data_para_br(data_iso),
                        fonte=nome,
                        fonte_url=url,
                        tipo_fonte="oficial",
                        nivel="Produtor",
                        converter=True,
                        observacao="Fallback CONAB Semanal usado porque o SIAGRO não retornou dado válido para este produto.",
                    )
                )
                total += 1

            status_fontes.append(
                {
                    "fonte": nome,
                    "url": url,
                    "status": "ok",
                    "total_registros": total,
                    "produtos_fallback": sorted(produtos_necessarios),
                    "observacao": "Fallback para Arroz, Feijão, Sorgo, Boi Gordo e Leite quando SIAGRO falhar.",
                }
            )

        except Exception as erro:
            status_fontes.append(
                {
                    "fonte": nome,
                    "url": url,
                    "status": "erro",
                    "total_registros": 0,
                    "erro": repr(erro),
                }
            )

    return itens


# =============================================================================
# Regionais
# =============================================================================

def coletar_aiba(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    itens: list[dict[str, Any]] = []

    try:
        html = baixar_texto(AIBA_URL, timeout=30)
        soup = BeautifulSoup(html, "html.parser")
        linhas = [limpar_texto(x) for x in soup.get_text("\n", strip=True).splitlines()]
        linhas = [x for x in linhas if x]

        for i in range(0, max(0, len(linhas) - 3)):
            produto_original = linhas[i]
            unidade = linhas[i + 1]
            preco_texto = linhas[i + 2]
            detalhe = linhas[i + 3]

            if "R$" not in preco_texto:
                continue

            produto_base = normalizar_produto_base(produto_original)
            if produto_base not in {"Soja", "Milho", "Algodão", "Arroz", "Feijão", "Sorgo"}:
                continue

            preco = parse_preco(preco_texto)
            if preco is None:
                continue

            data_iso = None
            m = re.search(r"(\d{2}/\d{2}/\d{4})", detalhe)
            if m:
                data_iso = parse_data_qualquer(m.group(1))
            data_iso = data_iso or agora_local().date().isoformat()

            unidade_final = unidade
            converter = True
            if produto_base in {"Soja", "Milho", "Arroz", "Feijão", "Sorgo"} and "saca" in remover_acentos(unidade).lower():
                converter = False
                unidade_final = "Saca 60 kg"
            if produto_base == "Algodão" and ("@" in unidade or "arroba" in remover_acentos(unidade).lower()):
                converter = False
                unidade_final = "Arroba (@)"

            itens.append(
                criar_item(
                    produto_original=produto_original,
                    produto_base=produto_base,
                    uf="BA",
                    estado="Bahia",
                    praca="Oeste da Bahia - AIBA",
                    unidade_original=unidade_final,
                    preco_original=preco,
                    data_referencia=data_iso,
                    data_inicio=data_iso,
                    data_fim=data_iso,
                    periodo_referencia=data_para_br(data_iso),
                    fonte="AIBA",
                    fonte_url=AIBA_URL,
                    tipo_fonte="regional",
                    nivel="Regional",
                    converter=converter,
                    observacao="Cotação regional AIBA preservada como referência complementar.",
                )
            )

        status_fontes.append(
            {
                "fonte": "AIBA",
                "url": AIBA_URL,
                "status": "ok" if itens else "sem_registros_extraidos",
                "total_registros": len(itens),
            }
        )

    except Exception as erro:
        status_fontes.append(
            {
                "fonte": "AIBA",
                "url": AIBA_URL,
                "status": "erro",
                "total_registros": 0,
                "erro": repr(erro),
            }
        )

    return itens


def carregar_regionais_anteriores() -> list[dict[str, Any]]:
    if not OUTPUT_JSON_REGIONAL.exists():
        return []

    try:
        obj = json.loads(OUTPUT_JSON_REGIONAL.read_text(encoding="utf-8"))
    except Exception:
        return []

    dados = obj.get("dados", []) if isinstance(obj, dict) else []
    if not isinstance(dados, list):
        return []

    saida = []
    for item in dados:
        if not isinstance(item, dict):
            continue
        fonte = limpar_texto(item.get("fonte")).upper()
        tipo = limpar_texto(item.get("tipo")).lower()
        if "AIBA" in fonte or "SEAGRI" in fonte or tipo == "regional":
            item = dict(item)
            item["preservado_por_falha_fonte"] = True
            item["observacao_preservacao"] = (
                "Registro regional preservado do JSON anterior. "
                "Será substituído automaticamente quando houver nova cotação válida da fonte regional."
            )
            saida.append(item)
    return saida


# =============================================================================
# Filtro, consolidação e saída
# =============================================================================

def preco_valido(item: dict[str, Any]) -> bool:
    produto = item.get("produto_base")
    preco = item.get("preco")
    if preco is None:
        return False

    faixas = {
        "Soja": (20, 300),
        "Milho": (20, 200),
        "Algodão": (20, 300),
        "Sorgo": (10, 200),
        "Arroz": (20, 400),
        "Feijão": (50, 600),
        "Boi Gordo": (100, 600),
        "Leite": (0.5, 10),
    }
    minimo, maximo = faixas.get(produto, (0.01, 999999))
    return minimo <= float(preco) <= maximo


def dentro_janela(item: dict[str, Any], data_corte_iso: str) -> bool:
    data_item = limpar_texto(item.get("data_referencia"))
    return bool(data_item and data_item >= data_corte_iso)


def chave_item(item: dict[str, Any]) -> tuple[str, ...]:
    return (
        slugify(item.get("fonte")),
        slugify(item.get("produto_base")),
        slugify(item.get("produto_original")),
        slugify(item.get("uf")),
        slugify(item.get("praca")),
        slugify(item.get("unidade")),
        slugify(item.get("nivel_comercializacao_chave")),
    )


def consolidar(itens: list[dict[str, Any]], data_corte_iso: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    validos = []
    descartados_validacao = 0
    descartados_data = 0

    for item in itens:
        if not preco_valido(item):
            descartados_validacao += 1
            continue
        if not dentro_janela(item, data_corte_iso):
            descartados_data += 1
            continue
        validos.append(item)

    grupos: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for item in validos:
        grupos.setdefault(chave_item(item), []).append(item)

    consolidados = []
    for grupo_itens in grupos.values():
        grupo_itens = sorted(grupo_itens, key=lambda x: limpar_texto(x.get("data_referencia")))
        atual = dict(grupo_itens[-1])

        historico = {}
        for item in grupo_itens:
            data_item = limpar_texto(item.get("data_referencia"))
            if data_item:
                historico[data_item] = item.get("preco")

        atual["historico_30_dias"] = [
            {"data": k, "valor": v}
            for k, v in sorted(historico.items())[-30:]
        ]
        consolidados.append(atual)

    consolidados.sort(
        key=lambda x: (
            slugify(x.get("produto_base")),
            slugify(x.get("uf")),
            slugify(x.get("praca")),
            slugify(x.get("fonte")),
        )
    )

    return consolidados, {
        "descartados_validacao": descartados_validacao,
        "descartados_data": descartados_data,
        "grupos": len(grupos),
    }


def mesclar_preservados(novos: list[dict[str, Any]], preservados: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chaves = {chave_item(x) for x in novos}
    saida = list(novos)

    for item in preservados:
        if chave_item(item) not in chaves:
            saida.append(item)
            chaves.add(chave_item(item))

    return saida


def salvar_csv(itens: list[dict[str, Any]]) -> None:
    campos = [
        "produto", "produto_base", "produto_original", "uf", "estado", "praca",
        "unidade", "unidade_original", "preco", "preco_original", "preco_formatado",
        "data_referencia", "data_referencia_inicio", "data_referencia_fim",
        "periodo_referencia", "fonte", "fonte_url", "tipo", "nivel_comercializacao",
        "categoria", "observacao",
    ]

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos, delimiter=";")
        w.writeheader()
        for item in itens:
            w.writerow({c: item.get(c, "") for c in campos})


def salvar_jsons(
    *,
    itens_tabela: list[dict[str, Any]],
    status_fontes: list[dict[str, Any]],
    data_corte_iso: str,
    stats: dict[str, int],
) -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    dados_html = [item_para_html(x) for x in itens_tabela]
    regionais = [
        item
        for item in itens_tabela
        if limpar_texto(item.get("tipo")).lower() == "regional"
        or "AIBA" in limpar_texto(item.get("fonte")).upper()
        or "SEAGRI" in limpar_texto(item.get("fonte")).upper()
    ]

    resumo = {
        "total_cotacoes_tabela": len(itens_tabela),
        "total_dados_html": len(dados_html),
        "total_cotacoes_brutas": stats.get("brutas", 0),
        "total_cotacoes_descartadas_por_validacao": stats.get("descartados_validacao", 0),
        "grupos_descartados_por_data_antiga": stats.get("descartados_data", 0),
        "total_sorgo_conab_tabela": len([x for x in itens_tabela if x.get("produto_base") == "Sorgo" and "CONAB" in x.get("fonte", "")]),
        "total_arroz_extraido": len([x for x in itens_tabela if x.get("produto_base") == "Arroz"]),
        "total_feijao_extraido": len([x for x in itens_tabela if x.get("produto_base") == "Feijão"]),
        "total_boi_gordo_extraido": len([x for x in itens_tabela if x.get("produto_base") == "Boi Gordo"]),
        "total_leite_extraido": len([x for x in itens_tabela if x.get("produto_base") == "Leite"]),
    }

    base = {
        "ok": True,
        "projeto": PROJETO,
        "modulo": MODULO,
        "versao": VERSAO,
        "gerado_em": agora_local().isoformat(),
        "ultima_sincronizacao": agora_local().strftime("%Y-%m-%d %H:%M:%S"),
        "data_limite_cotacoes_ativas": data_corte_iso,
        "dias_maximos_cotacao_ativa": DIAS_MAXIMOS_COTACAO_ATIVA,
        "politica": (
            "CONAB é a fonte principal. Produtos 360º para Soja, Milho e Algodão; "
            "SIAGRO/Preço Médio UF para Sorgo, Arroz, Feijão, Boi Gordo e Leite; "
            "regionais como complemento; CEPEA fora da tabela principal."
        ),
        "fontes": status_fontes,
        "resumo": resumo,
        "total_cotacoes_tabela": len(itens_tabela),
        "total_dados_html": len(dados_html),
        "dados": dados_html,
    }

    OUTPUT_JSON.write_text(json.dumps(base, ensure_ascii=False, indent=2), encoding="utf-8")

    regional_obj = dict(base)
    regional_obj["dados"] = [item_para_html(x) for x in regionais]
    regional_obj["total_cotacoes_tabela"] = len(regionais)
    regional_obj["total_dados_html"] = len(regionais)
    regional_obj["resumo"] = {**resumo, "total_regionais": len(regionais)}
    OUTPUT_JSON_REGIONAL.write_text(json.dumps(regional_obj, ensure_ascii=False, indent=2), encoding="utf-8")

    status = dict(base)
    status.pop("dados", None)
    status["debug_conab_produtos_360"] = str(OUTPUT_DEBUG_CONAB360)
    status["debug_sorgo_conab"] = str(OUTPUT_DEBUG_SIAGRO)
    OUTPUT_STATUS.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


# =============================================================================
# Execução principal
# =============================================================================

def main() -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    status_fontes: list[dict[str, Any]] = []
    preservados_regionais = carregar_regionais_anteriores()

    brutas: list[dict[str, Any]] = []
    brutas.extend(coletar_conab_360(status_fontes))

    siagro = coletar_siagro(status_fontes)
    brutas.extend(siagro)

    produtos_siagro_ok = {x.get("produto_base") for x in siagro if x.get("produto_base")}
    produtos_fallback = set(PRODUTOS_SIAGRO) - produtos_siagro_ok
    brutas.extend(coletar_conab_semanal_fallback(produtos_fallback, status_fontes))

    regionais_novos = coletar_aiba(status_fontes)
    brutas.extend(regionais_novos)

    if not regionais_novos and preservados_regionais:
        status_fontes.append(
            {
                "fonte": "Regionais preservados",
                "status": "ok",
                "total_registros": len(preservados_regionais),
                "observacao": "AIBA/regionais anteriores preservados por segurança.",
            }
        )
        brutas.extend(preservados_regionais)

    data_corte = (agora_local().date() - timedelta(days=DIAS_MAXIMOS_COTACAO_ATIVA)).isoformat()
    tabela, stats = consolidar(brutas, data_corte)
    stats["brutas"] = len(brutas)

    salvar_csv(tabela)
    salvar_jsons(
        itens_tabela=tabela,
        status_fontes=status_fontes,
        data_corte_iso=data_corte,
        stats=stats,
    )

    OUTPUT_DEBUG_PRIORIDADE.write_text(
        json.dumps(
            {
                "versao": VERSAO,
                "regra": (
                    "SIAGRO é principal para Sorgo, Arroz, Feijão, Boi Gordo e Leite. "
                    "CONAB Semanal é fallback quando SIAGRO não retorna dado válido."
                ),
                "produtos_siagro_ok": sorted(produtos_siagro_ok),
                "produtos_fallback": sorted(produtos_fallback),
                "total_tabela": len(tabela),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(json.dumps({
        "ok": True,
        "versao": VERSAO,
        "total_brutas": len(brutas),
        "total_tabela": len(tabela),
        "produtos_siagro_ok": sorted(produtos_siagro_ok),
        "produtos_fallback": sorted(produtos_fallback),
        "saida": str(OUTPUT_JSON),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
