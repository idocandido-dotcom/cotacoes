#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nordeste Agro — Coletor Automático de Cotações

Repositório: idocandido-dotcom/cotacoes
Pasta própria: cotacoes/

Este coletor gera:

cotacoes/public/cotacoes_nordeste.json
cotacoes/public/cotacoes_nordeste.csv
cotacoes/logs/status_ultima_execucao.json

Fontes iniciais:
- AIBA: cotações regionais do Oeste Baiano/MATOPIBA
- CONAB: tentativa de leitura dos arquivos públicos de preços agropecuários

Regra importante:
O coletor não inventa preço. Se uma fonte falhar, ele registra erro no JSON/log.
"""

from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


# ============================================================
# CONFIGURAÇÕES
# ============================================================

TIMEZONE = "America/Fortaleza"

ROOT_DIR = Path(__file__).resolve().parents[1]

PUBLIC_DIR = ROOT_DIR / "public"
LOGS_DIR = ROOT_DIR / "logs"

OUTPUT_JSON = PUBLIC_DIR / "cotacoes_nordeste.json"
OUTPUT_CSV = PUBLIC_DIR / "cotacoes_nordeste.csv"
OUTPUT_LOG = LOGS_DIR / "status_ultima_execucao.json"

AIBA_URL = "https://aiba.org.br/cotacoes/"

CONAB_URLS = [
    {
        "nome": "CONAB - Preços Agropecuários Semanal Município",
        "url": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalMunicipio.txt",
        "tipo": "semanal_municipio",
    },
    {
        "nome": "CONAB - Preços Agropecuários Semanal UF",
        "url": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt",
        "tipo": "semanal_uf",
    },
]

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NordesteAgroBot/1.0; "
        "+https://nordesteagro.com)"
    )
}


# ============================================================
# FUNÇÕES DE TEXTO E FORMATAÇÃO
# ============================================================

def agora_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def limpar_texto(texto: Any) -> str:
    texto = str(texto or "").replace("\xa0", " ")
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def remover_acentos(texto: Any) -> str:
    texto = str(texto or "")
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")
    return texto


def slugify(texto: Any) -> str:
    texto = remover_acentos(texto).lower()
    texto = re.sub(r"[^a-z0-9]+", "-", texto)
    return texto.strip("-")


def produto_eh_alvo(produto: Any) -> bool:
    produto_norm = remover_acentos(produto).lower()
    return any(remover_acentos(p).lower() in produto_norm for p in PRODUTOS_ALVO)


def normalizar_produto(produto: Any) -> str:
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
        if "carioca" in p_norm:
            return "Feijão Carioca"
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


def parse_preco(valor: Any) -> Optional[float]:
    if valor is None:
        return None

    s = limpar_texto(valor)

    if not s:
        return None

    s = s.replace("R$", "")
    s = s.replace("r$", "")
    s = re.sub(r"[^0-9,.\-]", "", s)

    if not s:
        return None

    if "," in s:
        s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def parse_percentual(valor: Any) -> Optional[float]:
    s = limpar_texto(valor)

    m = re.search(r"([+-]?\d+(?:[.,]\d+)?)\s*%", s)
    if not m:
        return None

    return parse_preco(m.group(1))


def parse_data(valor: Any) -> str:
    s = limpar_texto(valor)

    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        dia, mes, ano = m.groups()
        return f"{ano}-{mes}-{dia}"

    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)

    return agora_local().date().isoformat()


def baixar_texto(url: str, timeout: int = 60) -> str:
    resposta = requests.get(url, headers=HEADERS, timeout=timeout)
    resposta.raise_for_status()

    conteudo = resposta.content

    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return conteudo.decode(encoding)
        except UnicodeDecodeError:
            pass

    return conteudo.decode("latin1", errors="ignore")


# ============================================================
# COLETA AIBA
# ============================================================

def coletar_aiba(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes: list[dict[str, Any]] = []

    try:
        html = baixar_texto(AIBA_URL)
        soup = BeautifulSoup(html, "html.parser")

        linhas = [
            limpar_texto(linha)
            for linha in soup.get_text("\n", strip=True).splitlines()
            if limpar_texto(linha)
        ]

        ignorar = {
            "início",
            "home",
            "cotações",
            "cotacoes",
            "mercado bahia",
            "siga a aiba",
            "carregando dados...",
        }

        for i in range(0, len(linhas) - 3):
            produto = linhas[i]
            unidade = linhas[i + 1]
            preco_txt = linhas[i + 2]
            detalhe = linhas[i + 3]

            if produto.lower() in ignorar:
                continue

            if not preco_txt.startswith("R$"):
                continue

            if not produto_eh_alvo(produto):
                continue

            preco = parse_preco(preco_txt)

            if preco is None:
                continue

            produto_nome = normalizar_produto(produto)

            cotacoes.append(
                {
                    "produto": produto_nome,
                    "produto_slug": slugify(produto_nome),
                    "uf": "BA",
                    "estado": "Bahia",
                    "praca": "Oeste Baiano",
                    "unidade": limpar_texto(unidade),
                    "preco": preco,
                    "moeda": "BRL",
                    "variacao_percentual": parse_percentual(detalhe),
                    "data_referencia": parse_data(detalhe),
                    "fonte": "AIBA",
                    "fonte_url": AIBA_URL,
                    "tipo": "regional",
                    "categoria": "commodity_agricola",
                    "observacao": "Cotação regional do Oeste Baiano/MATOPIBA.",
                }
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


# ============================================================
# COLETA CONAB
# ============================================================

def detectar_delimitador(texto: str) -> str:
    primeira_linha = texto.splitlines()[0] if texto.splitlines() else ""

    candidatos = [";", "\t", "|", ","]
    contagens = {sep: primeira_linha.count(sep) for sep in candidatos}

    melhor = max(contagens, key=contagens.get)

    if contagens[melhor] == 0:
        return ";"

    return melhor


def normalizar_nome_coluna(nome: Any) -> str:
    nome = remover_acentos(nome).lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    return nome.strip("_")


def encontrar_coluna(colunas: list[str], possibilidades: list[str]) -> Optional[str]:
    colunas_norm = {col: normalizar_nome_coluna(col) for col in colunas}

    for col_original, col_norm in colunas_norm.items():
        for termo in possibilidades:
            termo_norm = normalizar_nome_coluna(termo)
            if termo_norm in col_norm:
                return col_original

    return None


def ler_csv_texto(texto: str) -> list[dict[str, Any]]:
    delimitador = detectar_delimitador(texto)
    arquivo = io.StringIO(texto)

    leitor = csv.DictReader(arquivo, delimiter=delimitador)

    registros = []

    for linha in leitor:
        if not linha:
            continue

        registro = {}

        for chave, valor in linha.items():
            if chave is None:
                continue

            registro[limpar_texto(chave)] = limpar_texto(valor)

        registros.append(registro)

    return registros


def coletar_conab(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes: list[dict[str, Any]] = []

    for fonte in CONAB_URLS:
        nome_fonte = fonte["nome"]
        url = fonte["url"]
        tipo_fonte = fonte["tipo"]

        total_fonte = 0

        try:
            texto = baixar_texto(url, timeout=120)
            registros = ler_csv_texto(texto)

            if not registros:
                raise RuntimeError("Arquivo lido, mas nenhum registro foi encontrado.")

            colunas = list(registros[0].keys())

            col_produto = encontrar_coluna(colunas, ["produto", "nome_produto", "produto_descricao"])
            col_uf = encontrar_coluna(colunas, ["uf", "estado", "sigla_uf"])
            col_municipio = encontrar_coluna(colunas, ["municipio", "município", "cidade", "praca", "praça"])
            col_preco = encontrar_coluna(colunas, ["preco", "preço", "valor", "vlr"])
            col_unidade = encontrar_coluna(colunas, ["unidade", "unid"])
            col_data = encontrar_coluna(colunas, ["data", "dt", "referencia", "referência", "semana"])

            if not col_produto or not col_uf or not col_preco:
                raise RuntimeError(
                    "Não foi possível identificar as colunas principais do arquivo CONAB. "
                    f"Colunas encontradas: {colunas}"
                )

            for linha in registros:
                uf = limpar_texto(linha.get(col_uf, "")).upper()

                if uf not in UFS_NORDESTE:
                    continue

                produto_original = limpar_texto(linha.get(col_produto, ""))

                if not produto_eh_alvo(produto_original):
                    continue

                preco = parse_preco(linha.get(col_preco, ""))

                if preco is None:
                    continue

                produto_nome = normalizar_produto(produto_original)

                praca = "Média UF"
                if col_municipio:
                    praca = limpar_texto(linha.get(col_municipio, "")) or "Média UF"

                unidade = ""
                if col_unidade:
                    unidade = limpar_texto(linha.get(col_unidade, ""))

                data_ref = agora_local().date().isoformat()
                if col_data:
                    data_ref = parse_data(linha.get(col_data, ""))

                cotacoes.append(
                    {
                        "produto": produto_nome,
                        "produto_slug": slugify(produto_nome),
                        "uf": uf,
                        "estado": UFS_NORDESTE[uf],
                        "praca": praca,
                        "unidade": unidade,
                        "preco": preco,
                        "moeda": "BRL",
                        "variacao_percentual": None,
                        "data_referencia": data_ref,
                        "fonte": nome_fonte,
                        "fonte_url": url,
                        "tipo": "oficial",
                        "categoria": "commodity_agricola",
                        "observacao": "Preço agropecuário compilado a partir de arquivo público da CONAB.",
                    }
                )

                total_fonte += 1

            status_fontes.append(
                {
                    "fonte": nome_fonte,
                    "url": url,
                    "status": "ok",
                    "total_registros": total_fonte,
                }
            )

        except Exception as erro:
            status_fontes.append(
                {
                    "fonte": nome_fonte,
                    "url": url,
                    "status": "erro",
                    "erro": str(erro),
                }
            )

    return cotacoes


# ============================================================
# HISTÓRICO DE 30 DIAS
# ============================================================

def ler_json_anterior() -> dict[str, Any]:
    if not OUTPUT_JSON.exists():
        return {}

    try:
        return json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def chave_cotacao(cotacao: dict[str, Any]) -> str:
    return "|".join(
        [
            str(cotacao.get("fonte", "")),
            str(cotacao.get("produto_slug", "")),
            str(cotacao.get("uf", "")),
            slugify(str(cotacao.get("praca", ""))),
            str(cotacao.get("unidade", "")),
        ]
    )


def atualizar_historico(cotacoes: list[dict[str, Any]], json_anterior: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    hoje = agora_local().date()

    historico_antigo = json_anterior.get("historico_30_dias", {})

    if not isinstance(historico_antigo, dict):
        historico_antigo = {}

    historico_novo: dict[str, list[dict[str, Any]]] = {}

    for chave, pontos in historico_antigo.items():
        if not isinstance(pontos, list):
            continue

        pontos_validos = []

        for ponto in pontos:
            try:
                data_ponto = date.fromisoformat(str(ponto.get("data")))
                preco_ponto = parse_preco(ponto.get("preco"))

                if preco_ponto is None:
                    continue

                if (hoje - data_ponto).days <= 30:
                    pontos_validos.append(
                        {
                            "data": data_ponto.isoformat(),
                            "preco": preco_ponto,
                        }
                    )
            except Exception:
                continue

        historico_novo[chave] = pontos_validos

    for cotacao in cotacoes:
        chave = chave_cotacao(cotacao)
        data_ref = cotacao.get("data_referencia") or hoje.isoformat()
        preco = parse_preco(cotacao.get("preco"))

        if preco is None:
            continue

        pontos = historico_novo.get(chave, [])

        pontos = [p for p in pontos if p.get("data") != data_ref]
        pontos.append(
            {
                "data": data_ref,
                "preco": preco,
            }
        )

        pontos_ordenados = []

        for ponto in pontos:
            try:
                data_ponto = date.fromisoformat(str(ponto.get("data")))

                if (hoje - data_ponto).days <= 30:
                    pontos_ordenados.append(ponto)
            except Exception:
                continue

        pontos_ordenados.sort(key=lambda p: p["data"])

        historico_novo[chave] = pontos_ordenados

    return historico_novo


def anexar_historico(cotacoes: list[dict[str, Any]], historico: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    for cotacao in cotacoes:
        chave = chave_cotacao(cotacao)
        cotacao["historico_30_dias"] = historico.get(chave, [])

    return cotacoes


# ============================================================
# SALVAMENTO
# ============================================================

def deduplicar(cotacoes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    vistos = set()
    saida = []

    for cotacao in cotacoes:
        chave = (
            cotacao.get("fonte"),
            cotacao.get("produto_slug"),
            cotacao.get("uf"),
            slugify(cotacao.get("praca")),
            cotacao.get("unidade"),
            cotacao.get("data_referencia"),
        )

        if chave in vistos:
            continue

        vistos.add(chave)
        saida.append(cotacao)

    saida.sort(
        key=lambda c: (
            c.get("uf", ""),
            c.get("produto", ""),
            c.get("praca", ""),
            c.get("fonte", ""),
        )
    )

    return saida


def salvar_csv(cotacoes: list[dict[str, Any]]) -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    campos = [
        "produto",
        "produto_slug",
        "uf",
        "estado",
        "praca",
        "unidade",
        "preco",
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


def salvar_json(payload: dict[str, Any]) -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    OUTPUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def salvar_log(payload: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    resumo_log = {
        "ok": payload.get("ok"),
        "ultima_sincronizacao": payload.get("ultima_sincronizacao"),
        "total_cotacoes": payload.get("resumo", {}).get("total_cotacoes"),
        "fontes": payload.get("fontes"),
    }

    OUTPUT_LOG.write_text(
        json.dumps(resumo_log, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def main() -> None:
    inicio = agora_local()

    status_fontes: list[dict[str, Any]] = []

    cotacoes: list[dict[str, Any]] = []

    cotacoes.extend(coletar_aiba(status_fontes))
    cotacoes.extend(coletar_conab(status_fontes))

    cotacoes = deduplicar(cotacoes)

    json_anterior = ler_json_anterior()
    historico = atualizar_historico(cotacoes, json_anterior)
    cotacoes = anexar_historico(cotacoes, historico)

    fontes_ok = [f["fonte"] for f in status_fontes if f.get("status") == "ok"]
    fontes_erro = [f for f in status_fontes if f.get("status") != "ok"]

    payload = {
        "ok": True,
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes",
        "repositorio": "idocandido-dotcom/cotacoes",
        "versao": "1.0.0",
        "ultima_sincronizacao": agora_local().strftime("%Y-%m-%d %H:%M:%S"),
        "ultima_sincronizacao_iso": agora_local().isoformat(),
        "fuso_horario": TIMEZONE,
        "frequencia_atualizacao": "diaria",
        "fonte_principal": "CONAB",
        "fontes_complementares": ["AIBA"],
        "resumo": {
            "total_cotacoes": len(cotacoes),
            "fontes_com_sucesso": fontes_ok,
            "total_fontes_com_erro": len(fontes_erro),
            "tempo_execucao_segundos": round((agora_local() - inicio).total_seconds(), 2),
        },
        "fontes": status_fontes,
        "cotacoes": cotacoes,
        "historico_30_dias": historico,
        "aviso_legal": (
            "As cotações apresentadas pelo Nordeste Agro são referenciais e compiladas "
            "a partir de fontes oficiais e regionais de mercado. Os valores podem variar "
            "conforme praça de negociação, qualidade do produto, volume negociado, logística "
            "e data de atualização. O Nordeste Agro não se responsabiliza por decisões "
            "comerciais tomadas com base nessas informações."
        ),
    }

    salvar_json(payload)
    salvar_csv(cotacoes)
    salvar_log(payload)

    print("Coleta finalizada.")
    print(f"Total de cotações: {len(cotacoes)}")
    print(f"JSON gerado em: {OUTPUT_JSON}")
    print(f"CSV gerado em: {OUTPUT_CSV}")
    print(f"Log gerado em: {OUTPUT_LOG}")

    if fontes_erro:
        print("Atenção: algumas fontes apresentaram erro.")
        for fonte in fontes_erro:
            print(f"- {fonte.get('fonte')}: {fonte.get('erro')}")


if __name__ == "__main__":
    main()
