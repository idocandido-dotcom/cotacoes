#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nordeste Agro — Coletor Automático de Cotações

Arquivo correto:
cotacoes/scripts/coletor_cotacoes_nordeste.py

Este script gera:
cotacoes/public/cotacoes_nordeste.json
cotacoes/public/cotacoes_nordeste.csv
cotacoes/logs/status_ultima_execucao.json
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from datetime import datetime
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
OUTPUT_CSV = PUBLIC_DIR / "cotacoes_nordeste.csv"
OUTPUT_LOG = LOGS_DIR / "status_ultima_execucao.json"

AIBA_URL = "https://aiba.org.br/cotacoes/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NordesteAgroBot/1.0; +https://nordesteagro.com)"
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


def agora_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def limpar_texto(valor: Any) -> str:
    texto = str(valor or "").replace("\xa0", " ")
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def remover_acentos(valor: Any) -> str:
    texto = str(valor or "")
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")
    return texto


def slugify(valor: Any) -> str:
    texto = remover_acentos(valor).lower()
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
    texto = limpar_texto(valor)

    if not texto:
        return None

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

    return agora_local().date().isoformat()


def baixar_html(url: str) -> str:
    resposta = requests.get(url, headers=HEADERS, timeout=60)
    resposta.raise_for_status()

    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return resposta.content.decode(encoding)
        except UnicodeDecodeError:
            continue

    return resposta.content.decode("latin1", errors="ignore")


def coletar_aiba(status_fontes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cotacoes: list[dict[str, Any]] = []

    try:
        html = baixar_html(AIBA_URL)
        soup = BeautifulSoup(html, "html.parser")

        linhas = [
            limpar_texto(linha)
            for linha in soup.get_text("\n", strip=True).splitlines()
            if limpar_texto(linha)
        ]

        ignorar = {
            "início",
            "inicio",
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
            preco_texto = linhas[i + 2]
            detalhe = linhas[i + 3]

            if produto.lower() in ignorar:
                continue

            if not preco_texto.startswith("R$"):
                continue

            if not produto_eh_alvo(produto):
                continue

            preco = parse_preco(preco_texto)

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
                    "historico_30_dias": [],
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

    log = {
        "ok": payload.get("ok"),
        "ultima_sincronizacao": payload.get("ultima_sincronizacao"),
        "total_cotacoes": payload.get("resumo", {}).get("total_cotacoes"),
        "fontes": payload.get("fontes"),
    }

    OUTPUT_LOG.write_text(
        json.dumps(log, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    inicio = agora_local()

    status_fontes: list[dict[str, Any]] = []

    cotacoes = []
    cotacoes.extend(coletar_aiba(status_fontes))

    payload = {
        "ok": True,
        "projeto": "Nordeste Agro",
        "modulo": "cotacoes",
        "repositorio": "idocandido-dotcom/cotacoes",
        "versao": "1.0.1",
        "ultima_sincronizacao": agora_local().strftime("%Y-%m-%d %H:%M:%S"),
        "ultima_sincronizacao_iso": agora_local().isoformat(),
        "fuso_horario": TIMEZONE,
        "frequencia_atualizacao": "diaria",
        "fonte_principal": "AIBA",
        "fontes_complementares": [],
        "resumo": {
            "total_cotacoes": len(cotacoes),
            "fontes_com_sucesso": [
                fonte["fonte"] for fonte in status_fontes if fonte.get("status") == "ok"
            ],
            "total_fontes_com_erro": len(
                [fonte for fonte in status_fontes if fonte.get("status") != "ok"]
            ),
            "tempo_execucao_segundos": round((agora_local() - inicio).total_seconds(), 2),
        },
        "fontes": status_fontes,
        "cotacoes": cotacoes,
        "historico_30_dias": {},
        "aviso_legal": (
            "As cotações apresentadas pelo Nordeste Agro são referenciais e compiladas "
            "a partir de fontes regionais e/ou oficiais de mercado. Os valores podem variar "
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
    print(f"JSON: {OUTPUT_JSON}")
    print(f"CSV: {OUTPUT_CSV}")
    print(f"LOG: {OUTPUT_LOG}")


if __name__ == "__main__":
    main()
