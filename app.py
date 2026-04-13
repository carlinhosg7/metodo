import os
import re
import json
import base64
import traceback
import html
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs
from io import StringIO
import csv as csvlib

import requests
from flask import Flask, request, redirect, url_for, session, render_template_string, flash

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from gspread.utils import rowcol_to_a1


# =========================
# CONFIG ENV
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-chave").strip()

WS_BASE = os.getenv("WS_BASE", "BASE").strip()
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES").strip()
WS_LISTAS = os.getenv("WS_LISTAS", "__LISTAS_VALIDACAO__").strip()
WS_PARAMETROS_COMERCIAIS = os.getenv("WS_PARAMETROS_COMERCIAIS", "PARAMETROS_COMERCIAIS").strip()

# ===== CLIENTES GOLD =====
GOLD_SHEET_ID = os.getenv("GOLD_SHEET_ID", "").strip()
GOLD_SHEET_URL = os.getenv("GOLD_SHEET_URL", "").strip()
GOLD_WS = os.getenv("GOLD_WS", "Tab").strip()

# ===== MUNICÍPIOS PÚBLICOS =====
MUNICIPIOS_URL = os.getenv(
    "MUNICIPIOS_URL",
    "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
).strip()

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").strip().lower() in ("1", "true", "sim", "yes")

# ===== CACHE =====
BASE_CACHE_TTL = int(os.getenv("BASE_CACHE_TTL", "300"))               # 5 min
LISTAS_CACHE_TTL = int(os.getenv("LISTAS_CACHE_TTL", "900"))           # 15 min
REP_NAME_CACHE_TTL = int(os.getenv("REP_NAME_CACHE_TTL", "1800"))      # 30 min
MUNICIPIOS_CACHE_TTL = int(os.getenv("MUNICIPIOS_CACHE_TTL", "86400")) # 24h
DEBUG_SHEETINFO_CACHE_TTL = int(os.getenv("DEBUG_SHEETINFO_CACHE_TTL", "120"))

APP_TITLE = "Acompanhamento de clientes"
LOGO_URL = "https://raw.githubusercontent.com/carlinhosg7/metodo/main/logo_kidy.png"

# ===== PLANILHA VENDAS (cabeçalho do painel) =====
VENDAS_SHEET_URL = os.getenv(
    "VENDAS_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1vLoJ755IpcRuW2NgvbejSw-1iUA5ZP7EHINVT9nXYig/edit?usp=sharing"
).strip()
VENDAS_WS = os.getenv("VENDAS_WS", "Tab").strip()
VENDAS_CACHE_TTL = int(os.getenv("VENDAS_CACHE_TTL", "600"))  # 10 min

# =========================
# AGENDA SEMANAL
# =========================
AGENDA_SHEET_URL = os.getenv(
    "AGENDA_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1mg2O7VZrPd2MKOfABkkce6QBp-wcjQV_iADltRkUWAg/edit?usp=sharing"
).strip()
WS_AGENDA = os.getenv("WS_AGENDA", "AGENDA_SEMANAL").strip()

DIAS_SEMANA = [
    "SEGUNDA",
    "TERCA",
    "QUARTA",
    "QUINTA",
    "SEXTA"
]

ATENDIMENTOS = [1, 2, 3, 4]

# =========================
# LISTAS FIXAS (fallback)
# =========================
DEFAULT_MESES = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]

DEFAULT_SEMANAS = [
    "Semana 01", "Semana 02", "Semana 03", "Semana 04", "sem Agenda"
]

DEFAULT_STATUS = [
    "CLIENTE COM BAIXO GIRO",
    "CLIENTE ESTOCADO KIDY",
    "CLIENTE ESTOCADO OUTRAS MARCAS",
    "CLIENTE JÁ COMPROU",
    "CLIENTE NÃO ATENDEU",
    "CLIENTE SEM VERBA",
    "CLIENTE VAI MANDAR O PEDIDO",
    "CLIENTE FECHOU A LOJA",
    "CLIENTE INADIMPLENTE COM A KIDY",
    "CLIENTE INADIMPLENTE COM OUTRAS MARCAS",
]

# =========================
# APP
# =========================
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = SECRET_KEY
app.permanent_session_lifetime = timedelta(days=7)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# =========================
# CACHE GLOBAL SIMPLES
# =========================
_MEM_CACHE = {}


def cache_get(key):
    item = _MEM_CACHE.get(key)
    if not item:
        return None
    expires_at = item.get("expires_at", 0)
    if expires_at < time.time():
        _MEM_CACHE.pop(key, None)
        return None
    return item.get("value")


def cache_set(key, value, ttl):
    _MEM_CACHE[key] = {
        "value": value,
        "expires_at": time.time() + ttl
    }


def cache_del_prefix(prefix):
    keys = [k for k in list(_MEM_CACHE.keys()) if k.startswith(prefix)]
    for k in keys:
        _MEM_CACHE.pop(k, None)


# =========================
# HELPERS
# =========================
def norm(s):
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def h(s):
    return html.escape(norm(s), quote=True)


def unique_list(values):
    out, seen = [], set()
    for v in values:
        v = norm(v)
        if not v:
            continue
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def is_admin():
    return session.get("user_type") == "admin"


def require_login():
    return "user_type" in session and "user_login" in session


def normalize_header(s):
    s = norm(s).lower()
    s = (
        s.replace("á", "a")
         .replace("à", "a")
         .replace("ã", "a")
         .replace("â", "a")
         .replace("é", "e")
         .replace("ê", "e")
         .replace("í", "i")
         .replace("ó", "o")
         .replace("ô", "o")
         .replace("õ", "o")
         .replace("ú", "u")
         .replace("ç", "c")
    )
    return s


def normalize_text_for_match(v):
    s = norm(v).upper()
    s = (
        s.replace("Á", "A")
         .replace("À", "A")
         .replace("Ã", "A")
         .replace("Â", "A")
         .replace("É", "E")
         .replace("Ê", "E")
         .replace("Í", "I")
         .replace("Ó", "O")
         .replace("Ô", "O")
         .replace("Õ", "O")
         .replace("Ú", "U")
         .replace("Ç", "C")
    )
    return s


def normalize_city_key(v):
    s = normalize_text_for_match(v)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def pick_col_exact(headers, candidates):
    hmap = {normalize_header(x): x for x in headers}
    for cand in candidates:
        key = normalize_header(cand)
        if key in hmap:
            return hmap[key]
    return None


def pick_col_flexible(headers, candidates):
    hmap = {normalize_header(x): x for x in headers}

    for cand in candidates:
        key = normalize_header(cand)
        if key in hmap:
            return hmap[key]

    for header in headers:
        header_norm = normalize_header(header)
        for cand in candidates:
            if normalize_header(cand) in header_norm:
                return header

    return None


def resolve_city_col(headers):
    prioridades = [
        ["Cidades"],
        ["Cidade"],
        ["Cidade Cliente", "CIDADE CLIENTE"],
        ["Município", "Municipio", "Municípios", "Municipios"],
    ]
    for candidatos in prioridades:
        col = pick_col_exact(headers, candidatos) or pick_col_flexible(headers, candidatos)
        if col:
            return col
    return None


def resolve_cnpj_col(headers):
    prioridades = [
        ["CNPJ", "Cnpj"],
        ["CPF/CNPJ", "Cpf/Cnpj", "CNPJ/CPF", "Cnpj/Cpf"],
        ["Documento", "Documento Cliente", "Doc", "CNPJ Cliente", "Cnpj Cliente"],
    ]
    for candidatos in prioridades:
        col = pick_col_exact(headers, candidatos) or pick_col_flexible(headers, candidatos)
        if col:
            return col
    return None


def clean_color_text(v):
    return norm(v)


def is_truthy_novo(v):
    s = normalize_text_for_match(v)
    return s in {"SIM", "S", "YES", "Y", "1", "TRUE", "VERDADEIRO", "NOVO", "CLIENTE NOVO"}


def get_row_class_from_color_text(status_cor_raw):
    s = normalize_text_for_match(status_cor_raw)

    if "VERMELH" in s:
        return "row-red", 1
    if "LARANJ" in s:
        return "row-orange", 2
    if "AMAREL" in s:
        return "row-yellow", 3
    if "VERDE" in s:
        return "row-green", 4
    if "AZUL" in s:
        return "row-blue", 5
    if "NOVO" in s or "NOVA" in s:
        return "row-blue", 5

    return "", 99


def resolve_status_cor_from_base(row, status_cor_col=None, cliente_novo_col=None):
    status_cor_raw = clean_color_text(row.get(status_cor_col, "")) if status_cor_col else ""

    if status_cor_raw:
        row_class, priority = get_row_class_from_color_text(status_cor_raw)
        return status_cor_raw, row_class, priority

    if cliente_novo_col:
        novo_val = row.get(cliente_novo_col, "")
        if is_truthy_novo(novo_val):
            return "AZUL", "row-blue", 5

    return "", "", 99


def get_rep_photo_src(codigo_rep):
    codigo = norm(codigo_rep)
    if not codigo:
        return ""

    exts = ["png", "jpg", "jpeg", "webp"]
    base_static_dir = os.path.join(app.root_path, "static", "representantes")

    for ext in exts:
        abs_path = os.path.join(base_static_dir, f"{codigo}.{ext}")
        if os.path.exists(abs_path):
            return f"/static/representantes/{codigo}.{ext}"

    return ""


def fmt_money(v):
    return norm(v)


def to_input_date(v):
    v = norm(v)
    if not v:
        return ""

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return v

    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", v)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    return ""


def from_input_date(v):
    v = norm(v)
    if not v:
        return ""

    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{dd}/{mm}/{yyyy}"

    return v


def normalizar_data_comparacao(v):
    v = norm(v)
    if not v:
        return ""

    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", v):
        return v

    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{dd}/{mm}/{yyyy}"

    return v


def parse_date_any(v):
    v = norm(v)
    if not v:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            pass

    return None


def get_dia_semana_ptbr(v):
    dt = parse_date_any(v)
    if dt is None:
        return ""

    mapa = {
        0: "SEGUNDA",
        1: "TERCA",
        2: "QUARTA",
        3: "QUINTA",
        4: "SEXTA",
        5: "SABADO",
        6: "DOMINGO",
    }
    return mapa.get(dt.weekday(), "")


def montar_agenda_da_base(rows, data_col, grupo_col, valor_col):
    agenda = _agenda_vazia()
    excedentes = []

    if not rows or not data_col or not grupo_col:
        return agenda, excedentes

    linhas_ordenadas = sorted(
        rows,
        key=lambda r: (
            to_input_date(norm(r.get(data_col, ""))) or "9999-12-31",
            norm(r.get(grupo_col, "")),
            norm(r.get(valor_col, "")) if valor_col else ""
        )
    )

    for r in linhas_ordenadas:
        data_base = norm(r.get(data_col, ""))
        dia = get_dia_semana_ptbr(data_base)
        if dia not in DIAS_SEMANA:
            continue

        cliente = norm(r.get(grupo_col, ""))
        valor = format_number_br(parse_number_br(r.get(valor_col, ""))) if valor_col else ""

        if not cliente and not valor:
            continue

        slot_livre = None
        for at in ATENDIMENTOS:
            atual_cliente = norm(agenda[dia][at].get("cliente", ""))
            atual_valor = norm(agenda[dia][at].get("valor", ""))
            if not atual_cliente and not atual_valor:
                slot_livre = at
                break

        if slot_livre is not None:
            agenda[dia][slot_livre]["cliente"] = cliente
            agenda[dia][slot_livre]["valor"] = valor
        else:
            excedentes.append({
                "dia": dia,
                "data": data_base,
                "cliente": cliente,
                "valor": valor,
            })

    return agenda, excedentes


def safe_cell(vals, idx_1_based):
    pos = idx_1_based - 1
    return norm(vals[pos]) if pos < len(vals) else ""


def set_last_save_debug(payload):
    session["last_save_debug"] = payload


def get_last_save_debug():
    return session.get("last_save_debug", {})


def parse_number_br(value):
    s = norm(value)
    if not s:
        return 0.0

    s = s.replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def parse_float_any(value):
    s = norm(value)
    if not s:
        return None

    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def format_number_br(value):
    try:
        n = float(value)
    except Exception:
        n = 0.0
    txt = f"{n:,.2f}"
    txt = txt.replace(",", "X").replace(".", ",").replace("X", ".")
    return txt


def format_money_br(value):
    return f"R$ {format_number_br(value)}"


def format_cnpj_key(v):
    s = re.sub(r"\D", "", norm(v))
    return s


def build_cidades_resumo_html(filtered_rows, cidade_col=None, cnpj_col=None, valor_col=None, fallback_id_col=None):
    if not filtered_rows or not cidade_col:
        return (
            """
            <div class="dash-map-placeholder" style="min-height:220px;">
              Nenhuma cidade encontrada para exibir no resumo.
            </div>
            """,
            0,
            ""
        )

    resumo = {}

    for r in filtered_rows:
        cidade = norm(r.get(cidade_col, ""))
        if not cidade:
            continue

        chave = normalize_city_key(cidade)
        if chave not in resumo:
            resumo[chave] = {
                "cidade": cidade,
                "cnpjs": set(),
                "valor": 0.0,
                "valor_tem_dado": False,
            }

        cnpj = ""
        if cnpj_col:
            cnpj = format_cnpj_key(r.get(cnpj_col, ""))

        if not cnpj and fallback_id_col:
            cnpj = format_cnpj_key(r.get(fallback_id_col, ""))

        if cnpj:
            resumo[chave]["cnpjs"].add(cnpj)

        if valor_col:
            valor_raw = norm(r.get(valor_col, ""))
            if valor_raw != "":
                resumo[chave]["valor_tem_dado"] = True
            resumo[chave]["valor"] += parse_number_br(valor_raw)

    if not resumo:
        return (
            """
            <div class="dash-map-placeholder" style="min-height:220px;">
              Nenhuma cidade encontrada para exibir no resumo.
            </div>
            """,
            0,
            ""
        )

    linhas = []
    cidades_sem_cnpj = 0
    cidades_sem_valor = 0
    cidades_com_valor = 0

    itens = sorted(
        resumo.values(),
        key=lambda x: (-(x.get("valor", 0.0)), x.get("cidade", ""))
    )

    for item in itens:
        qtd_cnpjs = len(item["cnpjs"])
        valor = float(item.get("valor", 0.0) or 0.0)
        valor_tem_dado = bool(item.get("valor_tem_dado", False))

        if qtd_cnpjs == 0:
            row_style = "background:#fecaca; color:#7f1d1d; font-weight:700;"
            cidades_sem_cnpj += 1
        elif (not valor_tem_dado) or valor <= 0:
            row_style = "background:#fee2e2; color:#991b1b; font-weight:700;"
            cidades_sem_valor += 1
        else:
            row_style = "background:#dcfce7; color:#166534; font-weight:700;"
            cidades_com_valor += 1

        linhas.append(f"""
        <tr style="{row_style}">
          <td>{h(item['cidade'])}</td>
          <td style="text-align:center;">{qtd_cnpjs}</td>
          <td style="text-align:right;">{h(format_number_br(valor))}</td>
        </tr>
        """)

    info_msg = (
        f"Sem CNPJ: {cidades_sem_cnpj} | "
        f"Vlr vazio/0: {cidades_sem_valor} | "
        f"Vlr > 0: {cidades_com_valor}"
    )

    html = f"""
    <div style="height:100%; min-height:100%; overflow:auto; width:100%;">
        <table class="dash-table-mini">
        <thead>
          <tr>
            <th>Cidades</th>
            <th style="width:90px; text-align:center;">CNPJs (Qtde CNPJ)</th>
            <th style="width:110px; text-align:right;">Vlr</th>
          </tr>
        </thead>
        <tbody>
          {''.join(linhas)}
        </tbody>
      </table>
    </div>
    """

    return html, len(itens), info_msg


def render_status_badge_text(status_cor):
    s = normalize_text_for_match(status_cor)
    if "VERMELH" in s:
        return "Vermelho"
    if "LARANJ" in s:
        return "Laranja"
    if "AMAREL" in s:
        return "Amarelo"
    if "VERDE" in s:
        return "Verde"
    if "AZUL" in s or "NOVO" in s:
        return "Azul"
    return norm(status_cor)


def extract_google_sheet_id(raw_value):
    raw = norm(raw_value)
    if not raw:
        return ""

    if "/spreadsheets/d/" in raw:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw)
        if m:
            return m.group(1)

    if raw.startswith("http://") or raw.startswith("https://"):
        try:
            parsed = urlparse(raw)
            qs = parse_qs(parsed.query)
            if "id" in qs and qs["id"]:
                return qs["id"][0]
        except Exception:
            pass

    return raw


def friendly_gspread_error(exc):
    txt = norm(str(exc))

    if isinstance(exc, WorksheetNotFound):
        return "A aba informada não foi encontrada na planilha."

    if "Response [404]" in txt or "Requested entity was not found" in txt:
        return (
            "Planilha Google Sheets não encontrada. "
            "Verifique se o ID/URL está correto e se a planilha existe."
        )

    if "Response [403]" in txt or "PERMISSION_DENIED" in txt or "The caller does not have permission" in txt:
        return (
            "Sem permissão para acessar a planilha Google Sheets. "
            "Compartilhe a planilha com o e-mail da service account como Editor."
        )

    if "This operation is not supported for this document" in txt:
        return (
            "O arquivo informado não é uma planilha Google Sheets válida. "
            "Converta o arquivo para Google Sheets e use o ID/URL correto."
        )

    return txt or "Erro ao acessar Google Sheets."


def resolve_gold_sheet_target():
    target = GOLD_SHEET_ID or GOLD_SHEET_URL or SHEET_ID
    return extract_google_sheet_id(target)


def render_error_page(subtitle, message, user_photo_url=""):
    body = f"<div class='card'><b>{h(message)}</b></div>"
    return render_template_string(
        BASE_HTML,
        title=APP_TITLE,
        subtitle=subtitle,
        logged=require_login(),
        user_login=session.get("user_login", ""),
        user_name=session.get("rep_name", ""),
        user_type=session.get("user_type", ""),
        user_photo_url=user_photo_url,
        body=body
    )


def connect_vendas_gs():
    vendas_sheet_id = extract_google_sheet_id(VENDAS_SHEET_URL)
    if not vendas_sheet_id:
        raise RuntimeError("URL/ID da planilha VENDAS não informado.")
    return connect_gs_by_key(vendas_sheet_id)


def get_vendas_rows_cached():
    cache_key = f"vendas::{extract_google_sheet_id(VENDAS_SHEET_URL)}::{VENDAS_WS}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    sh_vendas = connect_vendas_gs()

    try:
        ws_vendas = sh_vendas.worksheet(VENDAS_WS)
    except WorksheetNotFound:
        raise RuntimeError(f"A aba '{VENDAS_WS}' não foi encontrada na planilha VENDAS.")

    headers, rows = safe_get_raw_rows(ws_vendas)
    payload = (headers, rows)
    cache_set(cache_key, payload, VENDAS_CACHE_TTL)
    return payload


def invalidate_vendas_cache():
    cache_del_prefix(f"vendas::{extract_google_sheet_id(VENDAS_SHEET_URL)}::{VENDAS_WS}")


def get_vendas_info_by_rep(rep_code):
    rep_code = norm(rep_code)

    info = {
        "ok": False,
        "error": "",
        "rep_code": rep_code,
        "representante": "",
        "supervisor": "",
        "meta": 0.0,
        "realizado": 0.0,
        "percentual": 0.0,
    }

    if not rep_code:
        info["error"] = "Representante não informado."
        return info

    try:
        headers, rows = get_vendas_rows_cached()

        col_rep = pick_col_flexible(headers, [
            "Codigo Representante", "Código Representante", "Cod Representante", "COD_REP"
        ])
        col_nome = pick_col_flexible(headers, [
            "Representante", "Nome Representante"
        ])
        col_sup = pick_col_flexible(headers, [
            "Supervisor", "Supervisão"
        ])
        col_meta = pick_col_flexible(headers, [
            "Vlr Meta Entrega", "Meta Entrega", "Vlr Meta", "Meta"
        ])
        col_venda = pick_col_flexible(headers, [
            "Vlr Venda", "Valor Venda", "Venda"
        ])

        if not col_rep:
            raise RuntimeError("Coluna de representante não encontrada na planilha VENDAS.")

        for row in rows:
            rep_val = norm(row.get(col_rep, ""))
            rep_val_num = rep_val.lstrip("0") or "0"
            rep_code_num = rep_code.lstrip("0") or "0"

            if rep_val == rep_code or rep_val_num == rep_code_num:
                meta = parse_number_br(row.get(col_meta, "")) if col_meta else 0.0
                realizado = parse_number_br(row.get(col_venda, "")) if col_venda else 0.0
                percentual = (realizado / meta * 100.0) if meta > 0 else 0.0

                info["representante"] = norm(row.get(col_nome, "")) if col_nome else ""
                info["supervisor"] = norm(row.get(col_sup, "")) if col_sup else ""
                info["meta"] = meta
                info["realizado"] = realizado
                info["percentual"] = percentual
                info["ok"] = True
                return info

        info["error"] = f"Representante {rep_code} não encontrado na planilha VENDAS."
        return info

    except Exception as e:
        info["error"] = norm(str(e))
        return info


# =========================
# MUNICÍPIOS CACHE + ÍNDICE
# =========================
def load_public_municipios():
    cache_key = f"municipios::{MUNICIPIOS_URL}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached["rows"], cached["index"], ""

    try:
        resp = requests.get(MUNICIPIOS_URL, timeout=30)
        resp.raise_for_status()

        csv_text = resp.text
        if not csv_text.strip():
            return [], {}, "Arquivo público de municípios vazio."

        reader = csvlib.DictReader(StringIO(csv_text))
        rows = []
        index_exato = {}
        index_simplificado = {}

        def simplificar(txt):
            txt = normalize_city_key(txt)
            txt = re.sub(r"\b(DO|DA|DE|DOS|DAS)\b", " ", txt)
            txt = re.sub(r"\s+", " ", txt).strip()
            return txt

        for row in reader:
            clean_row = {}
            for k, v in row.items():
                clean_row[norm(k)] = norm(v)

            rows.append(clean_row)

            nome = norm(
                clean_row.get("nome", "") or
                clean_row.get("cidade", "") or
                clean_row.get("municipio", "") or
                clean_row.get("município", "")
            )

            if nome:
                nome_key = normalize_city_key(nome)
                nome_simpl = simplificar(nome)

                if nome_key and nome_key not in index_exato:
                    index_exato[nome_key] = clean_row

                if nome_simpl and nome_simpl not in index_simplificado:
                    index_simplificado[nome_simpl] = clean_row

        payload = {
            "rows": rows,
            "index": {
                "exato": index_exato,
                "simplificado": index_simplificado
            }
        }
        cache_set(cache_key, payload, MUNICIPIOS_CACHE_TTL)

        return rows, payload["index"], ""
    except Exception as e:
        return [], {}, f"Erro ao carregar municípios públicos: {norm(str(e))}"


def find_city_coords_public(rows_cidades, municipios_index, cidade_base_norm, cidade_original=""):
    if not cidade_base_norm:
        return None, None, ""

    municipios_index = municipios_index or {}
    index_exato = municipios_index.get("exato", {})
    index_simplificado = municipios_index.get("simplificado", {})

    def simplificar(txt):
        txt = normalize_city_key(txt)
        txt = re.sub(r"\b(DO|DA|DE|DOS|DAS)\b", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    melhor_row = index_exato.get(cidade_base_norm)

    if melhor_row is None:
        base_simpl = simplificar(cidade_original or cidade_base_norm)
        melhor_row = index_simplificado.get(base_simpl)

    if melhor_row is None and rows_cidades:
        for r in rows_cidades:
            nome = norm(
                r.get("nome", "") or
                r.get("cidade", "") or
                r.get("municipio", "") or
                r.get("município", "")
            )
            nome_norm = normalize_city_key(nome)

            if nome_norm and (
                cidade_base_norm in nome_norm or
                nome_norm in cidade_base_norm
            ):
                melhor_row = r
                break

    if melhor_row is None:
        return None, None, ""

    nome_final = norm(
        melhor_row.get("nome", "") or
        melhor_row.get("cidade", "") or
        melhor_row.get("municipio", "") or
        melhor_row.get("município", "")
    )
    lat = parse_float_any(melhor_row.get("latitude", "") or melhor_row.get("lat", ""))
    lon = parse_float_any(melhor_row.get("longitude", "") or melhor_row.get("lon", "") or melhor_row.get("lng", ""))

    return lat, lon, nome_final


def build_city_map_svg(city_points, width=900, height=520):
    if not city_points:
        return """
        <div class="dash-map-placeholder">
          Não foi possível montar o mapa.<br><br>
          Verifique o cruzamento das cidades e as colunas de latitude e longitude.
        </div>
        """

    valid_points = [
        p for p in city_points
        if p.get("lat") is not None and p.get("lon") is not None
    ]

    if not valid_points:
        return """
        <div class="dash-map-placeholder">
          Nenhuma coordenada válida encontrada.
        </div>
        """

    min_lon = min(p["lon"] for p in valid_points)
    max_lon = max(p["lon"] for p in valid_points)
    min_lat = min(p["lat"] for p in valid_points)
    max_lat = max(p["lat"] for p in valid_points)

    lon_span = max_lon - min_lon
    lat_span = max_lat - min_lat

    if lon_span == 0:
        lon_span = 0.3
    if lat_span == 0:
        lat_span = 0.3

    lon_margin = lon_span * 0.08
    lat_margin = lat_span * 0.08

    min_lon -= lon_margin
    max_lon += lon_margin
    min_lat -= lat_margin
    max_lat += lat_margin

    pad = 10

    def project(lon, lat):
        x = pad + ((lon - min_lon) / (max_lon - min_lon)) * (width - 2 * pad)
        y = pad + (1 - ((lat - min_lat) / (max_lat - min_lat))) * (height - 2 * pad)
        return x, y

    circles = []
    labels = []

    for p in valid_points:
        x, y = project(p["lon"], p["lat"])
        fill = p["fill"]
        cidade = p.get("cidade", "")
        status_txt = p.get("status_txt", "")
        total_2024 = p.get("total_2024", 0)
        total_2025 = p.get("total_2025", 0)
        total_2026 = p.get("total_2026", 0)

        title = h(
            f"{cidade} | {status_txt} | "
            f"2024: {format_number_br(total_2024)} | "
            f"2025: {format_number_br(total_2025)} | "
            f"2026: {format_number_br(total_2026)}"
        )

        circles.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="6.2" fill="{fill}" stroke="#ffffff" stroke-width="1.4">'
            f'<title>{title}</title></circle>'
        )

        labels.append(
            f'<text class="map-label" x="{x + 8:.2f}" y="{y - 8:.2f}" font-size="10" fill="#1f2937">{h(cidade[:22])}</text>'
        )

    map_uid = f"map_{int(time.time() * 1000)}_{len(valid_points)}"

    svg = f"""
    <div style="width:100%; background:#eef7f7; border:1px solid #cbd5e1; border-radius:6px; padding:6px; box-sizing:border-box;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; gap:8px; flex-wrap:wrap;">
        <div style="font-size:10px; color:#334155; font-weight:700;">
          Use os botões para zoom
        </div>
        <div style="display:flex; gap:6px; align-items:center;">
          <button type="button" onclick="zoomMap('{map_uid}', 1.2)" title="Aumentar zoom" style="padding:4px 10px; border-radius:6px; border:1px solid #94a3b8; background:#ffffff; cursor:pointer; font-weight:800; font-size:16px; line-height:1;">+</button>
          <button type="button" onclick="zoomMap('{map_uid}', 0.83)" title="Diminuir zoom" style="padding:4px 10px; border-radius:6px; border:1px solid #94a3b8; background:#ffffff; cursor:pointer; font-weight:800; font-size:16px; line-height:1;">−</button>
          <button type="button" onclick="resetMapZoom('{map_uid}')" title="Resetar zoom" style="padding:4px 10px; border-radius:6px; border:1px solid #94a3b8; background:#ffffff; cursor:pointer; font-weight:700; font-size:12px;">Reset</button>
        </div>
      </div>

      <div style="width:100%; height:100%; overflow:auto; background:#dff3f1; border-radius:4px;">
        <svg id="{map_uid}" viewBox="0 0 {width} {height}" width="100%" height="100%" style="display:block; background:#dff3f1; border-radius:4px; transform-origin:center center; transition:transform .15s ease;">
          <rect x="0" y="0" width="{width}" height="{height}" fill="#dff3f1"></rect>
          <rect x="8" y="8" width="{width-16}" height="{height-16}" fill="none" stroke="#94a3b8" stroke-width="1" stroke-dasharray="4 4"></rect>
          {''.join(circles)}
          <g class="map-labels" style="display:none;">
            {''.join(labels)}
          </g>
        </svg>
      </div>

      <div style="display:flex; gap:12px; justify-content:center; align-items:center; margin-top:6px; flex-wrap:wrap; font-size:10px;">
        <span style="display:flex; align-items:center; gap:6px;">
          <span style="width:10px; height:10px; border-radius:50%; background:#16a34a; display:inline-block;"></span>
          Vendas em 2026
        </span>
        <span style="display:flex; align-items:center; gap:6px;">
          <span style="width:10px; height:10px; border-radius:50%; background:#dc2626; display:inline-block;"></span>
          Sem vendas em 2026
        </span>
      </div>
    </div>

    <script>
      window._mapZoomLevels = window._mapZoomLevels || {{}};

      function applyMapLabelVisibility(mapId) {{
        const el = document.getElementById(mapId);
        if (!el) return;

        const zoom = window._mapZoomLevels[mapId] || 1;
        const labels = el.querySelector('.map-labels');
        if (!labels) return;

        if (zoom > 1.05) {{
          labels.style.display = 'block';
        }} else {{
          labels.style.display = 'none';
        }}
      }}

      function zoomMap(mapId, factor) {{
        const el = document.getElementById(mapId);
        if (!el) return;
        if (!window._mapZoomLevels[mapId]) window._mapZoomLevels[mapId] = 1;

        let next = window._mapZoomLevels[mapId] * factor;
        if (next < 1) next = 1;
        if (next > 8) next = 8;

        window._mapZoomLevels[mapId] = next;
        el.style.transform = 'scale(' + next + ')';
        applyMapLabelVisibility(mapId);
      }}

      function resetMapZoom(mapId) {{
        const el = document.getElementById(mapId);
        if (!el) return;
        window._mapZoomLevels[mapId] = 1;
        el.style.transform = 'scale(1)';
        applyMapLabelVisibility(mapId);
      }}

      setTimeout(function() {{
        if (!window._mapZoomLevels['{map_uid}']) {{
          window._mapZoomLevels['{map_uid}'] = 1;
        }}
        applyMapLabelVisibility('{map_uid}');
      }}, 0);
    </script>
    """
    return svg


# =========================
# AGENDA - FUNÇÕES
# =========================
def _agenda_vazia():
    agenda = {}
    for dia in DIAS_SEMANA:
        agenda[dia] = {}
        for at in ATENDIMENTOS:
            agenda[dia][at] = {
                "cliente": "",
                "valor": ""
            }
    return agenda


def connect_agenda_gs():
    agenda_sheet_id = extract_google_sheet_id(AGENDA_SHEET_URL)
    if not agenda_sheet_id:
        raise RuntimeError("URL/ID da planilha da agenda não informado.")
    return connect_gs_by_key(agenda_sheet_id)


def ensure_agenda_worksheet(sh_agenda):
    headers = ["REP", "DIA", "ATENDIMENTO", "CLIENTE", "VALOR"]

    try:
        ws = sh_agenda.worksheet(WS_AGENDA)
    except WorksheetNotFound:
        try:
            ws = sh_agenda.add_worksheet(title=WS_AGENDA, rows="5000", cols="10")
        except Exception as e:
            raise RuntimeError(
                f"Não foi possível acessar/criar a aba '{WS_AGENDA}' da agenda. "
                f"Compartilhe a planilha da agenda com a service account. Detalhe: {friendly_gspread_error(e)}"
            )

    ensure_headers(ws, headers)
    return ws


def carregar_agenda_rep(rep_code):
    rep_code = norm(rep_code)
    agenda = _agenda_vazia()

    if not rep_code:
        return agenda

    try:
        sh_agenda = connect_agenda_gs()
        ws_agenda = ensure_agenda_worksheet(sh_agenda)
        registros = safe_get_all_records(ws_agenda)
    except Exception:
        return agenda

    for row in registros:
        rep = norm(row.get("REP", ""))
        dia = normalize_text_for_match(row.get("DIA", ""))
        at_txt = norm(row.get("ATENDIMENTO", ""))
        cliente = norm(row.get("CLIENTE", ""))
        valor = norm(row.get("VALOR", ""))

        if rep != rep_code:
            continue

        try:
            at = int(at_txt)
        except Exception:
            continue

        if dia in agenda and at in agenda[dia]:
            agenda[dia][at]["cliente"] = cliente
            agenda[dia][at]["valor"] = valor

    return agenda


def salvar_agenda_rep(rep_code, agenda_dict):
    rep_code = norm(rep_code)
    if not rep_code:
        raise RuntimeError("Representante da agenda não informado.")

    sh_agenda = connect_agenda_gs()
    ws_agenda = ensure_agenda_worksheet(sh_agenda)

    all_values = ws_agenda.get_all_values()
    headers = [norm(x) for x in all_values[0]] if all_values else ["REP", "DIA", "ATENDIMENTO", "CLIENTE", "VALOR"]

    rep_col = headers.index("REP") + 1

    rows_to_delete = []
    for idx, row in enumerate(all_values[1:], start=2):
        rep_existente = safe_cell(row, rep_col)
        if rep_existente == rep_code:
            rows_to_delete.append(idx)

    for row_idx in reversed(rows_to_delete):
        ws_agenda.delete_rows(row_idx)

    linhas_novas = []
    for dia in DIAS_SEMANA:
        for at in ATENDIMENTOS:
            cliente = norm(agenda_dict.get(dia, {}).get(at, {}).get("cliente", ""))
            valor = norm(agenda_dict.get(dia, {}).get(at, {}).get("valor", ""))

            if cliente or valor:
                linhas_novas.append([rep_code, dia, at, cliente, valor])

    if linhas_novas:
        ws_agenda.append_rows(linhas_novas, value_input_option="USER_ENTERED")


def render_agenda_semana_html(
    rep_code,
    sup_sel="",
    rep_sel="",
    data_ini="",
    data_fim="",
    agenda_override=None,
    agenda_auto_carregada=False,
    agenda_excedentes=None,
):
    rep_code = norm(rep_code)
    data_ini = norm(data_ini)
    data_fim = norm(data_fim)
    agenda_excedentes = agenda_excedentes or []

    if not rep_code:
        return """
        <div class="dash-summary-box">
          Selecione um representante para exibir e salvar a agenda semanal.
        </div>
        """

    agenda = agenda_override if isinstance(agenda_override, dict) else carregar_agenda_rep(rep_code)

    header_top = []
    header_top.append("<tr>")
    header_top.append('<th style="width:90px;">DIA</th>')
    for at in ATENDIMENTOS:
        header_top.append(f'<th colspan="2" style="text-align:center;">ATENDIMENTO {at:02d}</th>')
    header_top.append("</tr>")

    header_sub = []
    header_sub.append("<tr>")
    header_sub.append("<th></th>")
    for _ in ATENDIMENTOS:
        header_sub.append('<th style="width:150px;">CLIENTE</th>')
        header_sub.append('<th style="width:80px;">VALOR 2025</th>')
    header_sub.append("</tr>")

    body_rows = []

    for dia in DIAS_SEMANA:
        row = [f"<tr><td><b>{h(dia)}</b></td>"]
        for at in ATENDIMENTOS:
            cliente = agenda[dia][at]["cliente"]
            valor = agenda[dia][at]["valor"]

            row.append(
                f'<td><input class="agenda-input" type="text" name="{h(dia)}_{at}_cliente" value="{h(cliente)}" placeholder="Cliente"></td>'
            )
            row.append(
                f'<td><input class="agenda-input agenda-valor" type="text" name="{h(dia)}_{at}_valor" value="{h(valor)}" placeholder="Valor 2025"></td>'
            )
        row.append("</tr>")
        body_rows.append("".join(row))

    hidden_sup = f'<input type="hidden" name="sup" value="{h(sup_sel)}">' if sup_sel else ""
    hidden_rep = f'<input type="hidden" name="rep" value="{h(rep_sel)}">' if rep_sel else ""
    hidden_data_ini = f'<input type="hidden" name="agenda_data_ini" value="{h(data_ini)}">' if data_ini else ""
    hidden_data_fim = f'<input type="hidden" name="agenda_data_fim" value="{h(data_fim)}">' if data_fim else ""

    aviso_html = ""
    if agenda_auto_carregada:
        intervalo_txt = ""
        if data_ini or data_fim:
            intervalo_txt = f" | intervalo: {h(data_ini or '...')} até {h(data_fim or '...')}"
        aviso_html += (
            '<div class="small" style="margin-top:6px; color:#065f46; font-weight:700;">'
            'Agenda preenchida automaticamente com clientes da BASE pelo dia da semana, usando o valor de 2025.'
            f'{intervalo_txt}'
            '</div>'
        )

    if agenda_excedentes:
        aviso_html += (
            '<div class="small" style="margin-top:6px; color:#9a3412; font-weight:700;">'
            f'{len(agenda_excedentes)} cliente(s) ficaram fora da grade por exceder o limite de 4 atendimentos em um ou mais dias.'
            '</div>'
        )

    return f"""
    <div class="card" style="margin-top:10px;">
      <form method="get" action="{url_for('admin_dashboard')}" class="no-print" style="margin-bottom:10px;">
        <div class="grid" style="grid-template-columns: 1.2fr 1fr 1fr auto;">
          {hidden_sup}
          {hidden_rep}
          <input type="hidden" name="auto_agenda" value="1">
          <div>
            <label>Data inicial da agenda</label>
            <input type="date" name="agenda_data_ini" value="{h(data_ini)}">
          </div>
          <div>
            <label>Data final da agenda</label>
            <input type="date" name="agenda_data_fim" value="{h(data_fim)}">
          </div>
          <div style="display:flex; align-items:flex-end; gap:8px;">
            <button type="submit" class="agenda-save-btn" style="background:#2563eb;">Carregar da Base</button>
          </div>
        </div>
      </form>

      <form method="post" action="{url_for('salvar_agenda')}">
        <input type="hidden" name="rep_code_agenda" value="{h(rep_code)}">
        {hidden_sup}
        {hidden_rep}
        {hidden_data_ini}
        {hidden_data_fim}

        <div class="agenda-topbar">
          <div class="agenda-rep-label">
            Agenda semanal do representante <b>{h(rep_code)}</b>
          </div>
          <div>
            <button type="submit" class="agenda-save-btn">Salvar Agenda</button>
          </div>
        </div>

        {aviso_html}

        <div class="agenda-wrapper">
          <table class="agenda-table">
            <thead>
              {''.join(header_top)}
              {''.join(header_sub)}
            </thead>
            <tbody>
              {''.join(body_rows)}
            </tbody>
          </table>
        </div>
      </form>
    </div>
    """


# =========================
# GOOGLE SHEETS
# =========================
def _load_service_account_info():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    elif GOOGLE_SA_JSON_B64:
        b64 = GOOGLE_SA_JSON_B64.strip()
        b64 += "=" * (-len(b64) % 4)
        info = json.loads(base64.b64decode(b64).decode("utf-8"))
    else:
        raise RuntimeError("Faltou GOOGLE_SERVICE_ACCOUNT_JSON (ou GOOGLE_SA_JSON_B64) nas variáveis de ambiente.")

    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    return info


def connect_gs_by_key(sheet_key_or_url):
    resolved_key = extract_google_sheet_id(sheet_key_or_url)
    if not resolved_key:
        raise RuntimeError("Sheet ID não informado.")

    info = _load_service_account_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    try:
        return gc.open_by_key(resolved_key)
    except Exception as e:
        raise RuntimeError(friendly_gspread_error(e))


def connect_gs():
    if not SHEET_ID:
        raise RuntimeError("Faltou SHEET_ID nas variáveis de ambiente.")
    return connect_gs_by_key(SHEET_ID)


def connect_gold_gs():
    gold_target = resolve_gold_sheet_target()
    if not gold_target:
        raise RuntimeError(
            "Faltou configurar a planilha de CLIENTES GOLD. "
            "Defina GOLD_SHEET_ID ou GOLD_SHEET_URL."
        )
    return connect_gs_by_key(gold_target)


def safe_get_all_records(ws):
    try:
        return ws.get_all_records()
    except Exception:
        return []


def safe_get_raw_rows(ws):
    try:
        values = ws.get_all_values()
    except Exception:
        return [], []

    if not values:
        return [], []

    headers = [norm(x) for x in values[0]]
    rows = []

    for raw in values[1:]:
        if len(raw) < len(headers):
            raw = raw + [""] * (len(headers) - len(raw))
        elif len(raw) > len(headers):
            raw = raw[:len(headers)]

        row = {headers[i]: raw[i] for i in range(len(headers))}
        rows.append(row)

    return headers, rows


def ensure_headers(ws, headers):
    current = [norm(x) for x in ws.row_values(1)]
    if not current:
        ws.append_row(headers, value_input_option="USER_ENTERED")
    elif current != headers:
        ws.update("A1", [headers], value_input_option="USER_ENTERED")


def ensure_edicoes_worksheet(sh):
    headers = [
        "timestamp",
        "user_type",
        "user_login",
        "rep_code",
        "client_key",
        "Data Agenda Visita",
        "Mês",
        "Semana Atendimento",
        "Status Cliente",
        "Observações"
    ]

    try:
        ws = sh.worksheet(WS_EDICOES)
    except WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title=WS_EDICOES, rows="5000", cols="30")
        except Exception as e:
            raise RuntimeError(
                f"Não foi possível acessar/criar a aba '{WS_EDICOES}'. "
                f"Crie essa aba manualmente na planilha ou conceda permissão de Editor à service account. "
                f"Detalhe: {friendly_gspread_error(e)}"
            )

    ensure_headers(ws, headers)
    return ws


def ensure_base_tracking_columns(ws_base):
    headers = [norm(x) for x in ws_base.row_values(1)]
    if not headers:
        raise RuntimeError("A aba BASE está sem cabeçalho na linha 1.")

    required = [
        "Data Agenda Visita",
        "Mês",
        "Semana Atendimento",
        "Status Cliente",
        "Observações"
    ]

    changed = False
    for col in required:
        if col not in headers:
            headers.append(col)
            changed = True

    if changed:
        ws_base.update("A1", [headers], value_input_option="USER_ENTERED")
        headers = [norm(x) for x in ws_base.row_values(1)]

    return headers


def get_optional_worksheet(sh, ws_name):
    try:
        return sh.worksheet(ws_name)
    except WorksheetNotFound:
        return None
    except Exception:
        return None


def get_base_structure_cached(sh):
    cache_key = f"base_structure::{extract_google_sheet_id(SHEET_ID)}::{WS_BASE}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    ws_base = sh.worksheet(WS_BASE)
    headers = ensure_base_tracking_columns(ws_base)
    rows = ws_base.get_all_values()

    if not rows:
        result = (headers, [])
        cache_set(cache_key, result, BASE_CACHE_TTL)
        return result

    final_headers = [norm(x) for x in rows[0]]
    data_rows = []

    for raw in rows[1:]:
        if len(raw) < len(final_headers):
            raw = raw + [""] * (len(final_headers) - len(raw))
        elif len(raw) > len(final_headers):
            raw = raw[:len(final_headers)]

        data_rows.append({final_headers[i]: raw[i] for i in range(len(final_headers))})

    result = (final_headers, data_rows)
    cache_set(cache_key, result, BASE_CACHE_TTL)
    return result


def get_listas_records_cached(sh):
    cache_key = f"listas::{extract_google_sheet_id(SHEET_ID)}::{WS_LISTAS}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    ws_listas = get_optional_worksheet(sh, WS_LISTAS)
    rows = safe_get_all_records(ws_listas) if ws_listas else []
    cache_set(cache_key, rows, LISTAS_CACHE_TTL)
    return rows


def invalidate_main_sheet_cache():
    cache_del_prefix(f"base_structure::{extract_google_sheet_id(SHEET_ID)}::{WS_BASE}")
    cache_del_prefix(f"listas::{extract_google_sheet_id(SHEET_ID)}::{WS_LISTAS}")
    cache_del_prefix(f"sheet_info::{extract_google_sheet_id(SHEET_ID)}")
    cache_del_prefix(f"rep_name::{extract_google_sheet_id(SHEET_ID)}::")


def try_get_rep_name(rep_code):
    rep_code = norm(rep_code)
    if not rep_code:
        return ""

    cache_key = f"rep_name::{extract_google_sheet_id(SHEET_ID)}::{rep_code}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        sh = connect_gs()
        headers, base_rows = get_base_structure_cached(sh)

        rep_col = pick_col_flexible(headers, [
            "Codigo Representante", "Código Representante",
            "CODIGO REPRESENTANTE", "COD_REP"
        ])
        nome_rep_col = pick_col_flexible(headers, [
            "Representante", "Nome Representante", "REPRESENTANTE"
        ])

        if not rep_col or not nome_rep_col:
            cache_set(cache_key, "", REP_NAME_CACHE_TTL)
            return ""

        for row in base_rows:
            if norm(row.get(rep_col, "")) == rep_code:
                val = norm(row.get(nome_rep_col, ""))
                cache_set(cache_key, val, REP_NAME_CACHE_TTL)
                return val

        cache_set(cache_key, "", REP_NAME_CACHE_TTL)
        return ""
    except Exception:
        return ""


def get_gold_info_by_rep(rep_code):
    rep_code = norm(rep_code)

    info = {
        "total_gold": 0,
        "gold_rows": [],
        "sheet_title": "",
        "worksheet_title": "",
        "rep_col": "",
        "codigo_col": "",
        "cliente_col": "",
        "grupo_col": "",
        "supervisor_col": "",
        "ok": False,
        "error": "",
        "resolved_sheet_id": resolve_gold_sheet_target(),
    }

    if not rep_code:
        info["error"] = "Selecione um representante para consultar CLIENTES GOLD."
        return info

    try:
        sh_gold = connect_gold_gs()
        info["sheet_title"] = norm(getattr(sh_gold, "title", ""))

        try:
            ws_gold = sh_gold.worksheet(GOLD_WS)
        except WorksheetNotFound:
            raise RuntimeError(
                f"A aba '{GOLD_WS}' não foi encontrada na planilha de CLIENTES GOLD."
            )

        info["worksheet_title"] = norm(getattr(ws_gold, "title", ""))

        headers_gold, rows_gold = safe_get_raw_rows(ws_gold)
        if not headers_gold:
            raise RuntimeError("A aba de clientes gold está vazia ou sem cabeçalho.")

        rep_gold_col = pick_col_flexible(headers_gold, [
            "Cod. Representante", "Cod Representante", "Código Representante",
            "Codigo Representante", "Representante", "COD_REP", "REP"
        ])

        codigo_gold_col = pick_col_flexible(headers_gold, [
            "Codigo", "Código", "Codigo Cliente", "Código Cliente",
            "Codigo Grupo Cliente", "Código Grupo Cliente",
            "Cod Cliente", "Cod. Cliente", "Cod Grupo Cliente", "Cod. Grupo Cliente"
        ])

        cliente_gold_col = pick_col_flexible(headers_gold, [
            "Cliente / Grupo", "Cliente Grupo", "Cliente", "Nome Cliente",
            "Razao Social", "Razão Social", "Fantasia", "Nome"
        ])

        grupo_gold_col = pick_col_flexible(headers_gold, [
            "Grupo Cliente / Cliente", "Grupo Cliente Cliente", "Grupo Cliente", "Grupo", "Cliente / Grupo"
        ])

        supervisor_gold_col = pick_col_flexible(headers_gold, [
            "Supervisor", "Cod. Supervisor", "Código Supervisor", "Codigo Supervisor"
        ])

        info["rep_col"] = rep_gold_col or ""
        info["codigo_col"] = codigo_gold_col or ""
        info["cliente_col"] = cliente_gold_col or ""
        info["grupo_col"] = grupo_gold_col or ""
        info["supervisor_col"] = supervisor_gold_col or ""

        if not rep_gold_col:
            raise RuntimeError(
                "Não encontrei a coluna do representante na planilha GOLD. "
                "Verifique o cabeçalho da aba informada em GOLD_WS."
            )

        gold_rows = []
        for row in rows_gold:
            rep_val = norm(row.get(rep_gold_col, ""))
            rep_val_num = rep_val.lstrip("0") or "0"
            rep_code_num = rep_code.lstrip("0") or "0"

            if rep_val == rep_code or rep_val_num == rep_code_num:
                codigo_val = norm(row.get(codigo_gold_col, "")) if codigo_gold_col else ""
                cliente_val = norm(row.get(cliente_gold_col, "")) if cliente_gold_col else ""
                grupo_val = norm(row.get(grupo_gold_col, "")) if grupo_gold_col else ""
                supervisor_val = norm(row.get(supervisor_gold_col, "")) if supervisor_gold_col else ""

                if not cliente_val and grupo_val:
                    cliente_val = grupo_val
                if not grupo_val and cliente_val:
                    grupo_val = cliente_val

                gold_rows.append({
                    "codigo": codigo_val,
                    "cliente_grupo": cliente_val,
                    "grupo_cliente_cliente": grupo_val,
                    "rep": rep_val,
                    "supervisor": supervisor_val
                })

        gold_rows.sort(
            key=lambda x: (
                norm(x.get("cliente_grupo", "")),
                norm(x.get("grupo_cliente_cliente", "")),
                norm(x.get("codigo", ""))
            )
        )

        info["gold_rows"] = gold_rows
        info["total_gold"] = len(gold_rows)
        info["ok"] = True
        return info

    except Exception as e:
        info["error"] = friendly_gspread_error(e)
        return info


def build_debug_sheet_info(sh=None):
    cache_key = f"sheet_info::{extract_google_sheet_id(SHEET_ID)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        if sh is None:
            sh = connect_gs()

        abas = [ws.title for ws in sh.worksheets()]
        result = {
            "sheet_id": extract_google_sheet_id(SHEET_ID),
            "spreadsheet_title": norm(getattr(sh, "title", "")),
            "worksheets": abas,
            "ok": True,
        }
        cache_set(cache_key, result, DEBUG_SHEETINFO_CACHE_TTL)
        return result
    except Exception as e:
        result = {
            "sheet_id": extract_google_sheet_id(SHEET_ID),
            "spreadsheet_title": "",
            "worksheets": [],
            "ok": False,
            "error": friendly_gspread_error(e),
        }
        cache_set(cache_key, result, DEBUG_SHEETINFO_CACHE_TTL)
        return result


# =========================
# PARÂMETROS COMERCIAIS
# =========================
def ensure_parametros_comerciais_worksheet(sh):
    headers = [
        "dias_uteis_inverno",
        "dias_uteis_verao",
        "qtd_positivacao_carteira",
        "atualizado_em",
        "atualizado_por"
    ]

    try:
        ws = sh.worksheet(WS_PARAMETROS_COMERCIAIS)
    except WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title=WS_PARAMETROS_COMERCIAIS, rows="50", cols="10")
        except Exception as e:
            raise RuntimeError(
                f"Não foi possível acessar/criar a aba '{WS_PARAMETROS_COMERCIAIS}'. "
                f"Detalhe: {friendly_gspread_error(e)}"
            )

    ensure_headers(ws, headers)

    vals = ws.get_all_values()
    if len(vals) < 2:
        ws.update("A2:E2", [["", "", "", "", ""]], value_input_option="USER_ENTERED")

    return ws


def get_parametros_comerciais(sh=None):
    info = {
        "dias_uteis_inverno": "",
        "dias_uteis_verao": "",
        "qtd_positivacao_carteira": "",
        "atualizado_em": "",
        "atualizado_por": "",
        "ok": False,
        "error": "",
    }

    try:
        if sh is None:
            sh = connect_gs()

        ws = ensure_parametros_comerciais_worksheet(sh)
        values = ws.get_all_values()
        headers = [norm(x) for x in values[0]] if values else []
        row = values[1] if len(values) > 1 else []
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        data = {headers[i]: row[i] for i in range(len(headers))}
        info.update({
            "dias_uteis_inverno": norm(data.get("dias_uteis_inverno", "")),
            "dias_uteis_verao": norm(data.get("dias_uteis_verao", "")),
            "qtd_positivacao_carteira": norm(data.get("qtd_positivacao_carteira", "")),
            "atualizado_em": norm(data.get("atualizado_em", "")),
            "atualizado_por": norm(data.get("atualizado_por", "")),
            "ok": True,
        })
        return info
    except Exception as e:
        info["error"] = norm(str(e))
        return info


def render_parametros_comerciais_box_html(parametros, compact=False):
    inverno = norm(parametros.get("dias_uteis_inverno", "")) or "-"
    verao = norm(parametros.get("dias_uteis_verao", "")) or "-"
    positivacao = norm(parametros.get("qtd_positivacao_carteira", "")) or "-"
    atualizado_em = norm(parametros.get("atualizado_em", ""))
    atualizado_por = norm(parametros.get("atualizado_por", ""))

    rodape = ""
    if atualizado_em or atualizado_por:
        rodape = f'<div class="small" style="margin-top:8px;">Atualizado em: <b>{h(atualizado_em or "-")}</b> | Por: <b>{h(atualizado_por or "-")}</b></div>'

    if compact:
        return f"""
        <div class="dash-coverage-box" style="display:flex; flex-wrap:wrap; gap:10px; justify-content:center;">
          <span>Inverno: <b>{h(inverno)}</b></span>
          <span>Verão: <b>{h(verao)}</b></span>
          <span>Positivação: <b>{h(positivacao)}</b></span>
        </div>
        {rodape}
        """

    return f"""
    <div class="card">
      <div style="font-size:18px; font-weight:700; margin-bottom:10px;">Parâmetros Comerciais</div>
      <div class="grid-2" style="grid-template-columns: repeat(3, 1fr);">
        <div class="pill" style="padding:12px; border-radius:12px;">Dias úteis coleção inverno: <b>{h(inverno)}</b></div>
        <div class="pill" style="padding:12px; border-radius:12px;">Dias úteis coleção verão: <b>{h(verao)}</b></div>
        <div class="pill" style="padding:12px; border-radius:12px;">Positivação p/ cobrir carteira: <b>{h(positivacao)}</b></div>
      </div>
      {rodape}
    </div>
    """


def render_parametros_comerciais_form_html(parametros):
    return f"""
    <div class="card no-print">
      <div style="font-size:18px; font-weight:700; margin-bottom:10px;">Parâmetros Comerciais</div>
      <form method="post" action="{url_for('salvar_parametros_comerciais')}">
        <div class="grid">
          <div>
            <label>Dias úteis coleção inverno</label>
            <input type="number" min="0" step="1" name="dias_uteis_inverno" value="{h(parametros.get('dias_uteis_inverno', ''))}" placeholder="Ex.: 22">
          </div>
          <div>
            <label>Dias úteis coleção verão</label>
            <input type="number" min="0" step="1" name="dias_uteis_verao" value="{h(parametros.get('dias_uteis_verao', ''))}" placeholder="Ex.: 24">
          </div>
          <div>
            <label>Qtd. positivação para cobrir carteira</label>
            <input type="number" min="0" step="1" name="qtd_positivacao_carteira" value="{h(parametros.get('qtd_positivacao_carteira', ''))}" placeholder="Ex.: 180">
          </div>
          <div style="display:flex; align-items:flex-end; gap:8px;">
            <button type="submit">Salvar parâmetros</button>
          </div>
        </div>
        <div class="small" style="margin-top:8px;">Esses campos aparecem no cabeçalho do dashboard, no bloco de cobertura e também na carteira.</div>
      </form>
    </div>
    """


# =========================
# ERROR HANDLER
# =========================
@app.errorhandler(Exception)
def handle_any_exception(e):
    app.logger.error("ERRO NÃO TRATADO:\n%s", traceback.format_exc())
    msg = traceback.format_exc()

    body = f"""
    <div class='card'>
      <b>Erro:</b><br>
      <pre style='white-space:pre-wrap'>{h(msg)}</pre>
    </div>
    """

    current_user_photo = ""
    if session.get("user_type") == "rep":
        current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

    return render_template_string(
        BASE_HTML,
        title=APP_TITLE,
        subtitle="Falha no servidor",
        logged=require_login(),
        user_login=session.get("user_login", ""),
        user_name=session.get("rep_name", ""),
        user_type=session.get("user_type", ""),
        user_photo_url=current_user_photo,
        body=body
    ), 500


# =========================
# TEMPLATES
# =========================
BASE_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; background: #f5f6f8; color: #111827; }
    .topbar { background: #ffffff; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #d1d5db; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
    .topbar-right { display: flex; align-items: center; gap: 10px; }
    .topbar-avatar { width: 36px; height: 36px; border-radius: 50%; object-fit: cover; border: 1px solid #d1d5db; background: #f8fafc; }

    .container { padding: 12px; }
    .card { background: #ffffff; border: 1px solid #d1d5db; border-radius: 12px; padding: 14px; margin-bottom: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.04); }

    .rep-card { display: flex; align-items: center; gap: 16px; }
    .rep-photo { width: 88px; height: 88px; border-radius: 50%; object-fit: cover; border: 2px solid #d1d5db; background: #f8fafc; flex-shrink: 0; }
    .rep-photo-placeholder { width: 88px; height: 88px; border-radius: 50%; border: 2px solid #d1d5db; background: #f8fafc; display: flex; align-items: center; justify-content: center; color: #6b7280; font-size: 12px; text-align: center; flex-shrink: 0; padding: 6px; box-sizing: border-box; }

    label { font-size: 12px; color: #4b5563; display: block; margin-bottom: 4px; font-weight: 600; }
    input, select {
      width: 100%;
      padding: 9px;
      border-radius: 10px;
      border: 1px solid #cbd5e1;
      background: #ffffff;
      color: #111827;
      box-sizing: border-box;
      font-family: Arial, sans-serif;
    }

    input:focus, select:focus {
      outline: none;
      border-color: #2563eb;
      box-shadow: 0 0 0 3px rgba(37,99,235,0.12);
    }

    button, .btn-link {
      padding: 9px 13px;
      border-radius: 10px;
      border: 0;
      background: #2563eb;
      color: #fff;
      cursor: pointer;
      font-weight: 600;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      box-sizing: border-box;
    }

    button.secondary, .btn-link.secondary { background: #6b7280; }
    button.danger, .btn-link.danger { background: #dc2626; }
    .btn-link.dark { background: #111827; }
    .btn-link.orange { background: #f97316; }

    table { width: 100%; border-collapse: collapse; font-size: 13px; background: #ffffff; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 8px; vertical-align: top; }
    th { position: sticky; top: 0; background: #f8fafc; color: #374151; text-align: left; z-index: 2; }

    .grid { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }

    .msg { padding: 10px 12px; border-radius: 10px; margin-bottom: 10px; font-weight: 600; }
    .ok { background: #ecfdf5; border: 1px solid #86efac; color: #166534; }
    .err { background: #fef2f2; border: 1px solid #fca5a5; color: #991b1b; }

    .pill { padding: 3px 8px; border-radius: 999px; font-size: 12px; background: #f3f4f6; border: 1px solid #d1d5db; display: inline-block; color: #111827; }
    .small { color: #6b7280; font-size: 12px; }
    .nowrap { white-space: nowrap; }
    .money { font-variant-numeric: tabular-nums; }

    .login-wrap { min-height: calc(100vh - 90px); display: flex; align-items: center; justify-content: center; padding: 24px; }
    .login-card { width: 100%; max-width: 520px; text-align: center; }
    .login-logo { max-width: 220px; width: 100%; height: auto; margin: 0 auto 18px auto; display: block; }
    .login-title { margin-top: 0; margin-bottom: 6px; color: #111827; }
    .login-subtitle { margin-top: 0; margin-bottom: 20px; color: #6b7280; font-size: 14px; }

    .row-red { background: rgba(220,38,38,0.16); }
    .row-orange { background: rgba(249,115,22,0.16); }
    .row-yellow { background: rgba(234,179,8,0.18); }
    .row-green { background: rgba(34,197,94,0.16); }
    .row-blue { background: rgba(56,189,248,0.14); }

    .rep-table-wrap {
      overflow: auto;
      max-height: 72vh;
    }

    .rep-table {
      width: max-content;
      min-width: 100%;
      border-collapse: separate;
      border-spacing: 0;
    }

    .rep-table th,
    .rep-table td {
      background-clip: padding-box;
    }

    .rep-table th.sticky-col,
    .rep-table td.sticky-col,
    .rep-table th.sticky-col-2,
    .rep-table td.sticky-col-2 {
      position: sticky;
      z-index: 4;
      background-clip: padding-box;
    }

    .rep-table th.sticky-col,
    .rep-table td.sticky-col {
      left: 0;
    }

    .rep-table th.sticky-col-2,
    .rep-table td.sticky-col-2 {
      left: 110px;
    }

    .rep-table thead th.sticky-col,
    .rep-table thead th.sticky-col-2 {
      z-index: 6;
      background: #f8fafc !important;
    }

    .rep-table tbody tr.row-red td.sticky-col,
    .rep-table tbody tr.row-red td.sticky-col-2 {
      background: #ead1d1 !important;
    }

    .rep-table tbody tr.row-orange td.sticky-col,
    .rep-table tbody tr.row-orange td.sticky-col-2 {
      background: #f1dbc9 !important;
    }

    .rep-table tbody tr.row-yellow td.sticky-col,
    .rep-table tbody tr.row-yellow td.sticky-col-2 {
      background: #efe4be !important;
    }

    .rep-table tbody tr.row-green td.sticky-col,
    .rep-table tbody tr.row-green td.sticky-col-2 {
      background: #d1e8d7 !important;
    }

    .rep-table tbody tr.row-blue td.sticky-col,
    .rep-table tbody tr.row-blue td.sticky-col-2 {
      background: #d2e5ef !important;
    }

    .rep-table tbody td.sticky-col,
    .rep-table tbody td.sticky-col-2 {
      box-shadow: inset -1px 0 0 #e5e7eb;
    }

    .rep-table tbody td.sticky-col-2,
    .rep-table thead th.sticky-col-2 {
      box-shadow: 2px 0 6px rgba(15, 23, 42, 0.08);
    }

    .debug-card {
      background: #0f172a;
      color: #e2e8f0;
      border: 1px solid #1e293b;
    }
    .debug-card .line {
      margin-bottom: 6px;
      word-break: break-word;
    }
    .debug-card .title {
      font-size: 16px;
      font-weight: 700;
      margin-bottom: 12px;
    }

    .dash-page {
      display: flex;
      flex-direction: column;
      gap: 12px;
      align-items: center;
    }

    .a3-page {
      width: min(100%, 1560px);
      background: #ffffff;
    }

    .print-scale-wrap {
      width: 100%;
      transform-origin: top left;
    }

    .dash-shell {
      background: #ffffff;
      border: 1px solid #cfd4dc;
      border-top: 3px solid #f97316;
      border-bottom: 3px solid #f97316;
      padding: 6px;
      border-radius: 8px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.05);
      width: 100%;
      box-sizing: border-box;
      overflow: hidden;
    }

    .dash-header {
      display: grid;
      grid-template-columns: 74px 1.35fr 1fr 64px;
      gap: 8px;
      align-items: center;
      border-bottom: 2px solid #f97316;
      padding-bottom: 6px;
      margin-bottom: 8px;
    }

    .dash-avatar {
      width: 62px;
      height: 62px;
      border-radius: 8px;
      object-fit: cover;
      border: 1px solid #d1d5db;
      background: #f8fafc;
    }

    .dash-avatar-placeholder {
      width: 62px;
      height: 62px;
      border-radius: 8px;
      border: 1px solid #d1d5db;
      background: #f8fafc;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #6b7280;
      font-size: 10px;
      text-align: center;
      padding: 6px;
      box-sizing: border-box;
    }

    .dash-title-wrap { min-width: 0; }
    .dash-main-title {
      font-size: 17px;
      font-weight: 800;
      text-transform: uppercase;
      text-align: center;
      margin-bottom: 3px;
    }

    .dash-subline {
      font-size: 10px;
      color: #374151;
      line-height: 1.35;
    }

    .dash-meta-box {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px;
    }

    .dash-metric {
      border: 1px solid #d1d5db;
      border-radius: 8px;
      padding: 5px;
      background: #fafafa;
    }

    .dash-metric-label {
      font-size: 9px;
      color: #6b7280;
      font-weight: 700;
      text-transform: uppercase;
      margin-bottom: 2px;
    }

    .dash-metric-value {
      font-size: 15px;
      font-weight: 800;
      color: #111827;
    }

    .dash-kidy-logo {
      max-width: 54px;
      width: 100%;
      height: auto;
      justify-self: end;
    }

    .dash-main-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 8px;
      align-items: stretch;
      margin-bottom: 8px;
    }

    .dash-left-stack {
      display: flex;
      flex-direction: column;
      gap: 8px;
      min-width: 0;
    }

    .dash-city-column {
      display: flex;
      flex-direction: column;
      min-width: 0;
    }

    .dash-city-column .dash-panel {
      flex: 1;
    }

    .dash-row-top {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      align-items: stretch;
    }

    .dash-row-middle {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      align-items: stretch;
    }

    .dash-row-bottom {
      display: block;
      flex: 1;
    }

    .dash-right-stack {
      display: flex;
      flex-direction: column;
      gap: 8px;
      height: 100%;
      min-height: 100%;
    }

    .dash-panel {
    display: flex;
    flex-direction: column;
    height: 100%;
    }

    .dash-panel-title {
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
      color: #111827;
      background: #f3f4f6;
      border-bottom: 1px solid #d1d5db;
      padding: 5px 8px;
      text-align: center;
    }

    .dash-panel-body {
      padding: 6px;
      box-sizing: border-box;
    }

    .dash-panel-body-map {
    padding: 6px;
    box-sizing: border-box;
    min-height: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
}

    .dash-table-mini {
      width: 100%;
      border-collapse: collapse;
      font-size: 9px;
    }

    .dash-table-mini th,
    .dash-table-mini td {
      border: 1px solid #d1d5db;
      padding: 2px 4px;
      line-height: 1.15;
    }

    .dash-table-mini th {
      background: #e5e7eb;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 8px;
      position: static;
    }

    .dash-table-big {
      width: 100%;
      border-collapse: collapse;
      font-size: 9px;
    }

    .dash-table-big th,
    .dash-table-big td {
      border: 1px solid #d1d5db;
      padding: 3px 4px;
      line-height: 1.12;
      vertical-align: middle;
    }

    .dash-table-big th {
      background: #e5e7eb;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 8px;
      position: static;
    }

    .dash-map-placeholder {
      min-height: 285px;
      background: #ecfeff;
      border: 2px dashed #06b6d4;
      color: #155e75;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      font-size: 12px;
      border-radius: 6px;
      padding: 10px;
      box-sizing: border-box;
    }

    .dash-gold-box {
      min-height: 58px;
      background: #fef3c7;
      border: 2px dashed #f59e0b;
      color: #92400e;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      font-size: 11px;
      border-radius: 6px;
      padding: 8px;
      box-sizing: border-box;
      flex-direction: column;
      gap: 4px;
    }

    .dash-coverage-box {
      min-height: 82px;
      background: #f8fafc;
      border: 2px dashed #94a3b8;
      color: #334155;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      font-size: 11px;
      border-radius: 6px;
      padding: 8px;
      box-sizing: border-box;
    }

    .dash-summary-box {
      min-height: 130px;
      background: #f8fafc;
      border: 2px dashed #94a3b8;
      color: #334155;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      font-size: 11px;
      border-radius: 6px;
      padding: 8px;
      box-sizing: border-box;
    }

    .print-toolbar {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }

    .print-note {
      font-size: 11px;
      color: #6b7280;
    }

    .status-chip {
      display: inline-block;
      min-width: 64px;
      padding: 2px 5px;
      border-radius: 999px;
      text-align: center;
      font-size: 8px;
      font-weight: 700;
      border: 1px solid rgba(0,0,0,0.08);
    }

    .chip-red { background: rgba(220,38,38,0.18); color: #991b1b; }
    .chip-orange { background: rgba(249,115,22,0.18); color: #9a3412; }
    .chip-yellow { background: rgba(234,179,8,0.22); color: #854d0e; }
    .chip-green { background: rgba(34,197,94,0.18); color: #166534; }
    .chip-blue { background: rgba(56,189,248,0.18); color: #0c4a6e; }
    .chip-gray { background: #e5e7eb; color: #374151; }

    .agenda-wrapper { overflow:auto; width:100%; }
    .agenda-table { width:100%; border-collapse:collapse; font-size:10px; }
    .agenda-table th, .agenda-table td {
      border:1px solid #9ca3af;
      padding:4px;
      vertical-align:middle;
      background:#ffffff;
    }
    .agenda-table thead th {
      background:#f3f4f6;
      text-align:center;
      font-size:10px;
      font-weight:800;
      text-transform:uppercase;
      position:static;
    }
    .agenda-input {
      width:100%;
      min-width:80px;
      padding:5px 6px;
      border-radius:4px;
      border:1px solid #cbd5e1;
      font-size:10px;
      box-sizing:border-box;
    }
    .agenda-valor {
      min-width:58px;
      text-align:center;
    }
    .agenda-topbar {
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:8px;
      margin-bottom:6px;
      flex-wrap:wrap;
    }
    .agenda-rep-label {
      font-size:11px;
      color:#374151;
      font-weight:700;
    }
    .agenda-save-btn {
      background:#f97316;
      padding:8px 12px;
      border-radius:8px;
      border:0;
      color:#fff;
      cursor:pointer;
      font-weight:700;
    }

    .no-break { page-break-inside: avoid; break-inside: avoid; }

    @page {
      size: A3 landscape;
      margin: 5mm;
    }

    @media print {
      @page {
        size: A3 landscape;
        margin: 0;
      }

      html, body {
        width: 420mm !important;
        height: 297mm !important;
        background: #ffffff !important;
        overflow: hidden !important;
        -webkit-print-color-adjust: exact !important;
        print-color-adjust: exact !important;
      }

      .topbar,
      .no-print,
      .msg {
        display: none !important;
      }

      body {
        margin: 0 !important;
        padding: 0 !important;
      }

      .container {
        padding: 0 !important;
        margin: 0 !important;
        width: 100% !important;
        overflow: hidden !important;
      }

      .dash-page {
        gap: 0 !important;
        width: 100% !important;
        align-items: stretch !important;
      }

      .a3-page {
        width: 420mm !important;
        height: 297mm !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: hidden !important;
        background: #ffffff !important;
        page-break-after: avoid !important;
        break-after: avoid !important;
      }

      .print-scale-wrap {
        width: 116% !important;
        transform: scale(0.86) !important;
        transform-origin: top left !important;
      }

      .dash-shell {
        width: 100% !important;
        min-height: 0 !important;
        height: auto !important;
        padding: 4mm !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        overflow: hidden !important;
      }

      .dash-header,
      .dash-row-top,
      .dash-row-bottom,
      .dash-right-stack,
      .dash-panel,
      .dash-panel-body,
      .dash-panel-body-map,
      .agenda-wrapper,
      .agenda-table {
        break-inside: avoid !important;
        page-break-inside: avoid !important;
      }

      .dash-header {
        margin-bottom: 5px !important;
        padding-bottom: 4px !important;
      }

      .dash-main-title {
        font-size: 15px !important;
      }

      .dash-subline {
        font-size: 9px !important;
        line-height: 1.15 !important;
      }

      .dash-metric {
        padding: 4px !important;
      }

      .dash-metric-label {
        font-size: 8px !important;
      }

      .dash-metric-value {
        font-size: 13px !important;
      }

      .dash-panel-title {
        font-size: 10px !important;
        padding: 4px 6px !important;
      }

      .dash-panel-body {
        padding: 4px !important;
      }

      .dash-panel-body-map {
        padding: 3px !important;
        min-height: 330px !important;
      }

      .dash-table-mini,
      .dash-table-big {
        font-size: 8px !important;
      }

      .dash-table-mini th, .dash-table-mini td,
      .dash-table-big th, .dash-table-big td {
        padding: 2px 3px !important;
        line-height: 1.05 !important;
      }

      .dash-gold-box,
      .dash-coverage-box,
      .dash-summary-box {
        min-height: unset !important;
        padding: 5px !important;
        font-size: 9px !important;
      }

      .agenda-table {
        font-size: 7.5px !important;
      }

      .agenda-table th,
      .agenda-table td {
        padding: 2px 3px !important;
      }

      .agenda-input {
        font-size: 7px !important;
        padding: 3px 4px !important;
        min-width: 40px !important;
      }

      .agenda-save-btn {
        display: none !important;
      }

      button,
      .btn-link {
        box-shadow: none !important;
      }
    }

    @media (max-width: 1200px) {
      .dash-header { grid-template-columns: 74px 1fr; }
      .dash-meta-box { grid-column: 1 / -1; }
      .dash-kidy-logo { justify-self: start; }
      .dash-row-top { grid-template-columns: 1fr; }
      .dash-row-bottom { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div><b>Acompanhamento de clientes</b> <span class="small">| {{ subtitle }}</span></div>
    <div class="topbar-right">
      {% if logged %}
        {% if user_type == 'admin' %}
          <a href="{{ url_for('admin_dashboard') }}" class="btn-link dark">Dashboard</a>
          <a href="{{ url_for('dashboard') }}" class="btn-link secondary">Carteira</a>
        {% endif %}
        {% if user_photo_url %}
          <img src="{{ user_photo_url }}" alt="Foto do usuário" class="topbar-avatar">
        {% endif %}
        <span class="pill">{{ user_name if user_name else user_login }} ({{ user_type }})</span>
        <a href="{{ url_for('logout') }}"><button class="danger">Sair</button></a>
      {% endif %}
    </div>
  </div>

  <div class="container">
    {% with messages = get_flashed_messages(with_categories=true) %}
      {% for cat, msg in messages %}
        <div class="msg {{ 'ok' if cat == 'ok' else 'err' }}">{{ msg }}</div>
      {% endfor %}
    {% endwith %}
    {{ body|safe }}
  </div>
<script>
function imprimirTelaA3() {
  window.print();
}
</script>
</body>
</html>
"""

LOGIN_BODY = """
<div class="login-wrap">
  <div class="card login-card">
    <img src="{{ logo_url }}" alt="Logo Kidy" class="login-logo">
    <h2 class="login-title">Acompanhamento de clientes</h2>
    <p class="login-subtitle">Faça login para acessar a carteira comercial</p>

    <form method="post">
      <div class="grid-2">
        <div>
          <label>Usuário</label>
          <input name="user" placeholder="admin ou código do representante" required>
        </div>
        <div>
          <label>Senha</label>
          <input name="pass" type="password" placeholder="admin123 ou o mesmo código" required>
        </div>
      </div>
      <div style="margin-top:12px;">
        <button type="submit">Entrar</button>
      </div>
    </form>
  </div>
</div>
"""


# =========================
# ROTAS
# =========================
@app.route("/", methods=["GET", "POST"])
def login():
    if require_login():
        if session.get("user_type") == "admin":
            return redirect(url_for("admin_dashboard"))
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        u = norm(request.form.get("user"))
        p = norm(request.form.get("pass"))

        if not u or not p:
            flash("Informe usuário e senha.", "err")
        elif u == ADMIN_USER and p == ADMIN_PASS:
            session.clear()
            session.permanent = True
            session["user_type"] = "admin"
            session["user_login"] = u
            session["rep_name"] = ""
            session["rep_code"] = ""
            flash("Logado como ADMIN.", "ok")
            return redirect(url_for("admin_dashboard"))
        elif u.isdigit() and p.isdigit() and u == p:
            rep_nome = try_get_rep_name(u)
            session.clear()
            session.permanent = True
            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u
            session["rep_name"] = rep_nome or f"Representante {u}"
            flash(f"Logado como {session['rep_name']}.", "ok")
            return redirect(url_for("dashboard"))
        else:
            flash("Login inválido.", "err")

    body = render_template_string(LOGIN_BODY, logo_url=LOGO_URL)
    return render_template_string(
        BASE_HTML,
        title=APP_TITLE,
        subtitle="Acesso",
        logged=False,
        user_login="",
        user_name="",
        user_type="",
        user_photo_url="",
        body=body
    )


@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada.", "ok")
    return redirect(url_for("login"))


@app.route("/salvar_agenda", methods=["POST"])
def salvar_agenda():
    if not require_login():
        flash("Sessão expirada. Faça login novamente.", "err")
        return redirect(url_for("login"))

    if not is_admin():
        flash("Somente admin pode salvar a agenda do dashboard.", "err")
        return redirect(url_for("dashboard"))

    rep_code = norm(request.form.get("rep_code_agenda", ""))
    sup = norm(request.form.get("sup", ""))
    rep = norm(request.form.get("rep", ""))
    agenda_data_ini = norm(request.form.get("agenda_data_ini", ""))
    agenda_data_fim = norm(request.form.get("agenda_data_fim", ""))

    if not rep_code:
        flash("Selecione um representante antes de salvar a agenda.", "err")
        return redirect(url_for("admin_dashboard", sup=sup, rep=rep))

    agenda_dict = _agenda_vazia()

    for dia in DIAS_SEMANA:
        for at in ATENDIMENTOS:
            cliente = norm(request.form.get(f"{dia}_{at}_cliente", ""))
            valor = norm(request.form.get(f"{dia}_{at}_valor", ""))
            agenda_dict[dia][at]["cliente"] = cliente
            agenda_dict[dia][at]["valor"] = valor

    try:
        salvar_agenda_rep(rep_code, agenda_dict)
        flash(f"Agenda do representante {rep_code} salva com sucesso na planilha Google Sheets.", "ok")
    except Exception as e:
        flash(f"Erro ao salvar agenda: {norm(str(e))}", "err")

    args = {}
    if sup:
        args["sup"] = sup
    if rep:
        args["rep"] = rep
    if agenda_data_ini:
        args["agenda_data_ini"] = agenda_data_ini
    if agenda_data_fim:
        args["agenda_data_fim"] = agenda_data_fim
    return redirect(url_for("admin_dashboard", **args))


@app.route("/salvar_parametros_comerciais", methods=["POST"])
def salvar_parametros_comerciais():
    if not require_login():
        flash("Sessão expirada. Faça login novamente.", "err")
        return redirect(url_for("login"))

    if not is_admin():
        flash("Somente admin pode salvar os parâmetros comerciais.", "err")
        return redirect(url_for("dashboard"))

    dias_uteis_inverno = norm(request.form.get("dias_uteis_inverno", ""))
    dias_uteis_verao = norm(request.form.get("dias_uteis_verao", ""))
    qtd_positivacao_carteira = norm(request.form.get("qtd_positivacao_carteira", ""))

    try:
        sh = connect_gs()
        ws = ensure_parametros_comerciais_worksheet(sh)
        atualizado_em = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        atualizado_por = norm(session.get("user_login", "")) or "admin"
        ws.update(
            "A2:E2",
            [[dias_uteis_inverno, dias_uteis_verao, qtd_positivacao_carteira, atualizado_em, atualizado_por]],
            value_input_option="USER_ENTERED"
        )
        flash("Parâmetros comerciais salvos com sucesso.", "ok")
    except Exception as e:
        flash(f"Erro ao salvar parâmetros comerciais: {norm(str(e))}", "err")

    return redirect(url_for("admin_dashboard"))


@app.route("/admin-dashboard", methods=["GET"])
def admin_dashboard():
    if not require_login():
        flash("Faça login para continuar.", "err")
        return redirect(url_for("login"))

    if not is_admin():
        flash("Acesso permitido somente para admin.", "err")
        return redirect(url_for("dashboard"))

    try:
        sh = connect_gs()
    except Exception as e:
        return render_error_page("Dashboard Admin", f"Erro ao conectar na planilha principal: {norm(str(e))}")

    try:
        debug_info = build_debug_sheet_info(sh) if DEBUG_MODE else {"worksheets": [], "sheet_id": "", "spreadsheet_title": ""}
        parametros_comerciais = get_parametros_comerciais(sh)
        headers, base_rows = get_base_structure_cached(sh)

        key_col = pick_col_flexible(headers, [
            "Codigo Grupo Cliente", "Código Grupo Cliente",
            "Codigo Cliente", "Código Cliente", "COD_CLIENTE", "Cliente"
        ])
        grupo_col = pick_col_flexible(headers, [
            "Grupo Cliente", "Nome Cliente", "Cliente",
            "Razao Social", "Razão Social", "Fantasia", "Nome"
        ])
        rep_col = pick_col_flexible(headers, [
            "Codigo Representante", "Código Representante",
            "CODIGO REPRESENTANTE", "COD_REP"
        ])
        nome_rep_col = pick_col_flexible(headers, [
            "Representante", "Nome Representante", "REPRESENTANTE"
        ])
        sup_col = pick_col_flexible(headers, [
            "Supervisor", "Código Supervisor", "Codigo Supervisor", "COD_SUP"
        ])
        cidade_col = resolve_city_col(headers)
        cnpj_col = resolve_cnpj_col(headers)

        t2024_col = pick_col_exact(headers, ["Total 2024 (PERIODO)"])
        t2025_col = pick_col_exact(headers, ["Total 2025 (PERIODO)"])
        t2026_col = pick_col_exact(headers, ["Total 2026 (PERIODO)"])

        status_cor_col = pick_col_exact(headers, ["STATUS COR", "Status Cor", "STATUSCOR", "StatusCor"])
        cliente_novo_col = pick_col_flexible(headers, ["Cliente Novo", "CLIENTE NOVO", "Novo", "NOVO"])

        data_agenda_col = pick_col_exact(headers, ["Data Agenda Visita"])
        mes_col = pick_col_exact(headers, ["Mês"])
        semana_col = pick_col_exact(headers, ["Semana Atendimento"])
        status_cliente_col = pick_col_exact(headers, ["Status Cliente"])
        observacoes_col = pick_col_exact(headers, ["Observações", "Observacao", "Observacoes"])

        sup_sel = norm(request.args.get("sup", ""))
        rep_sel = norm(request.args.get("rep", ""))
        q = norm(request.args.get("q", ""))
        data_ini = norm(request.args.get("data_ini", ""))
        data_fim = norm(request.args.get("data_fim", ""))
        filtro_mes = norm(request.args.get("filtro_mes", ""))
        filtro_semana = norm(request.args.get("filtro_semana", ""))
        agenda_data_ini = norm(request.args.get("agenda_data_ini", data_ini))
        agenda_data_fim = norm(request.args.get("agenda_data_fim", data_fim))
        auto_agenda = norm(request.args.get("auto_agenda", "")) == "1"

        sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if sup_col else []
        rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if rep_col else []

        filtered_rows = []
        q_lower = q.lower()

        for r in base_rows:
            if sup_sel and sup_col and norm(r.get(sup_col, "")) != sup_sel:
                continue
            if rep_sel and rep_col and norm(r.get(rep_col, "")) != rep_sel:
                continue

            if q_lower:
                grupo_val = norm(r.get(grupo_col, "")) if grupo_col else ""
                cidade_val = norm(r.get(cidade_col, "")) if cidade_col else ""
                ck_val = norm(r.get(key_col, "")) if key_col else ""
                nome_rep_val = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
                sup_val = norm(r.get(sup_col, "")) if sup_col else ""
                hay = f"{ck_val} {grupo_val} {cidade_val} {nome_rep_val} {sup_val}".lower()
                if q_lower not in hay:
                    continue

            data_agenda_val = norm(r.get(data_agenda_col, "")) if data_agenda_col else ""
            data_agenda_cmp = to_input_date(data_agenda_val)

            if data_ini:
                if not data_agenda_cmp or data_agenda_cmp < data_ini:
                    continue

            if data_fim:
                if not data_agenda_cmp or data_agenda_cmp > data_fim:
                    continue

            mes_val = norm(r.get(mes_col, "")) if mes_col else ""
            semana_val = norm(r.get(semana_col, "")) if semana_col else ""

            if filtro_mes and normalize_text_for_match(mes_val) != normalize_text_for_match(filtro_mes):
                continue

            if filtro_semana and normalize_text_for_match(semana_val) != normalize_text_for_match(filtro_semana):
                continue

            filtered_rows.append(r)

        header_rep_code = rep_sel
        header_rep_name = ""
        header_sup = sup_sel
        header_region = "REGIÃO / ÁREA"
        header_meta = "R$ 0,00"
        header_realizado = "R$ 0,00"
        header_percentual = "0,00%"
        rep_photo = get_rep_photo_src(header_rep_code) if header_rep_code else ""

        vendas_info = {
            "ok": False,
            "error": "",
            "representante": "",
            "supervisor": "",
            "meta": 0.0,
            "realizado": 0.0,
            "percentual": 0.0,
        }

        if header_rep_code:
            vendas_info = get_vendas_info_by_rep(header_rep_code)

            if vendas_info.get("ok"):
                header_rep_name = vendas_info.get("representante", "") or header_rep_name
                header_sup = vendas_info.get("supervisor", "") or header_sup
                header_meta = format_money_br(vendas_info.get("meta", 0.0))
                header_realizado = format_money_br(vendas_info.get("realizado", 0.0))
                header_percentual = f"{format_number_br(vendas_info.get('percentual', 0.0))}%"
            else:
                if rep_col:
                    for r in filtered_rows:
                        if norm(r.get(rep_col, "")) == header_rep_code:
                            header_rep_name = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
                            if not header_sup and sup_col:
                                header_sup = norm(r.get(sup_col, ""))
                            break

                total_realizado_2026 = sum(parse_number_br(r.get(t2026_col, "")) for r in filtered_rows) if t2026_col else 0.0
                header_realizado = format_money_br(total_realizado_2026)

        ranking_2026 = []
        if grupo_col and t2026_col:
            for r in filtered_rows:
                nome = norm(r.get(grupo_col, ""))
                valor = parse_number_br(r.get(t2026_col, ""))
                if nome and valor > 0:
                    status_cor_final, row_class, _ = resolve_status_cor_from_base(
                        r, status_cor_col=status_cor_col, cliente_novo_col=cliente_novo_col
                    )
                    ranking_2026.append({
                        "grupo": nome,
                        "valor": valor,
                        "status_cor": status_cor_final,
                        "row_class": row_class
                    })
            ranking_2026.sort(key=lambda x: x["valor"], reverse=True)
            ranking_2026 = ranking_2026[:10]

        ranking_2025 = []
        if grupo_col and t2025_col:
            for r in filtered_rows:
                nome = norm(r.get(grupo_col, ""))
                valor = parse_number_br(r.get(t2025_col, ""))
                if nome and valor > 0:
                    status_cor_final, row_class, _ = resolve_status_cor_from_base(
                        r, status_cor_col=status_cor_col, cliente_novo_col=cliente_novo_col
                    )
                    ranking_2025.append({
                        "grupo": nome,
                        "valor": valor,
                        "status_cor": status_cor_final,
                        "row_class": row_class
                    })
            ranking_2025.sort(key=lambda x: x["valor"], reverse=True)
            ranking_2025 = ranking_2025[:10]

        clientes_sem_compra = []
        if key_col and grupo_col and t2026_col:
            for r in filtered_rows:
                v2026 = parse_number_br(r.get(t2026_col, ""))
                if v2026 == 0:
                    status_cor_final, row_class, _ = resolve_status_cor_from_base(
                        r, status_cor_col=status_cor_col, cliente_novo_col=cliente_novo_col
                    )
                    clientes_sem_compra.append({
                        "codigo": norm(r.get(key_col, "")),
                        "grupo": norm(r.get(grupo_col, "")),
                        "t2024": parse_number_br(r.get(t2024_col, "")) if t2024_col else 0.0,
                        "t2025": parse_number_br(r.get(t2025_col, "")) if t2025_col else 0.0,
                        "t2026": parse_number_br(r.get(t2026_col, "")) if t2026_col else 0.0,
                        "data": norm(r.get(data_agenda_col, "")) if data_agenda_col else "",
                        "mes": norm(r.get(mes_col, "")) if mes_col else "",
                        "semana": norm(r.get(semana_col, "")) if semana_col else "",
                        "status": norm(r.get(status_cliente_col, "")) if status_cliente_col else "",
                        "status_cor": status_cor_final,
                        "row_class": row_class
                    })

            clientes_sem_compra.sort(
                key=lambda x: (x["t2025"], x["t2024"], x["grupo"]),
                reverse=True
            )

        agenda_override = None
        agenda_excedentes = []
        agenda_auto_carregada = False

        if header_rep_code and auto_agenda:
            agenda_rows_base = []
            for r in base_rows:
                rep_val = norm(r.get(rep_col, "")) if rep_col else ""
                if rep_val != header_rep_code:
                    continue

                data_base = to_input_date(norm(r.get(data_agenda_col, ""))) if data_agenda_col else ""
                if agenda_data_ini and (not data_base or data_base < agenda_data_ini):
                    continue
                if agenda_data_fim and (not data_base or data_base > agenda_data_fim):
                    continue

                dia_semana = get_dia_semana_ptbr(data_base)
                if dia_semana not in DIAS_SEMANA:
                    continue

                agenda_rows_base.append(r)

            agenda_override, agenda_excedentes = montar_agenda_da_base(
                rows=agenda_rows_base,
                data_col=data_agenda_col,
                grupo_col=grupo_col,
                valor_col=t2025_col,
            )
            agenda_auto_carregada = True

        agenda_semanal_html = render_agenda_semana_html(
            rep_code=header_rep_code,
            sup_sel=sup_sel,
            rep_sel=rep_sel,
            data_ini=agenda_data_ini,
            data_fim=agenda_data_fim,
            agenda_override=agenda_override,
            agenda_auto_carregada=agenda_auto_carregada,
            agenda_excedentes=agenda_excedentes,
        )

        gold_info = get_gold_info_by_rep(header_rep_code)
        total_gold = gold_info.get("total_gold", 0)

        total_carteira = len(filtered_rows)
        total_sem_compra = len(clientes_sem_compra)
        total_com_compra = max(total_carteira - total_sem_compra, 0)
        cobertura_pct = (total_com_compra / total_carteira * 100.0) if total_carteira > 0 else 0.0

        def chip_class(status_cor):
            s = normalize_text_for_match(status_cor)
            if "VERMELH" in s:
                return "chip-red"
            if "LARANJ" in s:
                return "chip-orange"
            if "AMAREL" in s:
                return "chip-yellow"
            if "VERDE" in s:
                return "chip-green"
            if "AZUL" in s or "NOVO" in s:
                return "chip-blue"
            return "chip-gray"

        ranking_2026_html = ""
        if ranking_2026:
            rows = []
            for i, item in enumerate(ranking_2026, start=1):
                rows.append(f"""
                <tr class="{h(item['row_class'])}">
                  <td style="width:22px; text-align:center;">{i}</td>
                  <td>{h(item['grupo'])}</td>
                  <td style="width:90px; text-align:right;">{h(format_number_br(item['valor']))}</td>
                  <td style="width:70px; text-align:center;">
                    <span class="status-chip {chip_class(item['status_cor'])}">{h(render_status_badge_text(item['status_cor']))}</span>
                  </td>
                </tr>
                """)
            ranking_2026_html = f"""
            <table class="dash-table-mini">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Grupo</th>
                  <th>Total 2026</th>
                  <th>Cor</th>
                </tr>
              </thead>
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
            """
        else:
            ranking_2026_html = """
            <div class="dash-map-placeholder" style="min-height:120px;">
              Sem dados para o Top 10 de 2026
            </div>
            """

        ranking_2025_html = ""
        if ranking_2025:
            rows = []
            for i, item in enumerate(ranking_2025, start=1):
                rows.append(f"""
                <tr class="{h(item['row_class'])}">
                  <td style="width:22px; text-align:center;">{i}</td>
                  <td>{h(item['grupo'])}</td>
                  <td style="width:90px; text-align:right;">{h(format_number_br(item['valor']))}</td>
                  <td style="width:70px; text-align:center;">
                    <span class="status-chip {chip_class(item['status_cor'])}">{h(render_status_badge_text(item['status_cor']))}</span>
                  </td>
                </tr>
                """)
            ranking_2025_html = f"""
            <table class="dash-table-mini">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Grupo</th>
                  <th>Total 2025</th>
                  <th>Cor</th>
                </tr>
              </thead>
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
            """
        else:
            ranking_2025_html = """
            <div class="dash-map-placeholder" style="min-height:120px;">
              Sem dados para o Top 10 de 2025
            </div>
            """

        clientes_sem_compra_html = ""
        if clientes_sem_compra:
            rows = []
            for item in clientes_sem_compra[:30]:
                rows.append(f"""
                <tr class="{h(item['row_class'])}">
                  <td>{h(item['codigo'])}</td>
                  <td>{h(item['grupo'])}</td>
                  <td style="text-align:right;">{h(format_number_br(item['t2024']))}</td>
                  <td style="text-align:right;">{h(format_number_br(item['t2025']))}</td>
                  <td style="text-align:right;">{h(format_number_br(item['t2026']))}</td>
                  <td>{h(item['data'])}</td>
                  <td>{h(item['mes'])}</td>
                  <td>{h(item['semana'])}</td>
                  <td>{h(item['status'])}</td>
                </tr>
                """)
            clientes_sem_compra_html = f"""
            <table class="dash-table-big">
              <thead>
                <tr>
                  <th>Código Grupo</th>
                  <th>Grupo</th>
                  <th>Total 2024</th>
                  <th>Total 2025</th>
                  <th>Total 2026</th>
                  <th>Data</th>
                  <th>Mês</th>
                  <th>Semana</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
            """
        else:
            clientes_sem_compra_html = """
            <div class="dash-map-placeholder" style="min-height:220px;">
              Nenhum cliente sem compra encontrado pela regra atual (Total 2026 = 0)
            </div>
            """

        mapa_svg_html, cidades_mapa_qtd, mapa_info_msg = build_cidades_resumo_html(
            filtered_rows,
            cidade_col=cidade_col,
            cnpj_col=cnpj_col,
            valor_col=t2026_col,
            fallback_id_col=key_col
        )

        gold_subinfo = ""
        gold_table_html = ""

        if not header_rep_code:
            gold_subinfo = """
            <div style="font-size:10px; color:#92400e;">
              Selecione um representante para consultar os clientes GOLD.
            </div>
            """
        elif gold_info.get("ok"):
            gold_subinfo = f"""
            <div style="font-size:10px; color:#92400e;">
              Rep: <b>{h(header_rep_code)}</b> | Aba GOLD: <b>{h(gold_info.get('worksheet_title', GOLD_WS))}</b>
            </div>
            """

            if gold_info.get("gold_rows"):
                gold_rows_html = []
                for item in gold_info.get("gold_rows", [])[:20]:
                    gold_rows_html.append(f"""
                    <tr>
                      <td>{h(item.get('codigo', ''))}</td>
                      <td>{h(item.get('cliente_grupo', ''))}</td>
                      <td>{h(item.get('grupo_cliente_cliente', ''))}</td>
                    </tr>
                    """)

                gold_table_html = f"""
                <div style="margin-top:8px; max-height:180px; overflow:auto; width:100%;">
                  <table class="dash-table-mini">
                    <thead>
                      <tr>
                        <th style="width:90px;">Código</th>
                        <th>Cliente / Grupo</th>
                        <th>Grupo Cliente / Cliente</th>
                      </tr>
                    </thead>
                    <tbody>
                      {''.join(gold_rows_html)}
                    </tbody>
                  </table>
                </div>
                """
            else:
                gold_table_html = """
                <div style="margin-top:8px; font-size:10px; color:#92400e;">
                  Nenhum cliente GOLD encontrado para este representante.
                </div>
                """
        elif gold_info.get("error"):
            gold_subinfo = f"""
            <div style="font-size:10px; color:#b91c1c;">
              Erro GOLD: {h(gold_info.get('error'))}
            </div>
            """

        parametros_form_html = render_parametros_comerciais_form_html(parametros_comerciais)

        body = f"""
        <div class="dash-page">

          {parametros_form_html}

          <div class="card no-print a3-page">
            <form method="get">
              <div class="grid">
                <div>
                  <label>Supervisor</label>
                  <select name="sup">
                    <option value="">(Todos)</option>
                    {''.join([f"<option value='{h(s)}' {'selected' if norm(s) == sup_sel else ''}>{h(s)}</option>" for s in sup_list])}
                  </select>
                </div>

                <div>
                  <label>Representante</label>
                  <select name="rep">
                    <option value="">(Todos)</option>
                    {''.join([f"<option value='{h(r)}' {'selected' if norm(r) == rep_sel else ''}>{h(r)}</option>" for r in rep_list])}
                  </select>
                </div>

                <div class="print-toolbar">
                  <button type="submit">Aplicar</button>
                  <a href="{url_for('admin_dashboard')}" class="btn-link secondary">Limpar</a>
                  <button type="button" class="btn-link orange" onclick="imprimirTelaA3()">Imprimir A3</button>
                </div>

                <div class="print-note">
                  Impressão ajustada para ficar o mais próximo possível da tela em uma página A3 horizontal.
                </div>
              </div>
            </form>
          </div>

          <div class="a3-page no-break">
            <div class="print-scale-wrap">
              <div class="dash-shell">

                <div class="dash-header">
                  <div>
                    {
                        f'<img src="{h(rep_photo)}" alt="Representante" class="dash-avatar">'
                        if rep_photo else
                        '<div class="dash-avatar-placeholder">FOTO<br>REP</div>'
                    }
                  </div>

                  <div class="dash-title-wrap">
                    <div class="dash-main-title">Acompanhamento de Representante</div>
                    <div class="dash-subline"><b>Representante:</b> {h(header_rep_name or "A definir")}</div>
                    <div class="dash-subline"><b>Código:</b> {h(header_rep_code or "A definir")} &nbsp; | &nbsp; <b>Supervisor:</b> {h(header_sup or "A definir")}</div>
                    <div class="dash-subline"><b>Região:</b> {h(header_region)}</div>
                    <div class="dash-subline"><b>Dias úteis:</b> Inverno {h(parametros_comerciais.get("dias_uteis_inverno", "") or "-")} | Verão {h(parametros_comerciais.get("dias_uteis_verao", "") or "-")} | <b>Positivação:</b> {h(parametros_comerciais.get("qtd_positivacao_carteira", "") or "-")}</div>
                  </div>

                  <div class="dash-meta-box">
                    <div class="dash-metric">
                      <div class="dash-metric-label">Meta</div>
                      <div class="dash-metric-value">{h(header_meta)}</div>
                    </div>
                    <div class="dash-metric">
                      <div class="dash-metric-label">Realizado</div>
                      <div class="dash-metric-value">{h(header_realizado)}</div>
                    </div>
                    <div class="dash-metric">
                      <div class="dash-metric-label">% Realizado</div>
                      <div class="dash-metric-value">{h(header_percentual)}</div>
                    </div>
                    <div class="dash-metric">
                      <div class="dash-metric-label">Positivação</div>
                      <div class="dash-metric-value">{h(parametros_comerciais.get("qtd_positivacao_carteira", "") or "-")}</div>
                    </div>
                  </div>

                  <div>
                    <img src="{h(LOGO_URL)}" alt="Logo Kidy" class="dash-kidy-logo">
                  </div>
                </div>

                <div class="dash-main-grid">
                  <div class="dash-left-stack">
                    <div class="dash-row-top">
                      <div class="dash-panel">
                        <div class="dash-panel-title">10 Maiores Clientes 2025</div>
                        <div class="dash-panel-body">
                          {ranking_2025_html}
                        </div>
                      </div>

                      <div class="dash-panel">
                        <div class="dash-panel-title">10 Maiores Clientes 2026</div>
                        <div class="dash-panel-body">
                          {ranking_2026_html}
                        </div>
                      </div>
                    </div>

                    <div class="dash-row-middle">
                      <div class="dash-panel">
                        <div class="dash-panel-title">Clientes Gold</div>
                        <div class="dash-panel-body">
                          <div class="dash-gold-box" style="align-items:stretch; justify-content:flex-start;">
                            <div style="text-align:center;">Total Clientes Gold: <b>{h(total_gold)}</b></div>
                            {gold_subinfo}
                            {gold_table_html}
                          </div>
                        </div>
                      </div>

                      <div class="dash-panel">
                        <div class="dash-panel-title">Cobertura da Carteira</div>
                        <div class="dash-panel-body">
                          <div class="dash-coverage-box">
                            Carteira: <b style="margin:0 6px;">{h(total_carteira)}</b> |
                            Com compra: <b style="margin:0 6px;">{h(total_com_compra)}</b> |
                            Sem compra: <b style="margin:0 6px;">{h(total_sem_compra)}</b> |
                            Cobertura: <b style="margin-left:6px;">{h(format_number_br(cobertura_pct))}%</b>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div class="dash-row-bottom">
                      <div class="dash-panel">
                        <div class="dash-panel-title">30 Maiores Clientes sem Compra</div>
                        <div class="dash-panel-body" style="height:100%;">
                          {clientes_sem_compra_html}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div class="dash-right-stack">
                    <div class="dash-panel" style="height:100%; display:flex; flex-direction:column;">
                      <div class="dash-panel-title">Resumo por Cidade</div>
                      <div class="dash-panel-body-map" style="flex:1; overflow:auto;">
                        {mapa_svg_html}
                      </div>
                    </div>
                  </div>
                </div>
        """

        if DEBUG_MODE:
            abas = ", ".join(debug_info.get("worksheets", []))
            body += f"""
            <div class="card debug-card no-print">
              <div class="title">DEBUG DASHBOARD ADMIN</div>
              <div class="line"><b>SHEET_ID:</b> {h(debug_info.get("sheet_id", ""))}</div>
              <div class="line"><b>NOME PLANILHA:</b> {h(debug_info.get("spreadsheet_title", ""))}</div>
              <div class="line"><b>ABAS:</b> {h(abas)}</div>
              <div class="line"><b>ROWS FILTRADAS:</b> {h(len(filtered_rows))}</div>
              <div class="line"><b>CLIENTES SEM COMPRA:</b> {h(len(clientes_sem_compra))}</div>
              <div class="line"><b>TOP 2026:</b> {h(len(ranking_2026))}</div>
              <div class="line"><b>TOP 2025:</b> {h(len(ranking_2025))}</div>
              <div class="line"><b>AGENDA SHEET ID:</b> {h(extract_google_sheet_id(AGENDA_SHEET_URL))}</div>
              <div class="line"><b>AGENDA ABA:</b> {h(WS_AGENDA)}</div>
              <div class="line"><b>REP AGENDA:</b> {h(header_rep_code)}</div>
              <div class="line"><b>CIDADES NO MAPA:</b> {h(cidades_mapa_qtd)}</div>
              <div class="line"><b>CIDADES SEM COORDENADA:</b> {h(cidades_sem_coordenada)}</div>
              <div class="line"><b>MUNICIPIOS URL:</b> {h(map_debug['municipios_url'])}</div>
              <div class="line"><b>COLUNA CIDADE BASE:</b> {h(cidade_col)}</div>
              <div class="line"><b>COLUNA CIDADE MUNICÍPIOS:</b> {h(map_debug['cidade_muni_col'])}</div>
              <div class="line"><b>COLUNA LAT:</b> {h(map_debug['lat_col'])}</div>
              <div class="line"><b>COLUNA LON:</b> {h(map_debug['lon_col'])}</div>
              <div class="line"><b>LABELS MAPA:</b> {h(map_debug['labels'])}</div>
              <div class="line"><b>ZOOM MAPA:</b> {h(map_debug['zoom'])}</div>
              <div class="line"><b>T2026 COL:</b> {h(t2026_col)}</div>
              <div class="line"><b>DATA AGENDA COL:</b> {h(data_agenda_col)}</div>
              <div class="line"><b>MÊS COL:</b> {h(mes_col)}</div>
              <div class="line"><b>SEMANA COL:</b> {h(semana_col)}</div>
              <div class="line"><b>STATUS CLIENTE COL:</b> {h(status_cliente_col)}</div>
              <div class="line"><b>OBS COL:</b> {h(observacoes_col)}</div>
              <hr style="border-color:#334155;">
              <div class="line"><b>GOLD SHEET ID/URL RESOLVIDO:</b> {h(gold_info.get('resolved_sheet_id', ''))}</div>
              <div class="line"><b>GOLD WS:</b> {h(GOLD_WS)}</div>
              <div class="line"><b>GOLD SHEET TITLE:</b> {h(gold_info.get('sheet_title', ''))}</div>
              <div class="line"><b>GOLD WORKSHEET TITLE:</b> {h(gold_info.get('worksheet_title', ''))}</div>
              <div class="line"><b>GOLD REP COL:</b> {h(gold_info.get('rep_col', ''))}</div>
              <div class="line"><b>GOLD CODIGO COL:</b> {h(gold_info.get('codigo_col', ''))}</div>
              <div class="line"><b>GOLD CLIENTE COL:</b> {h(gold_info.get('cliente_col', ''))}</div>
              <div class="line"><b>GOLD GRUPO COL:</b> {h(gold_info.get('grupo_col', ''))}</div>
              <div class="line"><b>GOLD SUPERVISOR COL:</b> {h(gold_info.get('supervisor_col', ''))}</div>
              <div class="line"><b>TOTAL GOLD:</b> {h(gold_info.get('total_gold', 0))}</div>
              <div class="line"><b>GOLD OK:</b> {h(gold_info.get('ok', False))}</div>
              <div class="line"><b>GOLD ERROR:</b> {h(gold_info.get('error', ''))}</div>
            </div>
            """

        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Dashboard Admin",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url="",
            body=body
        )

    except Exception as e:
        return render_error_page("Dashboard Admin", f"Erro ao abrir dashboard admin: {norm(str(e))}")


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not require_login():
        flash("Faça login para continuar.", "err")
        return redirect(url_for("login"))

    current_user_photo = ""
    if session.get("user_type") == "rep":
        current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

    try:
        sh = connect_gs()
    except Exception as e:
        return render_error_page("Erro", f"Erro ao conectar na planilha principal: {norm(str(e))}", current_user_photo)

    debug_info = build_debug_sheet_info(sh) if DEBUG_MODE else {"worksheets": [], "sheet_id": "", "spreadsheet_title": ""}
    last_save = get_last_save_debug()
    parametros_comerciais = get_parametros_comerciais(sh)

    try:
        headers, base_rows = get_base_structure_cached(sh)
    except WorksheetNotFound:
        return render_error_page("Erro", f"Aba não encontrada: {WS_BASE}", current_user_photo)
    except Exception as e:
        return render_error_page("Erro", f"Erro ao ler estrutura da BASE: {norm(str(e))}", current_user_photo)

    lista_rows = get_listas_records_cached(sh)

    key_col = pick_col_flexible(headers, [
        "Codigo Grupo Cliente", "Código Grupo Cliente",
        "Codigo Cliente", "Código Cliente", "COD_CLIENTE", "Cliente"
    ])
    grupo_col = pick_col_flexible(headers, [
        "Grupo Cliente", "Nome Cliente", "Cliente",
        "Razao Social", "Razão Social", "Fantasia", "Nome"
    ])
    rep_col = pick_col_flexible(headers, [
        "Codigo Representante", "Código Representante",
        "CODIGO REPRESENTANTE", "COD_REP"
    ])
    nome_rep_col = pick_col_flexible(headers, [
        "Representante", "Nome Representante", "REPRESENTANTE"
    ])
    sup_col = pick_col_flexible(headers, [
        "Supervisor", "Código Supervisor", "Codigo Supervisor", "COD_SUP"
    ])
    cidade_col = resolve_city_col(headers)

    t2024_col = pick_col_exact(headers, ["Total 2024 (PERIODO)"])
    t2025_col = pick_col_exact(headers, ["Total 2025 (PERIODO)"])
    t2026_col = pick_col_exact(headers, ["Total 2026 (PERIODO)"])

    status_cor_col = pick_col_exact(headers, ["STATUS COR", "Status Cor", "STATUSCOR", "StatusCor"])
    cliente_novo_col = pick_col_flexible(headers, ["Cliente Novo", "CLIENTE NOVO", "Novo", "NOVO"])

    data_agenda_col = pick_col_exact(headers, ["Data Agenda Visita"])
    mes_col = pick_col_exact(headers, ["Mês"])
    semana_col = pick_col_exact(headers, ["Semana Atendimento"])
    status_cliente_col = pick_col_exact(headers, ["Status Cliente"])
    observacoes_col = pick_col_exact(headers, ["Observações", "Observacao", "Observacoes"])

    meses = unique_list([r.get("Mês", "") for r in lista_rows]) or DEFAULT_MESES
    semanas = unique_list([r.get("Semana Atendimento", "") for r in lista_rows]) or DEFAULT_SEMANAS
    status_list = unique_list([r.get("Status Cliente", "") for r in lista_rows]) or DEFAULT_STATUS

    sup_sel = norm(request.args.get("sup", ""))
    rep_sel = norm(request.args.get("rep", ""))
    q = norm(request.args.get("q", ""))

    data_ini = norm(request.args.get("data_ini", ""))
    data_fim = norm(request.args.get("data_fim", ""))
    filtro_mes = norm(request.args.get("filtro_mes", ""))
    filtro_semana = norm(request.args.get("filtro_semana", ""))

    sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if (is_admin() and sup_col) else []
    rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if is_admin() else []

    def date_to_sortable(v):
        v = norm(v)
        if not v:
            return ""

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            return v

        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", v)
        if m:
            dd, mm, yyyy = m.groups()
            return f"{yyyy}-{mm}-{dd}"

        return ""

    prepared_rows = []
    q_lower = q.lower()

    for idx_base, r in enumerate(base_rows, start=2):
        repc = norm(r.get(rep_col, "")) if rep_col else ""

        if not is_admin() and repc != norm(session.get("rep_code", "")):
            continue
        if is_admin() and sup_col and sup_sel and norm(r.get(sup_col, "")) != sup_sel:
            continue
        if is_admin() and rep_sel and repc != rep_sel:
            continue

        if q_lower:
            grupo_val = norm(r.get(grupo_col, "")) if grupo_col else ""
            cidade_val = norm(r.get(cidade_col, "")) if cidade_col else ""
            ck_val = norm(r.get(key_col, "")) if key_col else ""
            nome_rep_val = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
            sup_val = norm(r.get(sup_col, "")) if sup_col else ""
            hay = f"{ck_val} {grupo_val} {cidade_val} {nome_rep_val} {sup_val}".lower()
            if q_lower not in hay:
                continue

        row_copy = dict(r)
        row_copy["Data Agenda Visita"] = norm(r.get(data_agenda_col, "")) if data_agenda_col else ""
        row_copy["Mês"] = norm(r.get(mes_col, "")) if mes_col else ""
        row_copy["Semana Atendimento"] = norm(r.get(semana_col, "")) if semana_col else ""
        row_copy["Status Cliente"] = norm(r.get(status_cliente_col, "")) if status_cliente_col else ""
        row_copy["Observações"] = norm(r.get(observacoes_col, "")) if observacoes_col else ""

        data_row_sort = date_to_sortable(row_copy["Data Agenda Visita"])
        if data_ini:
            if not data_row_sort or data_row_sort < data_ini:
                continue

        if data_fim:
            if not data_row_sort or data_row_sort > data_fim:
                continue

        if filtro_mes and normalize_text_for_match(row_copy["Mês"]) != normalize_text_for_match(filtro_mes):
            continue

        if filtro_semana and normalize_text_for_match(row_copy["Semana Atendimento"]) != normalize_text_for_match(filtro_semana):
            continue

        row_copy["_base_row_number"] = idx_base

        status_cor_final, row_class, priority = resolve_status_cor_from_base(
            row_copy,
            status_cor_col=status_cor_col,
            cliente_novo_col=cliente_novo_col
        )

        row_copy["_status_cor"] = status_cor_final
        row_copy["_row_class"] = row_class
        row_copy["_sort_priority"] = priority

        prepared_rows.append(row_copy)

    prepared_rows.sort(
        key=lambda r: (
            r.get("_sort_priority", 99),
            norm(r.get(grupo_col, "")) if grupo_col else "",
            norm(r.get(key_col, "")) if key_col else ""
        )
    )

    out_rows = prepared_rows[:PAGE_SIZE]

    rep_card_html = ""
    selected_rep_code = rep_sel if is_admin() else norm(session.get("rep_code", ""))

    if selected_rep_code and rep_col:
        rep_name_base = ""
        rep_sup_base = ""
        rep_reg_base = ""

        vendas_info_rep = get_vendas_info_by_rep(selected_rep_code)

        if vendas_info_rep.get("ok"):
            rep_name_base = vendas_info_rep.get("representante", "") or ""
            rep_sup_base = vendas_info_rep.get("supervisor", "") or ""
        else:
            for r in base_rows:
                if norm(r.get(rep_col, "")) == selected_rep_code:
                    rep_name_base = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
                    rep_sup_base = norm(r.get(sup_col, "")) if sup_col else ""
                    rep_reg_base = ""
                    if rep_name_base:
                        break

        foto_url = get_rep_photo_src(selected_rep_code)
        nome_card = rep_name_base or f"Representante {selected_rep_code}"
        sup_card = rep_sup_base
        regiao_card = rep_reg_base

        foto_html = (
            f'<img src="{h(foto_url)}" alt="Foto do representante" class="rep-photo">'
            if foto_url else
            '<div class="rep-photo-placeholder">Sem foto</div>'
        )

        infos = []
        infos.append(f"<div><b>Código:</b> {h(selected_rep_code)}</div>")
        if nome_card:
            infos.append(f"<div><b>Representante:</b> {h(nome_card)}</div>")
        if sup_card:
            infos.append(f"<div><b>Supervisor:</b> {h(sup_card)}</div>")
        if regiao_card:
            infos.append(f"<div><b>Região:</b> {h(regiao_card)}</div>")

        rep_card_html = f"""
        <div class="card">
          <div class="rep-card">
            {foto_html}
            <div>
              <div style="font-size:18px; font-weight:700; margin-bottom:6px;">Representante selecionado</div>
              {''.join(infos)}
            </div>
          </div>
        </div>
        """

    total_carteira_dashboard = len(prepared_rows)
    total_sem_compra_dashboard = 0
    if t2026_col:
        for r in prepared_rows:
            if parse_number_br(r.get(t2026_col, "")) <= 0:
                total_sem_compra_dashboard += 1
    total_com_compra_dashboard = max(total_carteira_dashboard - total_sem_compra_dashboard, 0)
    cobertura_pct_dashboard = (total_com_compra_dashboard / total_carteira_dashboard * 100.0) if total_carteira_dashboard > 0 else 0.0

    carteira_parametros_html = f"""
    <div class="card">
      <div style="font-size:18px; font-weight:700; margin-bottom:10px;">Cobertura da Carteira</div>
      <div class="dash-coverage-box">
        Carteira: <b style="margin:0 6px;">{h(total_carteira_dashboard)}</b> |
        Com compra: <b style="margin:0 6px;">{h(total_com_compra_dashboard)}</b> |
        Sem compra: <b style="margin:0 6px;">{h(total_sem_compra_dashboard)}</b> |
        Cobertura: <b style="margin-left:6px;">{h(format_number_br(cobertura_pct_dashboard))}%</b>
      </div>
      {render_parametros_comerciais_box_html(parametros_comerciais, compact=True)}
    </div>
    """

    parametros_card_html = render_parametros_comerciais_box_html(parametros_comerciais)

    debug_html = ""
    if DEBUG_MODE:
        abas = ", ".join(debug_info.get("worksheets", []))
        last_row = h(last_save.get("row_num", ""))
        last_ck = h(last_save.get("client_key", ""))
        last_data = h(last_save.get("data_agenda", ""))
        last_mes = h(last_save.get("mes", ""))
        last_semana = h(last_save.get("semana", ""))
        last_status = h(last_save.get("status_cliente", ""))
        last_obs = h(last_save.get("observacoes", ""))
        last_result = h(last_save.get("result", ""))

        debug_html = f"""
        <div class="card debug-card">
          <div class="title">DEBUG CONEXÃO / GRAVAÇÃO</div>
          <div class="line"><b>SHEET_ID:</b> {h(debug_info.get("sheet_id", ""))}</div>
          <div class="line"><b>NOME PLANILHA:</b> {h(debug_info.get("spreadsheet_title", ""))}</div>
          <div class="line"><b>ABAS:</b> {h(abas)}</div>
          <div class="line"><b>USUÁRIO:</b> {h(session.get("user_login", ""))} ({h(session.get("user_type", ""))})</div>
          <div class="line"><b>REPRESENTANTE LOGADO:</b> {h(session.get("rep_code", ""))}</div>
          <div class="line"><b>REPRESENTANTE FILTRADO:</b> {h(selected_rep_code)}</div>
          <hr style="border-color:#334155;">
          <div class="line"><b>ÚLTIMA LINHA GRAVADA:</b> {last_row}</div>
          <div class="line"><b>ÚLTIMO CLIENT_KEY:</b> {last_ck}</div>
          <div class="line"><b>ÚLTIMA DATA:</b> {last_data}</div>
          <div class="line"><b>ÚLTIMO MÊS:</b> {last_mes}</div>
          <div class="line"><b>ÚLTIMA SEMANA:</b> {last_semana}</div>
          <div class="line"><b>ÚLTIMO STATUS:</b> {last_status}</div>
          <div class="line"><b>ÚLTIMA OBS:</b> {last_obs}</div>
          <div class="line"><b>RESULTADO:</b> {last_result}</div>
        </div>
        """

    def opt_html(options, selected):
        out = ["<option value=''></option>"]
        for o in options:
            sel = "selected" if norm(o) == norm(selected) else ""
            out.append(f"<option value='{h(o)}' {sel}>{h(o)}</option>")
        return "\n".join(out)

    table_rows = []

    for idx, r in enumerate(out_rows, start=1):
        ck = norm(r.get(key_col, "")) if key_col else ""
        grupo = norm(r.get(grupo_col, "")) if grupo_col else ""
        repc = norm(r.get(rep_col, "")) if rep_col else ""
        nome_rep = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
        supv = norm(r.get(sup_col, "")) if sup_col else ""
        cidade = norm(r.get(cidade_col, "")) if cidade_col else ""

        t24 = fmt_money(r.get(t2024_col, "")) if t2024_col else ""
        t25 = fmt_money(r.get(t2025_col, "")) if t2025_col else ""
        t26 = fmt_money(r.get(t2026_col, "")) if t2026_col else ""

        dav = norm(r.get("Data Agenda Visita", ""))
        mes = norm(r.get("Mês", ""))
        sem = norm(r.get("Semana Atendimento", ""))
        stc = norm(r.get("Status Cliente", ""))
        obs = norm(r.get("Observações", ""))

        status_cor = r.get("_status_cor", "")
        klass = r.get("_row_class", "")
        base_row_number = r.get("_base_row_number", "")
        form_id = f"form_row_{idx}"

        hidden_filters = ""
        if sup_sel:
            hidden_filters += f'<input type="hidden" name="sup" value="{h(sup_sel)}">'
        if rep_sel:
            hidden_filters += f'<input type="hidden" name="rep" value="{h(rep_sel)}">'
        if q:
            hidden_filters += f'<input type="hidden" name="q" value="{h(q)}">'
        if data_ini:
            hidden_filters += f'<input type="hidden" name="data_ini" value="{h(data_ini)}">'
        if data_fim:
            hidden_filters += f'<input type="hidden" name="data_fim" value="{h(data_fim)}">'
        if filtro_mes:
            hidden_filters += f'<input type="hidden" name="filtro_mes" value="{h(filtro_mes)}">'
        if filtro_semana:
            hidden_filters += f'<input type="hidden" name="filtro_semana" value="{h(filtro_semana)}">'

        if is_admin():
            row_html = f"""
        <tr class="{h(klass)}">
          <td class="nowrap sticky-col">{h(ck)}</td>
          <td class="sticky-col-2">{h(grupo)}</td>
          <td>{h(cidade)}</td>
          <td class="money nowrap">{h(t24)}</td>
          <td class="money nowrap">{h(t25)}</td>
          <td class="money nowrap">{h(t26)}</td>
          <td>
            <form id="{form_id}" method="post" action="{url_for('salvar')}">
              <input type="hidden" name="client_key" value="{h(ck)}">
              <input type="hidden" name="rep_code" value="{h(repc)}">
              <input type="hidden" name="base_row_number" value="{h(base_row_number)}">
              <input type="hidden" name="Data Agenda Visita" value="{h(to_input_date(dav))}">
              <input type="hidden" name="Status Cliente" value="{h(stc)}">
              {hidden_filters}
            </form>
            <select name="Mês" form="{form_id}" style="min-width:140px;">
              {opt_html(meses, mes)}
            </select>
          </td>

          <td>
            <select name="Semana Atendimento" form="{form_id}" style="min-width:160px;">
              {opt_html(semanas, sem)}
            </select>
          </td>

          <td style="min-width:420px;">
            <div style="display:flex; align-items:center; gap:8px;">
              <input type="text"
                     name="Observações"
                     form="{form_id}"
                     placeholder="Digite observações..."
                     value="{h(obs)}"
                     style="flex:1; min-width:260px;">
              <button type="submit" form="{form_id}" style="white-space:nowrap;">Gravar</button>
            </div>
          </td>
        </tr>
        """
        else:
            row_html = f"""
        <tr class="{h(klass)}">
          <td class="nowrap sticky-col">{h(ck)}</td>
          <td class="sticky-col-2">{h(grupo)}</td>
          <td>{h(cidade)}</td>
          <td class="money nowrap">{h(t24)}</td>
          <td class="money nowrap">{h(t25)}</td>
          <td class="money nowrap">{h(t26)}</td>

          <td>
            <form id="{form_id}" method="post" action="{url_for('salvar')}">
              <input type="hidden" name="client_key" value="{h(ck)}">
              <input type="hidden" name="rep_code" value="{h(repc)}">
              <input type="hidden" name="base_row_number" value="{h(base_row_number)}">
              <input type="hidden" name="Data Agenda Visita" value="{h(to_input_date(dav))}">
              <input type="hidden" name="Status Cliente" value="{h(stc)}">
              {hidden_filters}
            </form>
            <select name="Mês" form="{form_id}" style="min-width:140px;">
              {opt_html(meses, mes)}
            </select>
          </td>

          <td>
            <select name="Semana Atendimento" form="{form_id}" style="min-width:160px;">
              {opt_html(semanas, sem)}
            </select>
          </td>

          <td style="min-width:420px;">
            <div style="display:flex; align-items:center; gap:8px;">
              <input type="text"
                     name="Observações"
                     form="{form_id}"
                     placeholder="Digite observações..."
                     value="{h(obs)}"
                     style="flex:1; min-width:260px;">
              <button type="submit" form="{form_id}" style="white-space:nowrap;">Gravar</button>
            </div>
          </td>
        </tr>
        """
        table_rows.append(row_html)

    filtros_html = ""
    if is_admin():
        filtros_html = f"""
        <div>
          <label>Filtro Supervisor</label>
          <select name="sup">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{h(s)}' {'selected' if norm(s) == sup_sel else ''}>{h(s)}</option>" for s in sup_list])}
          </select>
        </div>
        <div>
          <label>Filtro Representante</label>
          <select name="rep">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{h(r)}' {'selected' if norm(r) == rep_sel else ''}>{h(r)}</option>" for r in rep_list])}
          </select>
        </div>
        """

    body = f"""
    {debug_html}
    {rep_card_html}
    {parametros_card_html}
    {carteira_parametros_html}

    <div class="card">
      <form method="get">
        <div class="grid">
          {filtros_html}
          <div>
            <label>Buscar</label>
            <input name="q" value="{h(q)}" placeholder="cliente/grupo/cidade...">
          </div>

          <div>
            <label>Data inicial</label>
            <input type="date" name="data_ini" value="{h(data_ini)}">
          </div>

          <div>
            <label>Data final</label>
            <input type="date" name="data_fim" value="{h(data_fim)}">
          </div>

          <div>
            <label>Filtrar por Mês</label>
            <select name="filtro_mes">
              {opt_html(meses, filtro_mes)}
            </select>
          </div>

          <div>
            <label>Filtrar por Semana</label>
            <select name="filtro_semana">
              {opt_html(semanas, filtro_semana)}
            </select>
          </div>

          <div style="display:flex;align-items:flex-end;gap:8px;">
            <button type="submit">Aplicar</button>
            <a href="{url_for('dashboard')}"><button type="button" class="secondary">Limpar</button></a>
          </div>
        </div>
      </form>
    </div>

    <div class="card rep-table-wrap">
      <table class="rep-table">
        <thead>
          <tr>
            <th class="sticky-col">Codigo Grupo Cliente</th>
            <th class="sticky-col-2">Grupo Cliente</th>
            <th>Cidade</th>
            <th>Total 2024</th>
            <th>Total 2025</th>
            <th>Total 2026</th>
            <th>Mês</th>
            <th>Semana Atendimento</th>
            <th>Observações</th>
          </tr>
        </thead>
        <tbody>
          {''.join(table_rows)}
        </tbody>
      </table>
    </div>
    """

    return render_template_string(
        BASE_HTML,
        title=APP_TITLE,
        subtitle=f"Planilha: {WS_BASE}",
        logged=True,
        user_login=session.get("user_login"),
        user_name=session.get("rep_name", ""),
        user_type=session.get("user_type"),
        user_photo_url=current_user_photo,
        body=body
    )


@app.route("/salvar", methods=["POST"])
def salvar():
    if not require_login():
        flash("Sessão expirada. Faça login novamente.", "err")
        return redirect(url_for("login"))

    user_type = session.get("user_type")
    user_login = session.get("user_login")

    client_key = norm(request.form.get("client_key", ""))
    rep_code_form = norm(request.form.get("rep_code", ""))
    base_row_number = norm(request.form.get("base_row_number", ""))

    sup = norm(request.form.get("sup", ""))
    rep = norm(request.form.get("rep", ""))
    q = norm(request.form.get("q", ""))

    data_ini = norm(request.form.get("data_ini", ""))
    data_fim = norm(request.form.get("data_fim", ""))
    filtro_mes = norm(request.form.get("filtro_mes", ""))
    filtro_semana = norm(request.form.get("filtro_semana", ""))

    redirect_args = {
        k: v for k, v in {
            "sup": sup,
            "rep": rep,
            "q": q,
            "data_ini": data_ini,
            "data_fim": data_fim,
            "filtro_mes": filtro_mes,
            "filtro_semana": filtro_semana
        }.items() if v
    }

    if not client_key:
        flash("client_key vazio.", "err")
        return redirect(url_for("dashboard", **redirect_args))

    if not base_row_number.isdigit():
        flash("Linha da BASE inválida para gravação.", "err")
        return redirect(url_for("dashboard", **redirect_args))

    if user_type == "rep" and rep_code_form != norm(session.get("rep_code", "")):
        flash("Você não pode gravar alterações em clientes de outro representante.", "err")
        return redirect(url_for("dashboard", **redirect_args))

    try:
        sh = connect_gs()
        ws_base = sh.worksheet(WS_BASE)

        try:
            ws_ed = ensure_edicoes_worksheet(sh)
            edicoes_ok = True
        except Exception:
            ws_ed = None
            edicoes_ok = False

        headers = ensure_base_tracking_columns(ws_base)
        headers_norm = [norm(x) for x in headers]

        data_agenda = from_input_date(request.form.get("Data Agenda Visita", ""))
        mes = norm(request.form.get("Mês", ""))
        semana = norm(request.form.get("Semana Atendimento", ""))
        status_cliente = norm(request.form.get("Status Cliente", ""))
        observacoes = norm(request.form.get("Observações", ""))

        # Na tela da carteira do representante, Data Agenda Visita e Status Cliente
        # são mantidos apenas como apoio e não devem forçar atualização/confirmação
        # quando o usuário grava somente Mês, Semana ou Observações.
        is_admin_user = is_admin()
        campos_enviados = {
            "data": is_admin_user and ("Data Agenda Visita" in request.form),
            "mes": "Mês" in request.form,
            "semana": "Semana Atendimento" in request.form,
            "status": is_admin_user and ("Status Cliente" in request.form),
            "obs": "Observações" in request.form,
        }

        row_num = int(base_row_number)

        col_data = headers_norm.index("Data Agenda Visita") + 1
        col_mes = headers_norm.index("Mês") + 1
        col_semana = headers_norm.index("Semana Atendimento") + 1
        col_status = headers_norm.index("Status Cliente") + 1
        col_obs = headers_norm.index("Observações") + 1

        updates = []
        confirmacoes = {}

        # Regra prática para a carteira e dashboard:
        # só atualiza/confirma campos que realmente vieram preenchidos.
        # Isso evita falso erro ao gravar quando o usuário altera apenas Mês/Obs
        # e os demais campos chegam vazios no formulário.
        if campos_enviados["data"] and norm(data_agenda):
            updates.append({"range": rowcol_to_a1(row_num, col_data), "values": [[data_agenda]]})
            confirmacoes["data"] = normalizar_data_comparacao(data_agenda)

        if campos_enviados["mes"] and norm(mes):
            updates.append({"range": rowcol_to_a1(row_num, col_mes), "values": [[mes]]})
            confirmacoes["mes"] = norm(mes)

        if campos_enviados["semana"] and norm(semana):
            updates.append({"range": rowcol_to_a1(row_num, col_semana), "values": [[semana]]})
            confirmacoes["semana"] = norm(semana)

        if campos_enviados["status"] and norm(status_cliente):
            updates.append({"range": rowcol_to_a1(row_num, col_status), "values": [[status_cliente]]})
            confirmacoes["status"] = norm(status_cliente)

        # Observações pode ser gravada mesmo vazia quando o campo existir,
        # para permitir ajuste do texto na mesma linha.
        if campos_enviados["obs"]:
            updates.append({"range": rowcol_to_a1(row_num, col_obs), "values": [[observacoes]]})
            confirmacoes["obs"] = norm(observacoes)

        if updates:
            ws_base.batch_update(updates, value_input_option="RAW")

        esperado_data = confirmacoes.get("data", "")
        esperado_mes = confirmacoes.get("mes", "")
        esperado_semana = confirmacoes.get("semana", "")
        esperado_status = confirmacoes.get("status", "")
        esperado_obs = confirmacoes.get("obs", "")

        gravado_data = ""
        gravado_mes = ""
        gravado_semana = ""
        gravado_status = ""
        gravado_obs = ""
        conferiu = False

        for _ in range(5):
            time.sleep(0.35)

            gravado_data = norm(ws_base.acell(rowcol_to_a1(row_num, col_data)).value or "")
            gravado_mes = norm(ws_base.acell(rowcol_to_a1(row_num, col_mes)).value or "")
            gravado_semana = norm(ws_base.acell(rowcol_to_a1(row_num, col_semana)).value or "")
            gravado_status = norm(ws_base.acell(rowcol_to_a1(row_num, col_status)).value or "")
            gravado_obs = norm(ws_base.acell(rowcol_to_a1(row_num, col_obs)).value or "")

            conferiu = True

            if "data" in confirmacoes:
                conferiu = conferiu and (
                    normalizar_data_comparacao(gravado_data) == esperado_data
                )

            if "mes" in confirmacoes:
                conferiu = conferiu and (
                    normalize_text_for_match(gravado_mes) == normalize_text_for_match(esperado_mes)
                )

            if "semana" in confirmacoes:
                conferiu = conferiu and (
                    normalize_text_for_match(gravado_semana) == normalize_text_for_match(esperado_semana)
                )

            if "status" in confirmacoes:
                conferiu = conferiu and (
                    normalize_text_for_match(gravado_status) == normalize_text_for_match(esperado_status)
                )

            if "obs" in confirmacoes:
                conferiu = conferiu and (
                    norm(gravado_obs) == norm(esperado_obs)
                )

            if conferiu:
                break

        if not conferiu:
            set_last_save_debug({
                "row_num": row_num,
                "client_key": client_key,
                "data_agenda": gravado_data,
                "mes": gravado_mes,
                "semana": gravado_semana,
                "semana_esperada": esperado_semana,
                "status_cliente": gravado_status,
                "observacoes": gravado_obs,
                "result": "FALHA NA CONFIRMAÇÃO",
            })
            raise RuntimeError(
                "A gravação não foi confirmada na BASE. "
                f"Linha={row_num} | "
                f"Data='{gravado_data}' | "
                f"Mês='{gravado_mes}' | "
                f"Semana='{gravado_semana}' | "
                f"Status='{gravado_status}' | "
                f"Obs='{gravado_obs}'"
            )

        if edicoes_ok and ws_ed is not None:
            row_log = [
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                user_type,
                user_login,
                rep_code_form,
                client_key,
                data_agenda,
                mes,
                semana,
                status_cliente,
                observacoes
            ]
            ws_ed.append_row(row_log, value_input_option="USER_ENTERED")
            result_txt = "BASE OK / EDICOES OK"
        else:
            result_txt = "BASE OK / EDICOES NÃO DISPONÍVEL"

        set_last_save_debug({
            "row_num": row_num,
            "client_key": client_key,
            "data_agenda": gravado_data,
            "mes": gravado_mes,
            "semana": gravado_semana,
            "status_cliente": gravado_status,
            "observacoes": gravado_obs,
            "result": result_txt,
        })

        invalidate_main_sheet_cache()

        flash(f"Gravado com sucesso na BASE na linha {row_num}.", "ok")
        if not edicoes_ok:
            flash("A BASE foi gravada, mas a aba EDICOES não pôde ser usada. Crie a aba manualmente ou ajuste a permissão da service account.", "err")

    except Exception as e:
        app.logger.error("Erro ao gravar na planilha:\n%s", traceback.format_exc())
        set_last_save_debug({
            "row_num": base_row_number,
            "client_key": client_key,
            "data_agenda": request.form.get("Data Agenda Visita", ""),
            "mes": request.form.get("Mês", ""),
            "semana": request.form.get("Semana Atendimento", ""),
            "status_cliente": request.form.get("Status Cliente", ""),
            "observacoes": request.form.get("Observações", ""),
            "result": f"ERRO: {str(e)}",
        })
        flash(f"Erro ao gravar na planilha: {norm(str(e))}", "err")

    return redirect(url_for("dashboard", **redirect_args))


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=DEBUG_MODE
    )#