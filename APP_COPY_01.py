import os
import re
import json
import base64
import traceback
import html
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

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

MUNICIPIOS_SHEET_ID = os.getenv("MUNICIPIOS_SHEET_ID", "").strip()
WS_CIDADES = os.getenv("WS_CIDADES", "cidades").strip()

# ===== CLIENTES GOLD =====
GOLD_SHEET_ID = os.getenv("GOLD_SHEET_ID", "").strip()
GOLD_SHEET_URL = os.getenv("GOLD_SHEET_URL", "").strip()
GOLD_WS = os.getenv("GOLD_WS", "Tab").strip()

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").strip().lower() in ("1", "true", "sim", "yes")

APP_TITLE = "Acompanhamento de clientes"
LOGO_URL = "https://raw.githubusercontent.com/carlinhosg7/metodo/main/logo_kidy.png"

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


def build_city_map_svg(city_points, width=650, height=360):
    if not city_points:
        return """
        <div class="dash-map-placeholder">
          Não foi possível montar o mapa.<br><br>
          Verifique a planilha de municípios, a aba <b>cidades</b> e as colunas de cidade, latitude e longitude.
        </div>
        """

    valid_points = [
        p for p in city_points
        if p.get("lat") is not None and p.get("lon") is not None
    ]

    if not valid_points:
        return """
        <div class="dash-map-placeholder">
          Nenhuma coordenada válida encontrada na aba <b>cidades</b>.
        </div>
        """

    min_lon = min(p["lon"] for p in valid_points)
    max_lon = max(p["lon"] for p in valid_points)
    min_lat = min(p["lat"] for p in valid_points)
    max_lat = max(p["lat"] for p in valid_points)

    if min_lon == max_lon:
        max_lon += 0.01
    if min_lat == max_lat:
        max_lat += 0.01

    pad = 18

    def project(lon, lat):
        x = pad + ((lon - min_lon) / (max_lon - min_lon)) * (width - 2 * pad)
        y = pad + (1 - ((lat - min_lat) / (max_lat - min_lat))) * (height - 2 * pad)
        return x, y

    circles = []
    for p in valid_points:
        x, y = project(p["lon"], p["lat"])
        fill = p["fill"]
        title = h(f"{p['cidade']} | {p['status_txt']} | Total 2026: {format_number_br(p['total_2026'])}")
        circles.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4.8" fill="{fill}" stroke="#ffffff" stroke-width="1.2">'
            f'<title>{title}</title></circle>'
        )

    svg = f"""
    <div style="width:100%; height:100%; background:#eef7f7; border:1px solid #cbd5e1; border-radius:6px; padding:6px; box-sizing:border-box;">
      <svg viewBox="0 0 {width} {height}" width="100%" height="100%" style="display:block; background:#dff3f1; border-radius:4px;">
        <rect x="0" y="0" width="{width}" height="{height}" fill="#dff3f1"></rect>
        <rect x="10" y="10" width="{width-20}" height="{height-20}" fill="none" stroke="#94a3b8" stroke-width="1" stroke-dasharray="4 4"></rect>
        {''.join(circles)}
      </svg>

      <div style="display:flex; gap:12px; justify-content:center; align-items:center; margin-top:6px; flex-wrap:wrap; font-size:10px;">
        <span style="display:flex; align-items:center; gap:6px;">
          <span style="width:10px; height:10px; border-radius:50%; background:#16a34a; display:inline-block;"></span>
          Com vendas
        </span>
        <span style="display:flex; align-items:center; gap:6px;">
          <span style="width:10px; height:10px; border-radius:50%; background:#dc2626; display:inline-block;"></span>
          Sem vendas
        </span>
      </div>
    </div>
    """
    return svg


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
    dia_col = headers.index("DIA") + 1
    at_col = headers.index("ATENDIMENTO") + 1
    cliente_col = headers.index("CLIENTE") + 1
    valor_col = headers.index("VALOR") + 1

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


def render_agenda_semana_html(rep_code, sup_sel="", rep_sel=""):
    rep_code = norm(rep_code)
    if not rep_code:
        return """
        <div class="dash-summary-box">
          Selecione um representante para exibir e salvar a agenda semanal.
        </div>
        """

    agenda = carregar_agenda_rep(rep_code)

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
        header_sub.append('<th style="width:80px;">VALOR</th>')
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
                f'<td><input class="agenda-input agenda-valor" type="text" name="{h(dia)}_{at}_valor" value="{h(valor)}" placeholder="Valor"></td>'
            )
        row.append("</tr>")
        body_rows.append("".join(row))

    hidden_sup = f'<input type="hidden" name="sup" value="{h(sup_sel)}">' if sup_sel else ""
    hidden_rep = f'<input type="hidden" name="rep" value="{h(rep_sel)}">' if rep_sel else ""

    return f"""
    <form method="post" action="{url_for('salvar_agenda')}">
      <input type="hidden" name="rep_code_agenda" value="{h(rep_code)}">
      {hidden_sup}
      {hidden_rep}

      <div class="agenda-topbar">
        <div class="agenda-rep-label">
          Agenda semanal do representante <b>{h(rep_code)}</b>
        </div>
        <div>
          <button type="submit" class="agenda-save-btn">Salvar Agenda</button>
        </div>
      </div>

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


def connect_municipios_gs():
    target_id = MUNICIPIOS_SHEET_ID or SHEET_ID
    return connect_gs_by_key(target_id)


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


def get_base_structure(ws_base):
    headers = ensure_base_tracking_columns(ws_base)
    rows = ws_base.get_all_values()

    if not rows:
        return headers, []

    final_headers = [norm(x) for x in rows[0]]
    data_rows = []

    for raw in rows[1:]:
        if len(raw) < len(final_headers):
            raw = raw + [""] * (len(final_headers) - len(raw))
        elif len(raw) > len(final_headers):
            raw = raw[:len(final_headers)]

        data_rows.append({final_headers[i]: raw[i] for i in range(len(final_headers))})

    return final_headers, data_rows


def try_get_rep_name(rep_code):
    rep_code = norm(rep_code)
    if not rep_code:
        return ""

    try:
        sh = connect_gs()
        ws_base = sh.worksheet(WS_BASE)
        headers, base_rows = safe_get_raw_rows(ws_base)

        rep_col = pick_col_flexible(headers, [
            "Codigo Representante", "Código Representante",
            "CODIGO REPRESENTANTE", "COD_REP"
        ])
        nome_rep_col = pick_col_flexible(headers, [
            "Representante", "Nome Representante", "REPRESENTANTE"
        ])

        if not rep_col or not nome_rep_col:
            return ""

        for row in base_rows:
            if norm(row.get(rep_col, "")) == rep_code:
                return norm(row.get(nome_rep_col, ""))

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
            "Cod. Representante",
            "Cod Representante",
            "Código Representante",
            "Codigo Representante",
            "Representante",
            "COD_REP",
            "REP"
        ])

        codigo_gold_col = pick_col_flexible(headers_gold, [
            "Codigo",
            "Código",
            "Codigo Cliente",
            "Código Cliente",
            "Codigo Grupo Cliente",
            "Código Grupo Cliente",
            "Cod Cliente",
            "Cod. Cliente",
            "Cod Grupo Cliente",
            "Cod. Grupo Cliente",
            "Cliente Codigo",
            "Cliente Código"
        ])

        cliente_gold_col = pick_col_flexible(headers_gold, [
            "Cliente / Grupo",
            "Cliente Grupo",
            "Cliente",
            "Nome Cliente",
            "Razao Social",
            "Razão Social",
            "Fantasia",
            "Nome"
        ])

        grupo_gold_col = pick_col_flexible(headers_gold, [
            "Grupo Cliente / Cliente",
            "Grupo Cliente Cliente",
            "Grupo Cliente",
            "Grupo",
            "Cliente / Grupo"
        ])

        supervisor_gold_col = pick_col_flexible(headers_gold, [
            "Supervisor",
            "Cod. Supervisor",
            "Código Supervisor",
            "Codigo Supervisor"
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
    try:
        if sh is None:
            sh = connect_gs()

        abas = [ws.title for ws in sh.worksheets()]
        return {
            "sheet_id": extract_google_sheet_id(SHEET_ID),
            "spreadsheet_title": norm(getattr(sh, "title", "")),
            "worksheets": abas,
            "ok": True,
        }
    except Exception as e:
        return {
            "sheet_id": extract_google_sheet_id(SHEET_ID),
            "spreadsheet_title": "",
            "worksheets": [],
            "ok": False,
            "error": friendly_gspread_error(e),
        }


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

    .dash-shell {
      background: #ffffff;
      border: 1px solid #cfd4dc;
      border-top: 3px solid #f97316;
      border-bottom: 3px solid #f97316;
      padding: 10px;
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

    .dash-row-top {
      display: grid;
      grid-template-columns: 1fr 1fr 1.08fr;
      gap: 8px;
      margin-bottom: 8px;
    }

    .dash-row-bottom {
      display: grid;
      grid-template-columns: 1.22fr 0.92fr;
      gap: 8px;
      align-items: start;
    }

    .dash-right-stack {
      display: grid;
      grid-template-rows: auto auto;
      gap: 8px;
    }

    .dash-panel {
      border: 1px solid #9ca3af;
      background: #ffffff;
      overflow: hidden;
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
      margin: 4mm;
    }

    @media print {
      html, body {
        width: 420mm;
        height: 297mm;
        background: #ffffff !important;
      }

      .topbar,
      .no-print,
      .msg {
        display: none !important;
      }

      .container {
        padding: 0 !important;
        margin: 0 !important;
        width: 100%;
      }

      .dash-page {
        gap: 0 !important;
        width: 100%;
      }

      .a3-page {
        width: 412mm !important;
        height: 288mm !important;
        margin: 0 auto !important;
        overflow: hidden !important;
      }

      .dash-shell {
        width: 100% !important;
        height: 100% !important;
        padding: 6mm !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        overflow: hidden !important;
      }

      .dash-header,
      .dash-row-top,
      .dash-row-bottom,
      .dash-right-stack,
      .dash-panel,
      .dash-panel-body {
        break-inside: avoid !important;
        page-break-inside: avoid !important;
      }

      .dash-table-mini,
      .dash-table-big,
      .agenda-table {
        font-size: 8px !important;
      }

      .dash-table-mini th, .dash-table-mini td,
      .dash-table-big th, .dash-table-big td,
      .agenda-table th, .agenda-table td {
        padding: 2px 3px !important;
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
            return redirect(url_for("dashboard"))
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
    return redirect(url_for("admin_dashboard", **args))


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
        debug_info = build_debug_sheet_info(sh)

        try:
            ws_base = sh.worksheet(WS_BASE)
            headers, base_rows = get_base_structure(ws_base)
        except Exception:
            headers, base_rows = [], []

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
        cidade_col = pick_col_flexible(headers, ["Cidade", "Município", "Municipio"])

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

        sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if sup_col else []
        rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if rep_col else []

        filtered_rows = []
        for r in base_rows:
            if sup_sel and sup_col and norm(r.get(sup_col, "")) != sup_sel:
                continue
            if rep_sel and rep_col and norm(r.get(rep_col, "")) != rep_sel:
                continue
            filtered_rows.append(r)

        header_rep_code = rep_sel
        header_rep_name = ""
        header_sup = sup_sel
        header_region = "REGIÃO / ÁREA"
        header_meta = "R$ 0,00"
        header_realizado = "R$ 0,00"
        header_percentual = "0,00%"

        if header_rep_code and rep_col:
            for r in filtered_rows:
                if norm(r.get(rep_col, "")) == header_rep_code:
                    header_rep_name = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
                    if not header_sup and sup_col:
                        header_sup = norm(r.get(sup_col, ""))
                    break

        total_realizado_2026 = sum(parse_number_br(r.get(t2026_col, "")) for r in filtered_rows) if t2026_col else 0.0
        header_realizado = format_money_br(total_realizado_2026)

        rep_photo = get_rep_photo_src(header_rep_code) if header_rep_code else ""

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

        agenda_semanal_html = render_agenda_semana_html(
            rep_code=header_rep_code,
            sup_sel=sup_sel,
            rep_sel=rep_sel
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
            for item in clientes_sem_compra[:24]:
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

        mapa_svg_html = ""
        mapa_info_msg = ""
        cidades_mapa_qtd = 0
        map_debug = {
            "municipios_sheet_resolved": extract_google_sheet_id(MUNICIPIOS_SHEET_ID or SHEET_ID),
            "ws_cidades": WS_CIDADES,
            "cidade_muni_col": "",
            "lat_col": "",
            "lon_col": "",
        }

        try:
            sh_muni = connect_municipios_gs()
            ws_cidades = sh_muni.worksheet(WS_CIDADES)
            headers_cidades, rows_cidades = safe_get_raw_rows(ws_cidades)

            cidade_muni_col = pick_col_flexible(headers_cidades, [
                "cidade", "municipio", "município", "nome", "nome municipio", "nome município"
            ])
            lat_col = pick_col_flexible(headers_cidades, [
                "latitude", "lat"
            ])
            lon_col = pick_col_flexible(headers_cidades, [
                "longitude", "long", "lon", "lng"
            ])

            map_debug["cidade_muni_col"] = cidade_muni_col or ""
            map_debug["lat_col"] = lat_col or ""
            map_debug["lon_col"] = lon_col or ""

            if not cidade_col:
                raise RuntimeError("A coluna de cidade não foi encontrada na BASE.")

            if not cidade_muni_col:
                raise RuntimeError("A coluna de cidade não foi encontrada na aba 'cidades'.")

            if not lat_col or not lon_col:
                raise RuntimeError("As colunas de latitude/longitude não foram encontradas na aba 'cidades'.")

            vendas_por_cidade = {}
            for r in filtered_rows:
                cidade_base = normalize_city_key(r.get(cidade_col, ""))
                if not cidade_base:
                    continue

                total_2026 = parse_number_br(r.get(t2026_col, "")) if t2026_col else 0.0
                if cidade_base not in vendas_por_cidade:
                    vendas_por_cidade[cidade_base] = {
                        "cidade_original": norm(r.get(cidade_col, "")),
                        "total_2026": 0.0
                    }
                vendas_por_cidade[cidade_base]["total_2026"] += total_2026

            city_points = []
            for r in rows_cidades:
                cidade_sheet = normalize_city_key(r.get(cidade_muni_col, ""))
                if not cidade_sheet:
                    continue

                if cidade_sheet not in vendas_por_cidade:
                    continue

                lat = parse_float_any(r.get(lat_col, ""))
                lon = parse_float_any(r.get(lon_col, ""))
                total_2026 = vendas_por_cidade[cidade_sheet]["total_2026"]

                city_points.append({
                    "cidade": vendas_por_cidade[cidade_sheet]["cidade_original"] or norm(r.get(cidade_muni_col, "")),
                    "lat": lat,
                    "lon": lon,
                    "total_2026": total_2026,
                    "fill": "#16a34a" if total_2026 > 0 else "#dc2626",
                    "status_txt": "Com vendas" if total_2026 > 0 else "Sem vendas"
                })

            cidades_mapa_qtd = len(city_points)
            mapa_svg_html = build_city_map_svg(city_points)

            if not city_points:
                mapa_info_msg = "Nenhuma cidade cruzou entre a carteira e a planilha de municípios."

        except WorksheetNotFound:
            mapa_svg_html = f"""
            <div class="dash-map-placeholder">
              Aba <b>{h(WS_CIDADES)}</b> não encontrada na planilha de municípios.
            </div>
            """
        except Exception as e:
            erro_txt = friendly_gspread_error(e)
            mapa_svg_html = f"""
            <div class="dash-map-placeholder">
              Erro ao montar mapa.<br><br>
              {h(erro_txt)}
            </div>
            """

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

        body = f"""
        <div class="dash-page">

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
                  <button type="button" class="btn-link orange" onclick="window.print()">Imprimir A3</button>
                </div>

                <div class="print-note">
                  Ajustado para sair em uma única página A3 horizontal.
                </div>
              </div>
            </form>
          </div>

          <div class="a3-page no-break">
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
                </div>

                <div>
                  <img src="{h(LOGO_URL)}" alt="Logo Kidy" class="dash-kidy-logo">
                </div>
              </div>

              <div class="dash-row-top">

                <div class="dash-panel">
                  <div class="dash-panel-title">10 Maiores Clientes</div>
                  <div class="dash-panel-body">
                    {ranking_2026_html}
                  </div>
                </div>

                <div class="dash-panel">
                  <div class="dash-panel-title">10 Maiores Clientes 2025</div>
                  <div class="dash-panel-body">
                    {ranking_2025_html}
                  </div>
                </div>

                <div class="dash-panel">
                  <div class="dash-panel-title">Cidades da Região</div>
                  <div class="dash-panel-body">
                    {mapa_svg_html}
                    <div style="margin-top:6px; text-align:center; font-size:10px; color:#6b7280;">
                      Cidades plotadas: <b>{h(cidades_mapa_qtd)}</b>
                      {" | " + h(mapa_info_msg) if mapa_info_msg else ""}
                    </div>
                  </div>
                </div>

              </div>

              <div class="dash-row-bottom">

                <div class="dash-panel">
                  <div class="dash-panel-title">Clientes sem Compra</div>
                  <div class="dash-panel-body">
                    {clientes_sem_compra_html}
                  </div>
                </div>

                <div class="dash-right-stack">

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
              </div>

              <div style="margin-top:8px;">
                <div class="dash-panel">
                  <div class="dash-panel-title">Agenda Semanal do Representante</div>
                  <div class="dash-panel-body">
                    {agenda_semanal_html}
                  </div>
                </div>
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
              <div class="line"><b>MUNICIPIOS_SHEET_ID RESOLVIDO:</b> {h(map_debug['municipios_sheet_resolved'])}</div>
              <div class="line"><b>WS_CIDADES:</b> {h(map_debug['ws_cidades'])}</div>
              <div class="line"><b>COLUNA CIDADE BASE:</b> {h(cidade_col)}</div>
              <div class="line"><b>COLUNA CIDADE MUNICÍPIOS:</b> {h(map_debug['cidade_muni_col'])}</div>
              <div class="line"><b>COLUNA LAT:</b> {h(map_debug['lat_col'])}</div>
              <div class="line"><b>COLUNA LON:</b> {h(map_debug['lon_col'])}</div>
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
        flash(f"Erro ao abrir dashboard admin: {norm(str(e))}", "err")
        return redirect(url_for("dashboard"))


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not require_login():
        flash("Faça login para continuar.", "err")
        return redirect(url_for("login"))

    sh = connect_gs()
    debug_info = build_debug_sheet_info(sh)
    last_save = get_last_save_debug()

    try:
        ws_base = sh.worksheet(WS_BASE)
    except WorksheetNotFound:
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=get_rep_photo_src(session.get("rep_code", "")) if session.get("user_type") == "rep" else "",
            body=f"<div class='card'><b>Aba não encontrada:</b> {h(WS_BASE)}</div>"
        )

    try:
        ws_listas = sh.worksheet(WS_LISTAS)
    except WorksheetNotFound:
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=get_rep_photo_src(session.get("rep_code", "")) if session.get("user_type") == "rep" else "",
            body=f"<div class='card'><b>Aba não encontrada:</b> {h(WS_LISTAS)}</div>"
        )

    try:
        ensure_edicoes_worksheet(sh)
    except Exception as e:
        flash(str(e), "err")

    headers, base_rows = get_base_structure(ws_base)
    lista_rows = safe_get_all_records(ws_listas)

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
    cidade_col = pick_col_flexible(headers, ["Cidade", "Município", "Municipio"])

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

    sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if (is_admin() and sup_col) else []
    rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if is_admin() else []

    prepared_rows = []

    for idx_base, r in enumerate(base_rows, start=2):
        ck = norm(r.get(key_col, "")) if key_col else ""
        repc = norm(r.get(rep_col, "")) if rep_col else ""

        if not is_admin() and repc != norm(session.get("rep_code", "")):
            continue
        if is_admin() and sup_col and sup_sel and norm(r.get(sup_col, "")) != sup_sel:
            continue
        if is_admin() and rep_sel and repc != rep_sel:
            continue
        if q:
            hay = " ".join([norm(v) for v in r.values()])
            if q.lower() not in hay.lower():
                continue

        row_copy = dict(r)
        row_copy["Data Agenda Visita"] = norm(r.get(data_agenda_col, "")) if data_agenda_col else ""
        row_copy["Mês"] = norm(r.get(mes_col, "")) if mes_col else ""
        row_copy["Semana Atendimento"] = norm(r.get(semana_col, "")) if semana_col else ""
        row_copy["Status Cliente"] = norm(r.get(status_cliente_col, "")) if status_cliente_col else ""
        row_copy["Observações"] = norm(r.get(observacoes_col, "")) if observacoes_col else ""

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

    current_user_photo = ""
    if session.get("user_type") == "rep":
        current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

    rep_card_html = ""

    selected_rep_code = rep_sel if is_admin() else norm(session.get("rep_code", ""))

    if selected_rep_code and rep_col:
        rep_name_base = ""
        rep_sup_base = ""
        rep_reg_base = ""

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

        row_html = f"""
        <tr class="{h(klass)}">
          <td class="nowrap">{h(ck)}</td>
          <td>{h(grupo)}</td>
          <td class="nowrap">{h(repc)}</td>
          <td>{h(nome_rep)}</td>
          <td class="nowrap">{h(supv)}</td>
          <td>{h(cidade)}</td>
          <td class="money nowrap">{h(t24)}</td>
          <td class="money nowrap">{h(t25)}</td>
          <td class="money nowrap">{h(t26)}</td>
          <td class="nowrap"><b>{h(status_cor)}</b></td>

          <td>
            <form id="{form_id}" method="post" action="{url_for('salvar')}">
              <input type="hidden" name="client_key" value="{h(ck)}">
              <input type="hidden" name="rep_code" value="{h(repc)}">
              <input type="hidden" name="base_row_number" value="{h(base_row_number)}">
              {hidden_filters}
            </form>
            <input type="date" name="Data Agenda Visita" value="{h(to_input_date(dav))}" form="{form_id}" style="min-width:155px;">
          </td>

          <td>
            <select name="Mês" form="{form_id}" style="min-width:140px;">
              {opt_html(meses, mes)}
            </select>
          </td>

          <td>
            <select name="Semana Atendimento" form="{form_id}" style="min-width:160px;">
              {opt_html(semanas, sem)}
            </select>
          </td>

          <td>
            <select name="Status Cliente" form="{form_id}" style="min-width:260px;">
              {opt_html(status_list, stc)}
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

    <div class="card">
      <form method="get">
        <div class="grid">
          {filtros_html}
          <div>
            <label>Buscar</label>
            <input name="q" value="{h(q)}" placeholder="cliente/grupo/cidade...">
          </div>
          <div style="display:flex;align-items:flex-end;gap:8px;">
            <button type="submit">Aplicar</button>
            <a href="{url_for('dashboard')}"><button type="button" class="secondary">Limpar</button></a>
          </div>
        </div>
      </form>
    </div>

    <div class="card" style="overflow:auto; max-height:72vh;">
      <table>
        <thead>
          <tr>
            <th>Codigo Grupo Cliente</th>
            <th>Grupo Cliente</th>
            <th>Codigo Representante</th>
            <th>Representante</th>
            <th>Supervisor</th>
            <th>Cidade</th>
            <th>Total 2024</th>
            <th>Total 2025</th>
            <th>Total 2026</th>
            <th>Status Cor</th>
            <th>Data Agenda Visita</th>
            <th>Mês</th>
            <th>Semana Atendimento</th>
            <th>Status Cliente</th>
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

    redirect_args = {k: v for k, v in {"sup": sup, "rep": rep, "q": q}.items() if v}

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

        row_num = int(base_row_number)

        col_data = headers_norm.index("Data Agenda Visita") + 1
        col_mes = headers_norm.index("Mês") + 1
        col_semana = headers_norm.index("Semana Atendimento") + 1
        col_status = headers_norm.index("Status Cliente") + 1
        col_obs = headers_norm.index("Observações") + 1

        ws_base.batch_update(
            [
                {"range": rowcol_to_a1(row_num, col_data), "values": [[data_agenda]]},
                {"range": rowcol_to_a1(row_num, col_mes), "values": [[mes]]},
                {"range": rowcol_to_a1(row_num, col_semana), "values": [[semana]]},
                {"range": rowcol_to_a1(row_num, col_status), "values": [[status_cliente]]},
                {"range": rowcol_to_a1(row_num, col_obs), "values": [[observacoes]]},
            ],
            value_input_option="USER_ENTERED"
        )

        row_values = ws_base.row_values(row_num)

        gravado_data = safe_cell(row_values, col_data)
        gravado_mes = safe_cell(row_values, col_mes)
        gravado_semana = safe_cell(row_values, col_semana)
        gravado_status = safe_cell(row_values, col_status)
        gravado_obs = safe_cell(row_values, col_obs)

        conferiu = (
            gravado_data == norm(data_agenda) and
            gravado_mes == norm(mes) and
            gravado_semana == norm(semana) and
            gravado_status == norm(status_cliente) and
            gravado_obs == norm(observacoes)
        )

        if not conferiu:
            set_last_save_debug({
                "row_num": row_num,
                "client_key": client_key,
                "data_agenda": gravado_data,
                "mes": gravado_mes,
                "semana": gravado_semana,
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
    )