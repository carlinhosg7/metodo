import os
import re
import json
import base64
import traceback
import html
from datetime import datetime, timezone, timedelta

from flask import Flask, request, redirect, url_for, session, render_template_string, flash

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


# =========================
# CONFIG ENV
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-chave").strip()

# AJUSTADO PARA SUA ESTRUTURA REAL
WS_BASE = os.getenv("WS_BASE", "BASE").strip()
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES").strip()
WS_LISTAS = os.getenv("WS_LISTAS", "__LISTAS_VALIDACAO__").strip()

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))

APP_TITLE = "Acompanhamento de clientes"
LOGO_URL = "https://raw.githubusercontent.com/carlinhosg7/metodo/main/logo_kidy.png"


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
app = Flask(__name__)
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


def pick_col_exact(headers, candidates):
    hmap = {normalize_header(h): h for h in headers}
    for cand in candidates:
        key = normalize_header(cand)
        if key in hmap:
            return hmap[key]
    return None


def pick_col_flexible(headers, candidates):
    hmap = {normalize_header(h): h for h in headers}

    for cand in candidates:
        key = normalize_header(cand)
        if key in hmap:
            return hmap[key]

    for h in headers:
        hl = normalize_header(h)
        for cand in candidates:
            if normalize_header(cand) in hl:
                return h

    return None


def clean_color_text(v):
    return norm(v)


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
    for ext in exts:
        rel_path = os.path.join("static", "representantes", f"{codigo}.{ext}")
        if os.path.exists(rel_path):
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


def connect_gs():
    if not SHEET_ID:
        raise RuntimeError("Faltou SHEET_ID nas variáveis de ambiente.")

    info = _load_service_account_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)


def get_or_create_worksheet(sh, title, rows=1000, cols=30, headers=None):
    try:
        ws = sh.worksheet(title)
        return ws
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws


def ensure_headers_if_empty(ws, headers):
    try:
        row1 = [norm(x) for x in ws.row_values(1)]
        if not row1 and headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
    except Exception:
        if headers:
            ws.clear()
            ws.append_row(headers, value_input_option="USER_ENTERED")


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


def open_existing_or_create(sh, title, rows=1000, cols=30, headers=None):
    try:
        ws = sh.worksheet(title)
        if headers:
            ensure_headers_if_empty(ws, headers)
        return ws
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws


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


# =========================
# ERROR HANDLER
# =========================
@app.errorhandler(Exception)
def handle_any_exception(e):
    app.logger.error("ERRO NÃO TRATADO:\n%s", traceback.format_exc())
    msg = norm(str(e)) or "Erro interno."
    body = f"<div class='card'><b>Erro:</b><br><pre style='white-space:pre-wrap'>{h(msg)}</pre></div>"

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
    body {
      font-family: Arial, sans-serif;
      margin: 0;
      background: #ffffff;
      color: #111827;
    }

    .topbar {
      background: #ffffff;
      padding: 12px 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid #d1d5db;
      box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }

    .topbar-right {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .topbar-avatar {
      width: 36px;
      height: 36px;
      border-radius: 50%;
      object-fit: cover;
      border: 1px solid #d1d5db;
      background: #f8fafc;
    }

    .container { padding: 16px; }

    .card {
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 12px;
      padding: 16px;
      margin-bottom: 14px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    .rep-card {
      display: flex;
      align-items: center;
      gap: 16px;
    }

    .rep-photo {
      width: 88px;
      height: 88px;
      border-radius: 50%;
      object-fit: cover;
      border: 2px solid #d1d5db;
      background: #f8fafc;
      flex-shrink: 0;
    }

    .rep-photo-placeholder {
      width: 88px;
      height: 88px;
      border-radius: 50%;
      border: 2px solid #d1d5db;
      background: #f8fafc;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #6b7280;
      font-size: 12px;
      text-align: center;
      flex-shrink: 0;
      padding: 6px;
      box-sizing: border-box;
    }

    label {
      font-size: 12px;
      color: #4b5563;
      display: block;
      margin-bottom: 4px;
      font-weight: 600;
    }

    input, select {
      width: 100%;
      padding: 10px;
      border-radius: 10px;
      border: 1px solid #cbd5e1;
      background: #ffffff;
      color: #111827;
      box-sizing: border-box;
    }

    input:focus, select:focus {
      outline: none;
      border-color: #2563eb;
      box-shadow: 0 0 0 3px rgba(37,99,235,0.12);
    }

    button {
      padding: 10px 14px;
      border-radius: 10px;
      border: 0;
      background: #2563eb;
      color: #fff;
      cursor: pointer;
      font-weight: 600;
    }

    button.secondary { background: #6b7280; }
    button.danger { background: #dc2626; }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      background: #ffffff;
    }

    th, td {
      border-bottom: 1px solid #e5e7eb;
      padding: 10px;
      vertical-align: top;
    }

    th {
      position: sticky;
      top: 0;
      background: #f8fafc;
      color: #374151;
      text-align: left;
      z-index: 2;
    }

    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr 1fr;
      gap: 10px;
    }

    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }

    .msg {
      padding: 10px 12px;
      border-radius: 10px;
      margin-bottom: 10px;
      font-weight: 600;
    }

    .ok {
      background: #ecfdf5;
      border: 1px solid #86efac;
      color: #166534;
    }

    .err {
      background: #fef2f2;
      border: 1px solid #fca5a5;
      color: #991b1b;
    }

    .pill {
      padding: 3px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: #f3f4f6;
      border: 1px solid #d1d5db;
      display: inline-block;
      color: #111827;
    }

    .small { color: #6b7280; font-size: 12px; }
    .hint { color: #6b7280; font-size: 12px; margin-top: 6px; }
    .nowrap { white-space: nowrap; }
    .money { font-variant-numeric: tabular-nums; }

    .login-wrap {
      min-height: calc(100vh - 90px);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }

    .login-card {
      width: 100%;
      max-width: 520px;
      text-align: center;
    }

    .login-logo {
      max-width: 220px;
      width: 100%;
      height: auto;
      margin: 0 auto 18px auto;
      display: block;
    }

    .login-title {
      margin-top: 0;
      margin-bottom: 6px;
      color: #111827;
    }

    .login-subtitle {
      margin-top: 0;
      margin-bottom: 20px;
      color: #6b7280;
      font-size: 14px;
    }

    .row-red { background: rgba(220,38,38,0.16); }
    .row-orange { background: rgba(249,115,22,0.16); }
    .row-yellow { background: rgba(234,179,8,0.18); }
    .row-green { background: rgba(34,197,94,0.16); }
    .row-blue { background: rgba(56,189,248,0.14); }
  </style>
</head>
<body>
  <div class="topbar">
    <div><b>Acompanhamento de clientes</b> <span class="small">| {{ subtitle }}</span></div>
    <div class="topbar-right">
      {% if logged %}
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
      {% for cat,msg in messages %}
        <div class="msg {{ 'ok' if cat=='ok' else 'err' }}">{{ msg }}</div>
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
      <div class="hint">
        Admin: usuário e senha definidos nas variáveis de ambiente.<br>
        Representante: usuário = senha = código numérico.
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

        if u == ADMIN_USER and p == ADMIN_PASS:
            session.clear()
            session.permanent = True
            session["user_type"] = "admin"
            session["user_login"] = u
            session["rep_name"] = ""
            session["rep_code"] = ""

            flash("Logado como ADMIN.", "ok")
            return redirect(url_for("dashboard"))

        if u.isdigit() and p.isdigit() and u == p:
            rep_nome = try_get_rep_name(u)

            session.clear()
            session.permanent = True
            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u
            session["rep_name"] = rep_nome or f"Representante {u}"

            flash(f"Logado como {session['rep_name']}.", "ok")
            return redirect(url_for("dashboard"))

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


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not require_login():
        flash("Faça login para continuar.", "err")
        return redirect(url_for("login"))

    sh = connect_gs()

    # BASE
    try:
        ws_base = sh.worksheet(WS_BASE)
    except WorksheetNotFound:
        flash(f"A aba '{WS_BASE}' não foi encontrada.", "err")
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro de planilha",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=get_rep_photo_src(session.get("rep_code", "")) if session.get("user_type") == "rep" else "",
            body=f"<div class='card'><b>Erro:</b> aba <b>{h(WS_BASE)}</b> não encontrada.</div>"
        )

    # EDICOES - cria se não existir
    ed_headers = [
        "timestamp",
        "user_type",
        "user_login",
        "rep_code",
        "client_key",
        "Data Agenda Visita",
        "Mês",
        "Semana Atendimento",
        "Status Cliente"
    ]
    ws_ed = open_existing_or_create(sh, WS_EDICOES, rows=2000, cols=20, headers=ed_headers)

    # LISTAS - abre a aba real __LISTAS_VALIDACAO__; se não existir, cria
    listas_headers = ["Mês", "Semana Atendimento", "Status Cliente"]
    ws_listas = open_existing_or_create(sh, WS_LISTAS, rows=500, cols=10, headers=listas_headers)

    headers, base_rows = safe_get_raw_rows(ws_base)

    if not base_rows:
        current_user_photo = ""
        if session.get("user_type") == "rep":
            current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Base vazia",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=current_user_photo,
            body=f"<div class='card'>A aba <b>{h(WS_BASE)}</b> está vazia.</div>"
        )

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

    status_cor_col = pick_col_exact(headers, [
        "STATUS COR",
        "Status Cor",
        "STATUSCOR",
        "StatusCor"
    ])

    cliente_novo_col = pick_col_flexible(headers, [
        "Cliente Novo",
        "CLIENTE NOVO",
        "Novo",
        "NOVO",
        "Cliente_Novo"
    ])

    current_user_photo = ""
    if session.get("user_type") == "rep":
        current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

    if not key_col or not rep_col:
        body = """
        <div class='card'>
          <b>Erro de estrutura da BASE</b><br><br>
          A planilha precisa ter pelo menos as colunas:<br>
          - Codigo Grupo Cliente (ou equivalente)<br>
          - Codigo Representante (ou equivalente)
        </div>
        """
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro de estrutura",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=current_user_photo,
            body=body
        )

    if not t2024_col or not t2025_col or not t2026_col:
        body = f"""
        <div class='card'>
          <b>Não encontrei as colunas reais de totais na BASE.</b><br><br>
          Preciso exatamente destas:<br>
          - Total 2024 (PERIODO)<br>
          - Total 2025 (PERIODO)<br>
          - Total 2026 (PERIODO)<br><br>
          <span class='small'>Cabeçalhos encontrados: {h(', '.join(headers))}</span>
        </div>
        """
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro de colunas",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=current_user_photo,
            body=body
        )

    if not status_cor_col and not cliente_novo_col:
        body = """
        <div class='card'>
          <b>Não achei nem 'STATUS COR' nem coluna de 'Cliente Novo' na BASE.</b>
        </div>
        """
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Erro de colunas",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=current_user_photo,
            body=body
        )

    lista_rows = safe_get_all_records(ws_listas)
    meses = unique_list([r.get("Mês", "") for r in lista_rows]) or DEFAULT_MESES
    semanas = unique_list([r.get("Semana Atendimento", "") for r in lista_rows]) or DEFAULT_SEMANAS
    status_list = unique_list([r.get("Status Cliente", "") for r in lista_rows]) or DEFAULT_STATUS

    ed_rows = safe_get_all_records(ws_ed)
    latest = {}
    for r in ed_rows:
        ck = norm(r.get("client_key", ""))
        if ck:
            latest[ck] = {
                "Data Agenda Visita": norm(r.get("Data Agenda Visita", "")),
                "Mês": norm(r.get("Mês", "")),
                "Semana Atendimento": norm(r.get("Semana Atendimento", "")),
                "Status Cliente": norm(r.get("Status Cliente", "")),
            }

    sup_sel = norm(request.args.get("sup", ""))
    rep_sel = norm(request.args.get("rep", ""))
    q = norm(request.args.get("q", ""))

    sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if (is_admin() and sup_col) else []
    rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if is_admin() else []

    prepared_rows = []
    for r in base_rows:
        ck = norm(r.get(key_col, ""))
        repc = norm(r.get(rep_col, ""))

        if not is_admin() and repc != session.get("rep_code"):
            continue

        if is_admin() and sup_col and sup_sel:
            if norm(r.get(sup_col, "")) != sup_sel:
                continue

        if is_admin() and rep_sel:
            if repc != rep_sel:
                continue

        if q:
            hay = " ".join([norm(v) for v in r.values()])
            if q.lower() not in hay.lower():
                continue

        row_copy = dict(r)

        if ck in latest:
            row_copy["Data Agenda Visita"] = latest[ck]["Data Agenda Visita"]
            row_copy["Mês"] = latest[ck]["Mês"]
            row_copy["Semana Atendimento"] = latest[ck]["Semana Atendimento"]
            row_copy["Status Cliente"] = latest[ck]["Status Cliente"]
        else:
            row_copy.setdefault("Data Agenda Visita", "")
            row_copy.setdefault("Mês", "")
            row_copy.setdefault("Semana Atendimento", "")
            row_copy.setdefault("Status Cliente", "")

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
            norm(r.get(key_col, ""))
        )
    )

    out_rows = prepared_rows[:PAGE_SIZE]

    rep_card_html = ""

    selected_rep_code = rep_sel if is_admin() else norm(session.get("rep_code", ""))

    if selected_rep_code and nome_rep_col:
        rep_name_base = ""
        rep_sup_base = ""
        rep_reg_base = ""

        for r in base_rows:
            if norm(r.get(rep_col, "")) == selected_rep_code:
                rep_name_base = norm(r.get(nome_rep_col, ""))
                rep_sup_base = norm(r.get(sup_col, "")) if sup_col else ""
                rep_reg_base = ""
                if rep_name_base:
                    break

        foto_url = get_rep_photo_src(selected_rep_code)
        nome_card = rep_name_base or selected_rep_code
        sup_card = rep_sup_base
        regiao_card = rep_reg_base

        foto_html = (
            f'<img src="{h(foto_url)}" alt="Foto do representante" class="rep-photo">'
            if foto_url else
            '<div class="rep-photo-placeholder">Sem foto</div>'
        )

        rep_card_html = f"""
        <div class="card">
          <div class="rep-card">
            {foto_html}
            <div>
              <div style="font-size:20px;font-weight:700;">{h(nome_card)}</div>
              <div class="small">Código: {h(selected_rep_code)}</div>
              <div class="small">Supervisor: {h(sup_card)}</div>
              <div class="small">Região: {h(regiao_card)}</div>
            </div>
          </div>
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
        ck = norm(r.get(key_col, ""))
        grupo = norm(r.get(grupo_col, "")) if grupo_col else ""
        repc = norm(r.get(rep_col, ""))
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

        status_cor = r.get("_status_cor", "")
        klass = r.get("_row_class", "")

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
            <div style="margin-top:8px;">
              <button type="submit" form="{form_id}">Gravar</button>
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
            {''.join([f"<option value='{h(s)}' {'selected' if s == sup_sel else ''}>{h(s)}</option>" for s in sup_list])}
          </select>
        </div>
        <div>
          <label>Filtro Representante</label>
          <select name="rep">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{h(r)}' {'selected' if r == rep_sel else ''}>{h(r)}</option>" for r in rep_list])}
          </select>
        </div>
        """

    body = f"""
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
        <div class="hint">
          Total 2024/2025/2026 são exibidos exatamente da BASE, usando:
          <b>Total 2024 (PERIODO)</b>,
          <b>Total 2025 (PERIODO)</b>,
          <b>Total 2026 (PERIODO)</b>.
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
        subtitle=f"Planilha: {h(WS_BASE)}",
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

    sup = norm(request.form.get("sup", ""))
    rep = norm(request.form.get("rep", ""))
    q = norm(request.form.get("q", ""))

    redirect_args = {k: v for k, v in {"sup": sup, "rep": rep, "q": q}.items() if v}

    if not client_key:
        flash("client_key vazio.", "err")
        return redirect(url_for("dashboard", **redirect_args))

    if user_type == "rep" and rep_code_form != session.get("rep_code"):
        flash("Você não pode gravar alterações em clientes de outro representante.", "err")
        return redirect(url_for("dashboard", **redirect_args))

    try:
        sh = connect_gs()

        ed_headers = [
            "timestamp",
            "user_type",
            "user_login",
            "rep_code",
            "client_key",
            "Data Agenda Visita",
            "Mês",
            "Semana Atendimento",
            "Status Cliente"
        ]

        ws_ed = open_existing_or_create(
            sh,
            WS_EDICOES,
            rows=2000,
            cols=20,
            headers=ed_headers
        )

        data_agenda = from_input_date(request.form.get("Data Agenda Visita", ""))
        mes = norm(request.form.get("Mês", ""))
        semana = norm(request.form.get("Semana Atendimento", ""))
        status_cliente = norm(request.form.get("Status Cliente", ""))

        row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            user_type,
            user_login,
            rep_code_form,
            client_key,
            data_agenda,
            mes,
            semana,
            status_cliente,
        ]

        ws_ed.append_row(row, value_input_option="USER_ENTERED")
        flash("Alteração gravada com sucesso.", "ok")

    except Exception as e:
        app.logger.error("Erro ao gravar na planilha:\n%s", traceback.format_exc())
        flash(f"Erro ao gravar na planilha: {norm(str(e))}", "err")

    return redirect(url_for("dashboard", **redirect_args))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)