import os
import re
import json
import base64
import traceback
from datetime import datetime, timezone

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

WS_BASE = os.getenv("WS_BASE", "BASE").strip()
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES").strip()
WS_LISTAS = os.getenv("WS_LISTAS", "LISTAS").strip()

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


# =========================
# HELPERS
# =========================
def norm(s):
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


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
    return "user_type" in session


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


def parse_money_to_float(v):
    s = norm(v)
    if s == "":
        return 0.0

    s = s.replace("R$", "").replace(" ", "")

    try:
        # padrão BR: 1.234,56
        if "," in s:
            s2 = s.replace(".", "").replace(",", ".")
            return float(s2)

        # padrão simples: 1234.56
        return float(s)
    except Exception:
        return 0.0


def fmt_money(v):
    """
    Exibe exatamente o valor da base quando já vier em formato texto.
    Se vier numérico, formata em BR.
    """
    s = norm(v)
    if s == "":
        return ""

    # se já parece moeda brasileira, só devolve do jeito que veio
    if "," in s or "." in s:
        # tenta validar; se for válido, devolve o mesmo texto da base
        x = parse_money_to_float(s)
        if x != 0.0 or s in {"0", "0,0", "0,00", "0.00", "0,000", "0.000"}:
            return s

    # se vier número puro
    x = parse_money_to_float(s)
    inteiro, frac = f"{x:.2f}".split(".")
    inteiro = inteiro[::-1]
    inteiro = ".".join([inteiro[i:i+3] for i in range(0, len(inteiro), 3)])[::-1]
    return f"{inteiro},{frac}"


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
    """
    ORDEM:
    1 - VERMELHO
    2 - LARANJA
    3 - AMARELO
    4 - VERDE
    5 - AZUL
    """
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
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        if headers:
            ws.append_row(headers)
        return ws

    if headers:
        row1 = [norm(x) for x in ws.row_values(1)]
        if row1 != headers:
            ws.clear()
            ws.append_row(headers)
    return ws


def safe_get_all_records(ws):
    try:
        return ws.get_all_records()
    except Exception:
        return []


def safe_get_raw_rows(ws):
    """
    Lê a aba exatamente como está visível na planilha.
    Não soma, não agrega, não recalcula.
    """
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

        row = {}
        for i, h in enumerate(headers):
            row[h] = raw[i]
        rows.append(row)

    return headers, rows


# =========================
# ERROR HANDLER
# =========================
@app.errorhandler(Exception)
def handle_any_exception(e):
    app.logger.error("ERRO NÃO TRATADO:\n%s", traceback.format_exc())
    msg = norm(str(e)) or "Erro interno."
    body = f"<div class='card'><b>Erro:</b><br><pre style='white-space:pre-wrap'>{msg}</pre></div>"

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

    .grid { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }

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
    </form>
  </div>
</div>
"""


# =========================
# ROUTES
# =========================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = norm(request.form.get("user"))
        p = norm(request.form.get("pass"))

        if u == ADMIN_USER and p == ADMIN_PASS:
            session["user_type"] = "admin"
            session["user_login"] = u
            session["rep_name"] = ""
            session["rep_code"] = ""
            flash("Logado como ADMIN.", "ok")
            return redirect(url_for("dashboard"))

        if u and p and u == p and u.isdigit():
            rep_nome = ""

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

                if rep_col and nome_rep_col:
                    for row in base_rows:
                        if norm(row.get(rep_col, "")) == u:
                            rep_nome = norm(row.get(nome_rep_col, ""))
                            if rep_nome:
                                break
            except Exception:
                rep_nome = ""

            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u
            session["rep_name"] = rep_nome

            if rep_nome:
                flash(f"Logado como Representante {rep_nome}.", "ok")
            else:
                flash(f"Logado como Representante {u}.", "ok")

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
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    sh = connect_gs()
    ws_base = sh.worksheet(WS_BASE)

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
    ws_ed = get_or_create_worksheet(sh, WS_EDICOES, rows=2000, cols=20, headers=ed_headers)

    listas_headers = ["Mês", "Semana Atendimento", "Status Cliente"]
    ws_listas = get_or_create_worksheet(sh, WS_LISTAS, rows=500, cols=10, headers=listas_headers)

    headers, base_rows = safe_get_raw_rows(ws_base)

    if not base_rows:
        current_user_photo = ""
        if session.get("user_type") == "rep":
            current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

        flash(f"A aba {WS_BASE} está vazia.", "err")
        return render_template_string(
            BASE_HTML,
            title=APP_TITLE,
            subtitle="Base vazia",
            logged=True,
            user_login=session.get("user_login"),
            user_name=session.get("rep_name", ""),
            user_type=session.get("user_type"),
            user_photo_url=current_user_photo,
            body="<div class='card'>Sem dados na BASE.</div>"
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

    # AQUI ESTÁ O AJUSTE PRINCIPAL:
    # PEGA DIRETO AS COLUNAS DE TOTAL, SEM FICAR "ADIVINHANDO" 2024/2025/2026 SOLTOS
    t2024_col = pick_col_exact(headers, ["Total 2024", "TOTAL 2024", "Total2024", "TOTAL2024"])
    t2025_col = pick_col_exact(headers, ["Total 2025", "TOTAL 2025", "Total2025", "TOTAL2025"])
    t2026_col = pick_col_exact(headers, ["Total 2026", "TOTAL 2026", "Total2026", "TOTAL2026"])

    status_cor_col = pick_col_exact(headers, [
        "Status Cor",
        "STATUS COR",
        "StatusCor",
        "STATUSCOR"
    ])

    cliente_novo_col = pick_col_flexible(headers, [
        "Cliente Novo",
        "CLIENTE NOVO",
        "Novo",
        "NOVO",
        "Cliente_Novo"
    ])

    if not key_col or not rep_col:
        flash("BASE precisa ter 'Codigo Grupo Cliente' e 'Codigo Representante'.", "err")
        return redirect(url_for("logout"))

    if not status_cor_col and not cliente_novo_col:
        flash("Não achei nem 'Status Cor' nem coluna de 'Cliente Novo' na BASE.", "err")
        return redirect(url_for("logout"))

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

    current_user_photo = ""
    if session.get("user_type") == "rep":
        current_user_photo = get_rep_photo_src(session.get("rep_code", ""))

    rep_card_html = ""

    selected_rep_code = ""
    if is_admin():
        selected_rep_code = rep_sel
    else:
        selected_rep_code = norm(session.get("rep_code", ""))

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
            f'<img src="{foto_url}" alt="Foto do representante" class="rep-photo">'
            if foto_url else
            '<div class="rep-photo-placeholder">Sem foto</div>'
        )

        rep_card_html = f"""
        <div class="card">
          <div class="rep-card">
            {foto_html}
            <div>
              <div style="font-size:20px;font-weight:700;">{nome_card}</div>
              <div class="small">Código: {selected_rep_code}</div>
              <div class="small">Supervisor: {sup_card}</div>
              <div class="small">Região: {regiao_card}</div>
            </div>
          </div>
        </div>
        """

    def opt_html(options, selected):
        out = ["<option value=''></option>"]
        for o in options:
            sel = "selected" if norm(o) == norm(selected) else ""
            out.append(f"<option value='{o}' {sel}>{o}</option>")
        return "\n".join(out)

    table_rows = []
    for r in out_rows:
        ck = norm(r.get(key_col, ""))
        grupo = norm(r.get(grupo_col, "")) if grupo_col else ""
        repc = norm(r.get(rep_col, ""))
        nome_rep = norm(r.get(nome_rep_col, "")) if nome_rep_col else ""
        supv = norm(r.get(sup_col, "")) if sup_col else ""
        cidade = norm(r.get(cidade_col, "")) if cidade_col else ""

        # PEGA DIRETO DA BASE
        t24_raw = r.get(t2024_col, "") if t2024_col else ""
        t25_raw = r.get(t2025_col, "") if t2025_col else ""
        t26_raw = r.get(t2026_col, "") if t2026_col else ""

        t24 = fmt_money(t24_raw)
        t25 = fmt_money(t25_raw)
        t26 = fmt_money(t26_raw)

        dav = norm(r.get("Data Agenda Visita", ""))
        mes = norm(r.get("Mês", ""))
        sem = norm(r.get("Semana Atendimento", ""))
        stc = norm(r.get("Status Cliente", ""))

        status_cor = r.get("_status_cor", "")
        klass = r.get("_row_class", "")

        row_html = f"""
        <tr class="{klass}">
          <td class="nowrap">{ck}</td>
          <td>{grupo}</td>
          <td class="nowrap">{repc}</td>
          <td>{nome_rep}</td>
          <td class="nowrap">{supv}</td>
          <td>{cidade}</td>
          <td class="money nowrap">{t24}</td>
          <td class="money nowrap">{t25}</td>
          <td class="money nowrap">{t26}</td>
          <td class="nowrap"><b>{status_cor}</b></td>
          <td>
            <form method="post" action="{url_for('salvar')}" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
              <input type="hidden" name="client_key" value="{ck}">
              <input type="hidden" name="rep_code" value="{repc}">
              <input type="date" name="Data Agenda Visita" value="{dav}" style="min-width:155px;">
          </td>
          <td>
              <select name="Mês" style="min-width:140px;">
                {opt_html(meses, mes)}
              </select>
          </td>
          <td>
              <select name="Semana Atendimento" style="min-width:160px;">
                {opt_html(semanas, sem)}
              </select>
          </td>
          <td>
              <select name="Status Cliente" style="min-width:260px;">
                {opt_html(status_list, stc)}
              </select>
              <button type="submit">Gravar</button>
            </form>
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
            {''.join([f"<option value='{s}' {'selected' if s == sup_sel else ''}>{s}</option>" for s in sup_list])}
          </select>
        </div>
        <div>
          <label>Filtro Representante</label>
          <select name="rep">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{r}' {'selected' if r == rep_sel else ''}>{r}</option>" for r in rep_list])}
          </select>
        </div>
        """

    debug_totais = f"""
    <div class="hint">
      Colunas localizadas na BASE:
      Total 2024 = <b>{t2024_col or 'NÃO ENCONTRADA'}</b> |
      Total 2025 = <b>{t2025_col or 'NÃO ENCONTRADA'}</b> |
      Total 2026 = <b>{t2026_col or 'NÃO ENCONTRADA'}</b>
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
            <input name="q" value="{q}" placeholder="cliente/grupo/cidade...">
          </div>
          <div style="display:flex;align-items:flex-end;gap:8px;">
            <button type="submit">Aplicar</button>
            <a href="{url_for('dashboard')}"><button type="button" class="secondary">Limpar</button></a>
          </div>
        </div>
        <div class="hint">
          Status Cor vindo da BASE. Se Status Cor vier vazio e Cliente Novo estiver marcado, a linha fica azul.
        </div>
        {debug_totais}
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
        return redirect(url_for("login"))

    user_type = session.get("user_type")
    user_login = session.get("user_login")

    client_key = norm(request.form.get("client_key", ""))
    rep_code_form = norm(request.form.get("rep_code", ""))

    if not client_key:
        flash("Client_key vazio.", "err")
        return redirect(url_for("dashboard"))

    if user_type == "rep":
        if rep_code_form != session.get("rep_code"):
            flash("Você não pode gravar alterações em clientes de outro representante.", "err")
            return redirect(url_for("dashboard"))

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

    ws_ed = get_or_create_worksheet(
        sh,
        WS_EDICOES,
        rows=2000,
        cols=20,
        headers=ed_headers
    )

    row = [
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        user_type,
        user_login,
        rep_code_form,
        client_key,
        norm(request.form.get("Data Agenda Visita", "")),
        norm(request.form.get("Mês", "")),
        norm(request.form.get("Semana Atendimento", "")),
        norm(request.form.get("Status Cliente", "")),
    ]

    ws_ed.append_row(row)

    flash("Alteração gravada com sucesso.", "ok")
    return redirect(url_for("dashboard"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)