# =========================================================
# app.py
# Flask + Google Sheets + Cache + Retry 429
# =========================================================

import os
import json
import time
import base64
import traceback
import html
from functools import lru_cache
from datetime import datetime, timezone, timedelta

from flask import (
    Flask,
    request,
    redirect,
    url_for,
    session,
    render_template_string,
    flash
)

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError


# =========================================================
# CONFIG
# =========================================================
APP_TITLE = os.getenv("APP_TITLE", "Acompanhamento de clientes").strip()

SHEET_ID = os.getenv("SHEET_ID", "").strip()

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-chave").strip()

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").strip().lower() == "true"

# Nomes das abas
WS_BASE = os.getenv("WS_BASE", "BASE").strip()
#WS_AGENDA = os.getenv("WS_AGENDA", "AGENDA").strip()
WS_CARTEIRA = os.getenv("WS_CARTEIRA", "CARTEIRA").strip()

# TTL de cache em segundos
CACHE_TTL = int(os.getenv("CACHE_TTL", "60").strip())

# Timezone Brasil
BR_TZ = timezone(timedelta(hours=-3))

# Scopes Google
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

app = Flask(__name__)
app.secret_key = SECRET_KEY


# =========================================================
# CACHE SIMPLES EM MEMÓRIA
# =========================================================
_MEM_CACHE = {}


def cache_get(key):
    item = _MEM_CACHE.get(key)
    if not item:
        return None

    expires_at, value = item
    if time.time() > expires_at:
        _MEM_CACHE.pop(key, None)
        return None

    return value


def cache_set(key, value, ttl=CACHE_TTL):
    _MEM_CACHE[key] = (time.time() + ttl, value)


def cache_delete(key):
    _MEM_CACHE.pop(key, None)


def cache_clear_all():
    _MEM_CACHE.clear()


# =========================================================
# HELPERS
# =========================================================
def now_br():
    return datetime.now(BR_TZ)


def format_dt_br(dt):
    if not dt:
        return ""
    try:
        return dt.astimezone(BR_TZ).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(dt)


def safe_str(v):
    if v is None:
        return ""
    return str(v).strip()


def to_float(v, default=0.0):
    try:
        s = safe_str(v).replace(".", "").replace(",", ".")
        return float(s) if s else default
    except Exception:
        return default


def to_int(v, default=0):
    try:
        s = safe_str(v)
        return int(float(s)) if s else default
    except Exception:
        return default


def normalize_header(text):
    s = safe_str(text).lower()
    s = s.replace("á", "a").replace("à", "a").replace("ã", "a").replace("â", "a")
    s = s.replace("é", "e").replace("ê", "e")
    s = s.replace("í", "i")
    s = s.replace("ó", "o").replace("ô", "o").replace("õ", "o")
    s = s.replace("ú", "u")
    s = s.replace("ç", "c")
    s = " ".join(s.split())
    return s


def find_col(row_headers, possible_names):
    norm_map = {normalize_header(c): i for i, c in enumerate(row_headers)}
    for name in possible_names:
        key = normalize_header(name)
        if key in norm_map:
            return norm_map[key]
    return None


def rows_to_dicts(values):
    if not values:
        return []

    headers = values[0]
    data_rows = values[1:]

    records = []
    for row in data_rows:
        row = row + [""] * (len(headers) - len(row))
        rec = {headers[i]: row[i] for i in range(len(headers))}
        records.append(rec)
    return records


def require_login():
    return session.get("logged_in") is True


def current_user():
    return session.get("username", "")


# =========================================================
# GOOGLE SHEETS
# =========================================================
def _load_service_account_info():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    if GOOGLE_SA_JSON_B64:
        decoded = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
        return json.loads(decoded)

    raise RuntimeError(
        "Credenciais do Google não configuradas. "
        "Defina GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SA_JSON_B64."
    )


@lru_cache(maxsize=1)
def get_gspread_client():
    creds_info = _load_service_account_info()
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def with_backoff(func, *args, **kwargs):
    """
    Retry inteligente para erro 429 / 5xx
    """
    delays = [1, 2, 4, 8, 16]
    last_err = None

    for delay in delays:
        try:
            return func(*args, **kwargs)
        except APIError as e:
            last_err = e
            status = None
            try:
                status = e.response.status_code
            except Exception:
                pass

            if status in (429, 500, 502, 503, 504):
                time.sleep(delay)
                continue
            raise
        except Exception as e:
            last_err = e
            raise

    raise last_err


@lru_cache(maxsize=1)
def connect_gs_by_key(sheet_key):
    gc = get_gspread_client()
    return with_backoff(gc.open_by_key, sheet_key)


def connect_gs():
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID não foi configurado.")
    return connect_gs_by_key(SHEET_ID)


def get_worksheet(ws_name):
    cache_key = f"worksheet_obj::{ws_name}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    sh = connect_gs()
    ws = with_backoff(sh.worksheet, ws_name)
    cache_set(cache_key, ws, ttl=CACHE_TTL)
    return ws


def get_sheet_values(ws_name, force_refresh=False):
    """
    Lê todos os valores de uma aba com cache.
    """
    cache_key = f"sheet_values::{ws_name}"

    if not force_refresh:
        cached = cache_get(cache_key)
        if cached is not None:
            return cached

    ws = get_worksheet(ws_name)
    values = with_backoff(ws.get_all_values)
    cache_set(cache_key, values, ttl=CACHE_TTL)
    return values


def get_sheet_records(ws_name, force_refresh=False):
    values = get_sheet_values(ws_name, force_refresh=force_refresh)
    return rows_to_dicts(values)


def clear_sheet_cache(ws_name=None):
    if ws_name:
        cache_delete(f"worksheet_obj::{ws_name}")
        cache_delete(f"sheet_values::{ws_name}")
    else:
        cache_clear_all()
        try:
            connect_gs_by_key.cache_clear()
        except Exception:
            pass
        try:
            get_gspread_client.cache_clear()
        except Exception:
            pass


# =========================================================
# EXTRAÇÃO / LÓGICA DE NEGÓCIO
# =========================================================
def parse_base_data():
    """
    Espera uma aba BASE com colunas aproximadas como:
    - Código Cliente / Codigo Cliente
    - Cliente / Nome Cliente / Razao Social
    - Código Representante
    - Representante
    - Código Supervisor
    - Supervisor
    - Cidade
    - UF
    - Data Última Compra
    - Valor / Vlr Venda / Vlr Total
    """
    values = get_sheet_values(WS_BASE)
    if not values:
        return {
            "records": [],
            "headers": [],
            "kpis": {
                "total_clientes": 0,
                "total_representantes": 0,
                "total_supervisores": 0,
                "valor_total": 0.0,
            }
        }

    headers = values[0]
    rows = values[1:]

    idx_cod_cliente = find_col(headers, [
        "Código Cliente", "Codigo Cliente", "Cod Cliente", "Cliente ID"
    ])
    idx_cliente = find_col(headers, [
        "Cliente", "Nome Cliente", "Razão Social", "Razao Social"
    ])
    idx_cod_rep = find_col(headers, [
        "Código Representante", "Codigo Representante", "Cod Rep", "Representante Codigo"
    ])
    idx_rep = find_col(headers, [
        "Representante", "Nome Representante"
    ])
    idx_cod_sup = find_col(headers, [
        "Código Supervisor", "Codigo Supervisor", "Cod Supervisor"
    ])
    idx_sup = find_col(headers, [
        "Supervisor", "Nome Supervisor"
    ])
    idx_cidade = find_col(headers, [
        "Cidade"
    ])
    idx_uf = find_col(headers, [
        "UF", "Estado"
    ])
    idx_ultima_compra = find_col(headers, [
        "Data Última Compra", "Data Ultima Compra", "Última Compra", "Ultima Compra"
    ])
    idx_valor = find_col(headers, [
        "Valor", "Vlr Venda", "Vlr Total", "Valor Total", "Vlr"
    ])

    records = []
    clientes_set = set()
    reps_set = set()
    sups_set = set()
    valor_total = 0.0

    for row in rows:
        row = row + [""] * (len(headers) - len(row))

        cod_cliente = row[idx_cod_cliente] if idx_cod_cliente is not None else ""
        cliente = row[idx_cliente] if idx_cliente is not None else ""
        cod_rep = row[idx_cod_rep] if idx_cod_rep is not None else ""
        rep = row[idx_rep] if idx_rep is not None else ""
        cod_sup = row[idx_cod_sup] if idx_cod_sup is not None else ""
        sup = row[idx_sup] if idx_sup is not None else ""
        cidade = row[idx_cidade] if idx_cidade is not None else ""
        uf = row[idx_uf] if idx_uf is not None else ""
        ultima_compra = row[idx_ultima_compra] if idx_ultima_compra is not None else ""
        valor = row[idx_valor] if idx_valor is not None else ""

        valor_num = to_float(valor, 0.0)

        rec = {
            "cod_cliente": safe_str(cod_cliente),
            "cliente": safe_str(cliente),
            "cod_rep": safe_str(cod_rep),
            "representante": safe_str(rep),
            "cod_sup": safe_str(cod_sup),
            "supervisor": safe_str(sup),
            "cidade": safe_str(cidade),
            "uf": safe_str(uf),
            "ultima_compra": safe_str(ultima_compra),
            "valor": valor_num,
        }
        records.append(rec)

        if rec["cod_cliente"] or rec["cliente"]:
            clientes_set.add((rec["cod_cliente"], rec["cliente"]))
        if rec["cod_rep"] or rec["representante"]:
            reps_set.add((rec["cod_rep"], rec["representante"]))
        if rec["cod_sup"] or rec["supervisor"]:
            sups_set.add((rec["cod_sup"], rec["supervisor"]))

        valor_total += valor_num

    return {
        "records": records,
        "headers": headers,
        "kpis": {
            "total_clientes": len(clientes_set),
            "total_representantes": len(reps_set),
            "total_supervisores": len(sups_set),
            "valor_total": valor_total,
        }
    }

def parse_carteira_data():
    """
    Espera uma aba CARTEIRA com colunas aproximadas como:
    - Código Cliente
    - Cliente
    - Código Representante
    - Representante
    - Supervisor
    - Cidade
    - UF
    - Situação
    """
    try:
        values = get_sheet_values(WS_CARTEIRA)
    except WorksheetNotFound:
        # Se não existir aba CARTEIRA, usa BASE como fallback
        base = parse_base_data()
        return {
            "records": base["records"],
            "headers": base["headers"],
            "total": len(base["records"])
        }

    if not values:
        return {"records": [], "headers": [], "total": 0}

    headers = values[0]
    rows = values[1:]

    idx_cod_cliente = find_col(headers, ["Código Cliente", "Codigo Cliente", "Cod Cliente"])
    idx_cliente = find_col(headers, ["Cliente", "Nome Cliente", "Razão Social", "Razao Social"])
    idx_cod_rep = find_col(headers, ["Código Representante", "Codigo Representante", "Cod Rep"])
    idx_rep = find_col(headers, ["Representante"])
    idx_sup = find_col(headers, ["Supervisor"])
    idx_cidade = find_col(headers, ["Cidade"])
    idx_uf = find_col(headers, ["UF", "Estado"])
    idx_situacao = find_col(headers, ["Situação", "Situacao", "Status"])

    records = []

    for row in rows:
        row = row + [""] * (len(headers) - len(row))
        records.append({
            "cod_cliente": safe_str(row[idx_cod_cliente]) if idx_cod_cliente is not None else "",
            "cliente": safe_str(row[idx_cliente]) if idx_cliente is not None else "",
            "cod_rep": safe_str(row[idx_cod_rep]) if idx_cod_rep is not None else "",
            "representante": safe_str(row[idx_rep]) if idx_rep is not None else "",
            "supervisor": safe_str(row[idx_sup]) if idx_sup is not None else "",
            "cidade": safe_str(row[idx_cidade]) if idx_cidade is not None else "",
            "uf": safe_str(row[idx_uf]) if idx_uf is not None else "",
            "situacao": safe_str(row[idx_situacao]) if idx_situacao is not None else "",
        })

    return {
        "records": records,
        "headers": headers,
        "total": len(records)
    }


# =========================================================
# HTML BASE
# =========================================================
BASE_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: #f5f6fa;
      color: #222;
    }
    .topbar {
      background: #0f172a;
      color: #fff;
      padding: 14px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    .brand {
      font-size: 18px;
      font-weight: bold;
    }
    .nav {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .nav a, .nav span {
      color: #fff;
      text-decoration: none;
      padding: 8px 12px;
      border-radius: 8px;
      background: rgba(255,255,255,0.08);
      font-size: 14px;
    }
    .nav a:hover {
      background: rgba(255,255,255,0.16);
    }
    .container {
      max-width: 1280px;
      margin: 20px auto;
      padding: 0 16px;
    }
    .card {
      background: #fff;
      border-radius: 14px;
      box-shadow: 0 4px 18px rgba(0,0,0,0.08);
      padding: 18px;
      margin-bottom: 18px;
    }
    .grid {
      display: grid;
      gap: 16px;
    }
    .grid-4 {
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }
    .kpi {
      border-left: 6px solid #2563eb;
      padding: 16px;
      background: #fff;
      border-radius: 14px;
      box-shadow: 0 4px 18px rgba(0,0,0,0.08);
    }
    .kpi-title {
      font-size: 13px;
      color: #666;
      margin-bottom: 6px;
    }
    .kpi-value {
      font-size: 28px;
      font-weight: 700;
      color: #111827;
    }
    h1, h2, h3 {
      margin-top: 0;
    }
    form .row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }
    input, select, button {
      width: 100%;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid #d1d5db;
      font-size: 14px;
    }
    button {
      background: #2563eb;
      color: white;
      border: none;
      cursor: pointer;
      font-weight: bold;
    }
    button:hover {
      background: #1d4ed8;
    }
    .btn-secondary {
      background: #64748b;
    }
    .btn-secondary:hover {
      background: #475569;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 780px;
    }
    th, td {
      border-bottom: 1px solid #e5e7eb;
      text-align: left;
      padding: 10px 8px;
      font-size: 14px;
      vertical-align: top;
    }
    th {
      background: #f8fafc;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .flash {
      padding: 12px 14px;
      margin-bottom: 14px;
      border-radius: 10px;
      font-size: 14px;
    }
    .flash-success {
      background: #dcfce7;
      color: #166534;
    }
    .flash-error {
      background: #fee2e2;
      color: #991b1b;
    }
    .muted {
      color: #6b7280;
      font-size: 13px;
    }
    .login-box {
      max-width: 420px;
      margin: 60px auto;
    }
    .error-box {
      white-space: pre-wrap;
      font-family: Consolas, monospace;
      font-size: 12px;
      background: #111827;
      color: #f8fafc;
      border-radius: 12px;
      padding: 16px;
      overflow-x: auto;
    }
    .badge {
      display: inline-block;
      padding: 5px 10px;
      border-radius: 999px;
      background: #e2e8f0;
      font-size: 12px;
      color: #334155;
    }
  </style>
</head>
<body>
  {% if show_nav %}
  <div class="topbar">
    <div class="brand">{{ app_title }}</div>
    <div class="nav">
      <a href="{{ url_for('dashboard') }}">Dashboard</a>
      <a href="{{ url_for('carteira') }}">Carteira</a>
      <a href="{{ url_for('admin_dashboard') }}">Admin</a>
      <span>{{ current_user }}{% if session_role %} ({{ session_role }}){% endif %}</span>
      <a href="{{ url_for('logout') }}">Sair</a>
    </div>
  </div>
  {% endif %}

  <div class="container">
    {% with messages = get_flashed_messages(with_categories=true) %}
      {% if messages %}
        {% for category, message in messages %}
          <div class="flash {{ 'flash-error' if category == 'error' else 'flash-success' }}">
            {{ message }}
          </div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    {{ content|safe }}
  </div>
</body>
</html>
"""


def render_page(title, content, show_nav=True):
    return render_template_string(
        BASE_HTML,
        title=title,
        app_title=APP_TITLE,
        content=content,
        show_nav=show_nav,
        current_user=current_user(),
        session_role=session.get("role", "")
    )


# =========================================================
# ROTAS
# =========================================================
@app.route("/")
def home():
    if require_login():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = safe_str(request.form.get("username"))
        pwd = safe_str(request.form.get("password"))

        if user == ADMIN_USER and pwd == ADMIN_PASS:
            session["logged_in"] = True
            session["username"] = user
            session["role"] = "admin"
            flash("Login realizado com sucesso.", "success")
            return redirect(url_for("dashboard"))

        flash("Usuário ou senha inválidos.", "error")
        return redirect(url_for("login"))

    content = f"""
    <div class="login-box">
      <div class="card">
        <h2>{html.escape(APP_TITLE)}</h2>
        <p class="muted">Entre com suas credenciais para acessar o sistema.</p>

        <form method="post">
          <div class="row">
            <div>
              <label>Usuário</label>
              <input type="text" name="username" placeholder="Digite o usuário" required>
            </div>
          </div>

          <div class="row">
            <div>
              <label>Senha</label>
              <input type="password" name="password" placeholder="Digite a senha" required>
            </div>
          </div>

          <button type="submit">Entrar</button>
        </form>
      </div>
    </div>
    """
    return render_page("Login", content, show_nav=False)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada com sucesso.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    try:
        base = parse_base_data()

        kpis = base["kpis"]
        base_records = base["records"]

        # filtros
        filtro_rep = safe_str(request.args.get("rep"))
        filtro_sup = safe_str(request.args.get("sup"))
        filtro_cliente = safe_str(request.args.get("cliente"))

        filtrados = []

        for r in base_records:
            ok = True

            if filtro_rep and filtro_rep.lower() not in (r["representante"] + " " + r["cod_rep"]).lower():
                ok = False

            if filtro_sup and filtro_sup.lower() not in (r["supervisor"] + " " + r["cod_sup"]).lower():
                ok = False

            if filtro_cliente and filtro_cliente.lower() not in (r["cliente"] + " " + r["cod_cliente"]).lower():
                ok = False

            if ok:
                filtrados.append(r)

        # -------------------------------
        # LINHAS DA TABELA DE CLIENTES
        # -------------------------------

        linhas_clientes = []

        for r in filtrados[:200]:

            linhas_clientes.append(f"""
            <tr>
              <td>{html.escape(r["cod_cliente"])}</td>
              <td>{html.escape(r["cliente"])}</td>
              <td>{html.escape(r["cod_rep"])}</td>
              <td>{html.escape(r["representante"])}</td>
              <td>{html.escape(r["cod_sup"])}</td>
              <td>{html.escape(r["supervisor"])}</td>
              <td>{html.escape(r["cidade"])}</td>
              <td>{html.escape(r["uf"])}</td>
              <td>{html.escape(r["ultima_compra"])}</td>
              <td>R$ {r["valor"]:,.2f}</td>
            </tr>
            """)

        content = f"""
        <div class="card">
          <h1>Dashboard</h1>

          <p class="muted">
            Última atualização: {format_dt_br(now_br())}
            &nbsp;|&nbsp;
            Cache: {CACHE_TTL}s
          </p>

          <form method="get">
            <div class="row">

              <div>
                <label>Representante</label>
                <input type="text" name="rep" value="{html.escape(filtro_rep)}">
              </div>

              <div>
                <label>Supervisor</label>
                <input type="text" name="sup" value="{html.escape(filtro_sup)}">
              </div>

              <div>
                <label>Cliente</label>
                <input type="text" name="cliente" value="{html.escape(filtro_cliente)}">
              </div>

            </div>

            <div class="row">
              <div><button type="submit">Filtrar</button></div>
              <div>
                <a href="{url_for('dashboard')}" class="btn-secondary" style="padding:10px 12px;display:inline-block;text-align:center;">
                    Limpar
                </a>
              </div>
            </div>

          </form>
        </div>

        <div class="grid grid-4">

          <div class="kpi">
            <div class="kpi-title">Total de clientes</div>
            <div class="kpi-value">{kpis["total_clientes"]}</div>
          </div>

          <div class="kpi">
            <div class="kpi-title">Representantes</div>
            <div class="kpi-value">{kpis["total_representantes"]}</div>
          </div>

          <div class="kpi">
            <div class="kpi-title">Supervisores</div>
            <div class="kpi-value">{kpis["total_supervisores"]}</div>
          </div>

          <div class="kpi">
            <div class="kpi-title">Valor total</div>
            <div class="kpi-value">R$ {kpis["valor_total"]:,.2f}</div>
          </div>

        </div>

        <div class="card">

          <h2>Clientes</h2>

          <p class="muted">Mostrando até 200 registros filtrados.</p>

          <div class="table-wrap">

            <table>

              <thead>
                <tr>
                  <th>Cód. Cliente</th>
                  <th>Cliente</th>
                  <th>Cód. Rep</th>
                  <th>Representante</th>
                  <th>Cód. Sup</th>
                  <th>Supervisor</th>
                  <th>Cidade</th>
                  <th>UF</th>
                  <th>Última compra</th>
                  <th>Valor</th>
                </tr>
              </thead>

              <tbody>
                {''.join(linhas_clientes) if linhas_clientes else '<tr><td colspan="10">Nenhum registro encontrado.</td></tr>'}
              </tbody>

            </table>

          </div>

        </div>
        """

        return render_page("Dashboard", content)

    except Exception:

        err = traceback.format_exc()

        content = f"""
        <div class="card">
          <h1>Dashboard</h1>
          <p>Erro ao carregar o dashboard.</p>
          <div class="error-box">{html.escape(err)}</div>
        </div>
        """

        return render_page("Falha no servidor", content), 500

@app.route("/carteira")
def carteira():
    if not require_login():
        return redirect(url_for("login"))

    try:
        data = parse_carteira_data()
        records = data["records"]

        filtro_rep = safe_str(request.args.get("rep"))
        filtro_sup = safe_str(request.args.get("sup"))
        filtro_cliente = safe_str(request.args.get("cliente"))
        filtro_situacao = safe_str(request.args.get("situacao"))

        filtrados = []
        for r in records:
            ok = True

            if filtro_rep and filtro_rep.lower() not in (r["representante"] + " " + r["cod_rep"]).lower():
                ok = False
            if filtro_sup and filtro_sup.lower() not in r["supervisor"].lower():
                ok = False
            if filtro_cliente and filtro_cliente.lower() not in (r["cliente"] + " " + r["cod_cliente"]).lower():
                ok = False
            if filtro_situacao and filtro_situacao.lower() not in r["situacao"].lower():
                ok = False

            if ok:
                filtrados.append(r)

        linhas = []
        for r in filtrados[:300]:
            linhas.append(f"""
            <tr>
              <td>{html.escape(r["cod_cliente"])}</td>
              <td>{html.escape(r["cliente"])}</td>
              <td>{html.escape(r["cod_rep"])}</td>
              <td>{html.escape(r["representante"])}</td>
              <td>{html.escape(r["supervisor"])}</td>
              <td>{html.escape(r["cidade"])}</td>
              <td>{html.escape(r["uf"])}</td>
              <td><span class="badge">{html.escape(r["situacao"])}</span></td>
            </tr>
            """)

        content = f"""
        <div class="card">
          <h1>Carteira</h1>
          <p class="muted">Total de registros: {len(records)}</p>

          <form method="get">
            <div class="row">
              <div>
                <label>Representante</label>
                <input type="text" name="rep" value="{html.escape(filtro_rep)}" placeholder="Nome ou código do representante">
              </div>
              <div>
                <label>Supervisor</label>
                <input type="text" name="sup" value="{html.escape(filtro_sup)}" placeholder="Supervisor">
              </div>
              <div>
                <label>Cliente</label>
                <input type="text" name="cliente" value="{html.escape(filtro_cliente)}" placeholder="Cliente">
              </div>
              <div>
                <label>Situação</label>
                <input type="text" name="situacao" value="{html.escape(filtro_situacao)}" placeholder="Situação / status">
              </div>
            </div>

            <div class="row">
              <div><button type="submit">Filtrar</button></div>
              <div><a href="{url_for('carteira')}" style="text-decoration:none;"><button type="button" class="btn-secondary">Limpar</button></a></div>
            </div>
          </form>
        </div>

        <div class="card">
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Cód. Cliente</th>
                  <th>Cliente</th>
                  <th>Cód. Rep</th>
                  <th>Representante</th>
                  <th>Supervisor</th>
                  <th>Cidade</th>
                  <th>UF</th>
                  <th>Situação</th>
                </tr>
              </thead>
              <tbody>
                {''.join(linhas) if linhas else '<tr><td colspan="8">Nenhum registro encontrado.</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>
        """
        return render_page("Carteira", content)

    except Exception:
        err = traceback.format_exc()
        content = f"""
        <div class="card">
          <h1>Carteira</h1>
          <p>Erro ao carregar a carteira.</p>
          <div class="error-box">{html.escape(err)}</div>
        </div>
        """
        return render_page("Falha no servidor", content), 500


@app.route("/admin-dashboard")
def admin_dashboard():
    if not require_login():
        return redirect(url_for("login"))

    try:
        content = f"""
        <div class="card">
          <h1>Admin</h1>
          <p class="muted">Painel administrativo do sistema.</p>

          <div class="grid grid-4">
            <div class="kpi">
              <div class="kpi-title">Usuário logado</div>
              <div class="kpi-value">{html.escape(current_user())}</div>
            </div>
            <div class="kpi">
              <div class="kpi-title">Aba BASE</div>
              <div class="kpi-value">{html.escape(WS_BASE)}</div>
            </div>
            <div class="kpi">
              <div class="kpi-title">Aba AGENDA</div>
              <div class="kpi-value">{html.escape(WS_AGENDA)}</div>
            </div>
            <div class="kpi">
              <div class="kpi-title">Aba CARTEIRA</div>
              <div class="kpi-value">{html.escape(WS_CARTEIRA)}</div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Ações</h2>
          <form method="post" action="{url_for('admin_refresh_cache')}">
            <div class="row">
              <div><button type="submit">Limpar cache do sistema</button></div>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Configurações</h2>
          <div class="table-wrap">
            <table>
              <tbody>
                <tr><th>SHEET_ID</th><td>{html.escape(SHEET_ID[:8] + '...' if SHEET_ID else '')}</td></tr>
                <tr><th>CACHE_TTL</th><td>{CACHE_TTL}s</td></tr>
                <tr><th>DEBUG_MODE</th><td>{DEBUG_MODE}</td></tr>
                <tr><th>Agora</th><td>{format_dt_br(now_br())}</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        """
        return render_page("Admin", content)

    except Exception:
        err = traceback.format_exc()
        content = f"""
        <div class="card">
          <h1>Admin</h1>
          <p>Erro ao carregar o painel administrativo.</p>
          <div class="error-box">{html.escape(err)}</div>
        </div>
        """
        return render_page("Falha no servidor", content), 500


@app.route("/admin/refresh-cache", methods=["POST"])
def admin_refresh_cache():
    if not require_login():
        return redirect(url_for("login"))

    clear_sheet_cache()
    flash("Cache limpo com sucesso.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/health")
def health():
    return {
        "status": "ok",
        "app": APP_TITLE,
        "time": now_br().isoformat(),
        "cache_items": len(_MEM_CACHE)
    }, 200


# =========================================================
# ERROR HANDLERS
# =========================================================
@app.errorhandler(429)
def too_many_requests(_e):
    content = """
    <div class="card">
      <h1>Muitas requisições</h1>
      <p>O sistema recebeu muitas chamadas em sequência. Aguarde alguns segundos e tente novamente.</p>
    </div>
    """
    return render_page("429", content), 429


@app.errorhandler(Exception)
def handle_global_exception(e):
    err = traceback.format_exc()

    # Identifica erro de quota da Google Sheets
    quota_msg = ""
    if "Quota exceeded" in err or "429" in err:
        quota_msg = """
        <div class="flash flash-error">
          A API do Google Sheets atingiu o limite temporário de leitura.
          O sistema possui retry e cache, mas no momento a quota foi excedida.
          Aguarde alguns segundos e atualize a página.
        </div>
        """

    content = f"""
    <div class="card">
      <h1>Falha no servidor</h1>
      {quota_msg}
      <p>Ocorreu um erro inesperado ao processar a requisição.</p>
      <div class="error-box">{html.escape(err if DEBUG_MODE else str(e))}</div>
    </div>
    """
    return render_page("Falha no servidor", content), 500


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=DEBUG_MODE)