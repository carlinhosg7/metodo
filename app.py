import os
import re
import json
from datetime import datetime

from flask import Flask, request, redirect, url_for, session, render_template_string, flash

import gspread
from google.oauth2.service_account import Credentials


# =========================
# CONFIG ENV
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-chave")

WS_BASE = os.getenv("WS_BASE", "BASE")
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES")
WS_LISTAS = os.getenv("WS_LISTAS", "LISTAS")


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


# =========================
# GOOGLE SHEETS CONNECTION
# =========================
def connect_gs():

    if not SHEET_ID:
        raise RuntimeError("Faltou SHEET_ID nas variáveis de ambiente.")

    if not SA_JSON:
        raise RuntimeError("Faltou GOOGLE_SERVICE_ACCOUNT_JSON nas variáveis de ambiente.")

    try:
        info = json.loads(SA_JSON)

        # Corrige quebra de linha do private_key
        if "private_key" in info:
            info["private_key"] = info["private_key"].replace("\\n", "\n")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        creds = Credentials.from_service_account_info(info, scopes=scopes)

        gc = gspread.authorize(creds)

        return gc.open_by_key(SHEET_ID)

    except Exception as e:
        raise RuntimeError(f"Erro ao conectar no Google Sheets: {e}")


def ensure_headers(ws, headers):
    row1 = ws.row_values(1)
    if [norm(x) for x in row1] != headers:
        ws.clear()
        ws.append_row(headers)


def get_all_records(ws):
    return ws.get_all_records()


def pick_col(headers, candidates):

    hmap = {norm(h).lower(): h for h in headers}

    for cand in candidates:
        k = norm(cand).lower()
        if k in hmap:
            return hmap[k]

    for h in headers:
        hl = norm(h).lower()
        for cand in candidates:
            if norm(cand).lower() in hl:
                return h

    return None


def is_admin():
    return session.get("user_type") == "admin"


def require_login():
    return "user_type" in session


def row_color_class(status_cor):
    s = norm(status_cor).lower()

    if "vermel" in s:
        return "row-red"

    if "verde" in s:
        return "row-green"

    if "amarel" in s:
        return "row-yellow"

    if "laranj" in s:
        return "row-orange"

    return ""


def unique_list(values):

    out = []
    seen = set()

    for v in values:

        v = norm(v)

        if not v:
            continue

        if v not in seen:
            seen.add(v)
            out.append(v)

    return out


# =========================
# TEMPLATES
# =========================
BASE_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<style>
body{font-family:Arial;margin:0;background:#0f172a;color:#e5e7eb}
.topbar{background:#111827;padding:12px 16px;display:flex;justify-content:space-between;border-bottom:1px solid #1f2937}
.container{padding:16px}
.card{background:#111827;border:1px solid #1f2937;border-radius:12px;padding:16px;margin-bottom:14px}
button{padding:8px 12px;border-radius:8px;border:0;background:#2563eb;color:white;cursor:pointer}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{border-bottom:1px solid #1f2937;padding:10px}
.row-red{background:rgba(220,38,38,0.20)}
.row-green{background:rgba(34,197,94,0.18)}
.row-yellow{background:rgba(234,179,8,0.18)}
.row-orange{background:rgba(249,115,22,0.18)}
</style>
</head>
<body>
<div class="topbar">
<b>Carteira Comercial</b>
{% if logged %}
<span>{{ user_login }} ({{ user_type }})</span>
<a href="{{ url_for('logout') }}"><button>Sair</button></a>
{% endif %}
</div>
<div class="container">
{{ body|safe }}
</div>
</body>
</html>
"""


LOGIN_BODY = """
<div class="card" style="max-width:400px;margin:auto;">
<h2>Login</h2>
<form method="post">
<label>Usuário</label>
<input name="user" required>

<label>Senha</label>
<input name="pass" type="password" required>

<br><br>
<button type="submit">Entrar</button>
</form>
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

            return redirect(url_for("dashboard"))

        if u and p and u == p and u.isdigit():

            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u

            return redirect(url_for("dashboard"))

    body = render_template_string(LOGIN_BODY)

    return render_template_string(
        BASE_HTML,
        title="Login",
        logged=False,
        body=body
    )


@app.route("/logout")
def logout():

    session.clear()

    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():

    if not require_login():
        return redirect(url_for("login"))

    sh = connect_gs()

    ws_base = sh.worksheet(WS_BASE)

    rows = ws_base.get_all_records()

    if not rows:
        return "BASE vazia"

    headers = ws_base.row_values(1)

    key_col = pick_col(headers, ["Codigo Grupo Cliente","Codigo Cliente","Cliente"])
    rep_col = pick_col(headers, ["Codigo Representante"])

    table = []

    for r in rows:

        ck = norm(r.get(key_col,""))
        rep = norm(r.get(rep_col,""))

        table.append(f"<tr><td>{ck}</td><td>{rep}</td></tr>")

    body = f"""
    <div class="card">
    <table>
    <tr>
    <th>{key_col}</th>
    <th>{rep_col}</th>
    </tr>
    {''.join(table)}
    </table>
    </div>
    """

    return render_template_string(
        BASE_HTML,
        title="Dashboard",
        logged=True,
        user_login=session.get("user_login"),
        user_type=session.get("user_type"),
        body=body
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000"))
    )