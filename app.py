# app.py
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

# Aceita 2 jeitos:
# 1) JSON direto (recomendado se você conseguir colar sem quebrar)
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
# 2) Base64 do JSON (mais blindado)
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-chave").strip()

WS_BASE = os.getenv("WS_BASE", "BASE").strip()
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES").strip()
WS_LISTAS = os.getenv("WS_LISTAS", "LISTAS").strip()

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))


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

def row_color_class(status_cor):
    s = norm(status_cor).lower()
    if "vermel" in s or "red" in s:
        return "row-red"
    if "verde" in s or "green" in s:
        return "row-green"
    if "amarel" in s or "yellow" in s:
        return "row-yellow"
    if "laranj" in s or "orange" in s:
        return "row-orange"
    return ""

def pick_col(headers, candidates):
    # match case-insensitive exato
    hmap = {norm(h).lower(): h for h in headers}
    for cand in candidates:
        k = norm(cand).lower()
        if k in hmap:
            return hmap[k]
    # contains fallback
    for h in headers:
        hl = norm(h).lower()
        for cand in candidates:
            if norm(cand).lower() in hl:
                return h
    return None


# =========================
# GOOGLE SHEETS
# =========================
def _load_service_account_info():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    elif GOOGLE_SA_JSON_B64:
        b64 = GOOGLE_SA_JSON_B64.strip()
        # padding base64 (evita erro "not a multiple of 4")
        b64 += "=" * (-len(b64) % 4)
        info = json.loads(base64.b64decode(b64).decode("utf-8"))
    else:
        raise RuntimeError("Faltou GOOGLE_SERVICE_ACCOUNT_JSON (ou GOOGLE_SA_JSON_B64) nas variáveis de ambiente.")

    # Corrige quebra de linha do private_key quando vem com \\n
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

    # Se pediram headers, garante pelo menos a primeira linha
    if headers:
        row1 = [norm(x) for x in ws.row_values(1)]
        if row1 != headers:
            ws.clear()
            ws.append_row(headers)
    return ws

def safe_get_all_records(ws):
    # Se a planilha estiver vazia ou sem header, não quebra
    try:
        return ws.get_all_records()
    except Exception:
        return []


# =========================
# ERROR HANDLER (pra não ficar 500 mudo)
# =========================
@app.errorhandler(Exception)
def handle_any_exception(e):
    app.logger.error("ERRO NÃO TRATADO:\n%s", traceback.format_exc())
    # Mostra mensagem minimamente útil pro usuário (sem expor chave)
    msg = norm(str(e)) or "Erro interno."
    body = f"<div class='card'><b>Erro:</b><br><pre style='white-space:pre-wrap'>{msg}</pre></div>"
    return render_template_string(
        BASE_HTML,
        title="Erro",
        subtitle="Falha no servidor",
        logged=require_login(),
        user_login=session.get("user_login", ""),
        user_type=session.get("user_type", ""),
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
    body { font-family: Arial, sans-serif; margin:0; background:#0f172a; color:#e5e7eb; }
    .topbar{background:#111827;padding:12px 16px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #1f2937;}
    .container{padding:16px;}
    .card{background:#111827;border:1px solid #1f2937;border-radius:12px;padding:16px;margin-bottom:14px;}
    label{font-size:12px;color:#9ca3af;display:block;margin-bottom:4px;}
    input,select{width:100%;padding:10px;border-radius:10px;border:1px solid #374151;background:#0b1220;color:#e5e7eb;}
    button{padding:10px 14px;border-radius:10px;border:0;background:#2563eb;color:#fff;cursor:pointer;}
    button.secondary{background:#374151;}
    button.danger{background:#dc2626;}
    table{width:100%;border-collapse:collapse;font-size:13px;}
    th,td{border-bottom:1px solid #1f2937;padding:10px;vertical-align:top;}
    th{position:sticky;top:0;background:#0b1220;color:#9ca3af;text-align:left;}
    .grid{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:10px;}
    .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
    .msg{padding:10px 12px;border-radius:10px;margin-bottom:10px;}
    .ok{background:#052e16;border:1px solid #14532d;}
    .err{background:#3f1d1d;border:1px solid #7f1d1d;}
    .pill{padding:3px 8px;border-radius:999px;font-size:12px;background:#0b1220;border:1px solid #1f2937;display:inline-block;}
    .small{color:#9ca3af;font-size:12px;}
    .row-red{background:rgba(220,38,38,0.20);}
    .row-green{background:rgba(34,197,94,0.18);}
    .row-yellow{background:rgba(234,179,8,0.18);}
    .row-orange{background:rgba(249,115,22,0.18);}
    .hint{color:#9ca3af;font-size:12px;margin-top:6px;}
    .nowrap{white-space:nowrap;}
  </style>
</head>
<body>
  <div class="topbar">
    <div><b>Carteira Comercial</b> <span class="small">| {{ subtitle }}</span></div>
    <div>
      {% if logged %}
        <span class="pill">{{ user_login }} ({{ user_type }})</span>
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
<div class="card" style="max-width:520px;margin:auto;">
  <h2 style="margin-top:0;">Login</h2>
  <form method="post">
    <div class="grid-2">
      <div>
        <label>Usuário</label>
        <input name="user" placeholder="admin ou código do representante" required>
        <div class="hint">Admin: use ADMIN_USER/ADMIN_PASS. Rep: use o próprio código como senha.</div>
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
            flash("Logado como ADMIN.", "ok")
            return redirect(url_for("dashboard"))

        if u and p and u == p and u.isdigit():
            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u
            flash(f"Logado como Representante {u}.", "ok")
            return redirect(url_for("dashboard"))

        flash("Login inválido.", "err")

    body = render_template_string(LOGIN_BODY)
    return render_template_string(BASE_HTML, title="Login", subtitle="Acesso", logged=False, body=body)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not require_login():
        return redirect(url_for("login"))

    sh = connect_gs()

    # BASE é obrigatória
    try:
        ws_base = sh.worksheet(WS_BASE)
    except WorksheetNotFound:
        flash(f"Não achei a aba BASE. Configure WS_BASE no Render (hoje: '{WS_BASE}').", "err")
        body = "<div class='card'>Aba BASE não encontrada.</div>"
        return render_template_string(
            BASE_HTML, title="Dashboard", subtitle="Erro", logged=True,
            user_login=session.get("user_login"), user_type=session.get("user_type"),
            body=body
        )

    # EDICOES e LISTAS: se não existirem, cria automaticamente
    ed_headers = ["timestamp","user_type","user_login","rep_code","client_key",
                  "Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    ws_ed = get_or_create_worksheet(sh, WS_EDICOES, rows=2000, cols=20, headers=ed_headers)

    listas_headers = ["Mês","Semana Atendimento","Status Cliente"]
    ws_listas = get_or_create_worksheet(sh, WS_LISTAS, rows=500, cols=10, headers=listas_headers)

    base_rows = safe_get_all_records(ws_base)
    if not base_rows:
        flash(f"A aba {WS_BASE} está vazia.", "err")
        return render_template_string(
            BASE_HTML, title="Dashboard", subtitle="Base vazia",
            logged=True, user_login=session.get("user_login"), user_type=session.get("user_type"),
            body="<div class='card'>Sem dados na BASE.</div>"
        )

    headers = [norm(h) for h in ws_base.row_values(1)]

    # Colunas (flexível)
    key_col = pick_col(headers, [
        "Codigo Grupo Cliente","Código Grupo Cliente",
        "Codigo Cliente","Código Cliente","COD_CLIENTE","Cliente"
    ])
    grupo_col = pick_col(headers, ["Grupo Cliente","Grupo","Nome Grupo","Razao Social","Nome Cliente"])
    cidade_col = pick_col(headers, ["Cidade","Município","Municipio"])
    rep_col = pick_col(headers, ["Codigo Representante","Código Representante","CODIGO REPRESENTANTE","COD_REP"])
    sup_col = pick_col(headers, ["Supervisor","Código Supervisor","Codigo Supervisor","COD_SUP"])
    cor_col = pick_col(headers, ["STATUS COR","Status Cor","COR","Status COR"])

    if not key_col or not rep_col:
        flash("BASE precisa ter uma coluna chave (cliente/grupo) e Código Representante.", "err")
        return redirect(url_for("logout"))

    # Listas dropdown (LISTAS)
    lista_rows = safe_get_all_records(ws_listas)
    meses = unique_list([r.get("Mês","") for r in lista_rows])
    semanas = unique_list([r.get("Semana Atendimento","") for r in lista_rows])
    status_list = unique_list([r.get("Status Cliente","") for r in lista_rows])

    # Carrega edições (EDICOES) e pega a última por client_key
    ed_rows = safe_get_all_records(ws_ed)
    latest = {}
    for r in ed_rows:
        ck = norm(r.get("client_key",""))
        if ck:
            latest[ck] = {
                "Data Agenda Visita": norm(r.get("Data Agenda Visita","")),
                "Mês": norm(r.get("Mês","")),
                "Semana Atendimento": norm(r.get("Semana Atendimento","")),
                "Status Cliente": norm(r.get("Status Cliente","")),
            }

    # filtros
    sup_sel = norm(request.args.get("sup", ""))
    rep_sel = norm(request.args.get("rep", ""))
    q = norm(request.args.get("q", ""))

    # listas para filtro admin
    sup_list = unique_list([r.get(sup_col,"") for r in base_rows]) if (is_admin() and sup_col) else []
    rep_list = unique_list([r.get(rep_col,"") for r in base_rows]) if is_admin() else []

    # aplica filtros + segurança rep
    out_rows = []
    for r in base_rows:
        ck = norm(r.get(key_col, ""))
        repc = norm(r.get(rep_col, ""))

        if not is_admin():
            if repc != session.get("rep_code"):
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

        # aplica última edição
        if ck in latest:
            r["Data Agenda Visita"] = latest[ck]["Data Agenda Visita"]
            r["Mês"] = latest[ck]["Mês"]
            r["Semana Atendimento"] = latest[ck]["Semana Atendimento"]
            r["Status Cliente"] = latest[ck]["Status Cliente"]
        else:
            r.setdefault("Data Agenda Visita","")
            r.setdefault("Mês","")
            r.setdefault("Semana Atendimento","")
            r.setdefault("Status Cliente","")

        out_rows.append(r)

    # paginação simples
    out_rows = out_rows[:PAGE_SIZE]

    def opt_html(options, selected):
        out = ["<option value=''></option>"]
        for o in options:
            sel = "selected" if norm(o) == norm(selected) else ""
            out.append(f"<option value='{o}' {sel}>{o}</option>")
        return "\n".join(out)

    # monta tabela
    table_rows = []
    for r in out_rows:
        ck = norm(r.get(key_col,""))
        repc = norm(r.get(rep_col,""))
        supv = norm(r.get(sup_col,"")) if sup_col else ""
        corv = norm(r.get(cor_col,"")) if cor_col else ""
        klass = row_color_class(corv)

        grupo = norm(r.get(grupo_col,"")) if grupo_col else ""
        cidade = norm(r.get(cidade_col,"")) if cidade_col else ""

        dav = norm(r.get("Data Agenda Visita",""))
        mes = norm(r.get("Mês",""))
        sem = norm(r.get("Semana Atendimento",""))
        stc = norm(r.get("Status Cliente",""))

        row_html = f"""
        <tr class="{klass}">
          <td class="nowrap">{ck}</td>
          <td class="nowrap">{repc}</td>
          {f"<td class='nowrap'>{supv}</td>" if sup_col else ""}
          {f"<td>{grupo}</td>" if grupo_col else ""}
          {f"<td>{cidade}</td>" if cidade_col else ""}
          <td>
            <form method="post" action="{url_for('salvar')}" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
              <input type="hidden" name="client_key" value="{ck}">
              <input type="hidden" name="rep_code" value="{repc}">
              <input type="date" name="Data Agenda Visita" value="{dav}" style="min-width:155px;">
          </td>
          <td>
              <select name="Mês" style="min-width:120px;">
                {opt_html(meses, mes)}
              </select>
          </td>
          <td>
              <select name="Semana Atendimento" style="min-width:160px;">
                {opt_html(semanas, sem)}
              </select>
          </td>
          <td>
              <select name="Status Cliente" style="min-width:180px;">
                {opt_html(status_list, stc)}
              </select>
              <button type="submit">Gravar</button>
            </form>
          </td>
          {f"<td>{corv}</td>" if cor_col else ""}
        </tr>
        """
        table_rows.append(row_html)

    # filtros UI
    filtros_html = ""
    if is_admin():
        filtros_html = f"""
        <div>
          <label>Filtro Supervisor</label>
          <select name="sup">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{s}' {'selected' if s==sup_sel else ''}>{s}</option>" for s in sup_list])}
          </select>
        </div>
        <div>
          <label>Filtro Representante</label>
          <select name="rep">
            <option value="">(Todos)</option>
            {''.join([f"<option value='{r}' {'selected' if r==rep_sel else ''}>{r}</option>" for r in rep_list])}
          </select>
        </div>
        """

    body = f"""
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
        <div class="hint">Mostrando até {PAGE_SIZE} linhas. (Você pode aumentar PAGE_SIZE no Render.)</div>
      </form>
    </div>

    <div class="card" style="overflow:auto; max-height:72vh;">
      <div style="margin-bottom:10px;">
        <b>Total exibido:</b> {len(out_rows)}
      </div>
      <table>
        <thead>
          <tr>
            <th>{key_col}</th>
            <th>{rep_col}</th>
            {f"<th>{sup_col}</th>" if sup_col else ""}
            {f"<th>{grupo_col}</th>" if grupo_col else ""}
            {f"<th>{cidade_col}</th>" if cidade_col else ""}
            <th>Data Agenda Visita</th>
            <th>Mês</th>
            <th>Semana Atendimento</th>
            <th>Status Cliente</th>
            {f"<th>{cor_col}</th>" if cor_col else ""}
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
        title="Dashboard",
        subtitle=f"Planilha: {WS_BASE}",
        logged=True,
        user_login=session.get("user_login"),
        user_type=session.get("user_type"),
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

    ed_headers = ["timestamp","user_type","user_login","rep_code","client_key",
                  "Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    ws_ed = get_or_create_worksheet(sh, WS_EDICOES, rows=2000, cols=20, headers=ed_headers)

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