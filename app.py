# app.py
import os
import re
import json
from datetime import datetime

import pandas as pd
from flask import Flask, request, redirect, url_for, session, render_template_string, flash

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIG (ENV)
# ============================================================
# Coloque no Render (ou no .env local):
# SHEET_ID=1FAQ-cTeZlh4mZXw-Ya0ipX9JfP92j-9B_4QpcPb3Me8
# GOOGLE_SERVICE_ACCOUNT_JSON={...conteúdo do json...}
#
# Admin:
# ADMIN_USER=admin
# ADMIN_PASS=admin123

SHEET_ID = os.getenv("SHEET_ID", "").strip()
SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")

SECRET_KEY = os.getenv("SECRET_KEY", "troque-esta-secret-bem-grande")

WS_BASE = os.getenv("WS_BASE", "BASE")
WS_EDICOES = os.getenv("WS_EDICOES", "EDICOES")
WS_LISTAS = os.getenv("WS_LISTAS", "LISTAS")

# ============================================================
# APP
# ============================================================
app = Flask(__name__)
app.secret_key = SECRET_KEY

# ============================================================
# HELPERS
# ============================================================
def norm(s):
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def connect_gs():
    if not SHEET_ID:
        raise RuntimeError("Faltou definir SHEET_ID nas variáveis de ambiente.")
    if not SA_JSON:
        raise RuntimeError("Faltou definir GOOGLE_SERVICE_ACCOUNT_JSON nas variáveis de ambiente.")

    info = json.loads(SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh

def ws_get_df(ws):
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df.columns = [norm(c) for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).replace({"nan": "", "None": ""}).fillna("")
    return df

def ensure_headers(ws, headers):
    # garante que a linha 1 tem os cabeçalhos esperados
    row1 = ws.row_values(1)
    if [norm(x) for x in row1] != headers:
        ws.clear()
        ws.append_row(headers)

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

def pick_col(df, candidates):
    cols = {norm(c).lower(): c for c in df.columns}
    for cand in candidates:
        k = norm(cand).lower()
        if k in cols:
            return cols[k]
    for c in df.columns:
        cl = norm(c).lower()
        for cand in candidates:
            if norm(cand).lower() in cl:
                return c
    return None

# ============================================================
# TEMPLATES
# ============================================================
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
      </div>
      <div>
        <label>Senha</label>
        <input name="pass" type="password" placeholder="admin123 ou o mesmo código" required>
      </div>
    </div>
    <div style="margin-top:12px;">
      <button type="submit">Entrar</button>
    </div>
    <p class="small" style="margin-top:12px;">
      Representante: usuário = código / senha = código.<br>
      Admin: usuário = {{ admin_user }} / senha = definida em ADMIN_PASS.
    </p>
  </form>
</div>
"""

# ============================================================
# ROUTES
# ============================================================
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

        # representante: user=senha=código
        if u and p and u == p and u.isdigit():
            session["user_type"] = "rep"
            session["user_login"] = u
            session["rep_code"] = u
            flash(f"Logado como Representante {u}.", "ok")
            return redirect(url_for("dashboard"))

        flash("Login inválido.", "err")

    body = render_template_string(LOGIN_BODY, admin_user=ADMIN_USER)
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
    ws_base = sh.worksheet(WS_BASE)
    ws_ed = sh.worksheet(WS_EDICOES)
    ws_listas = sh.worksheet(WS_LISTAS)

    # listas dropdown
    df_list = ws_get_df(ws_listas)
    meses = df_list.get("Mês", pd.Series([], dtype=str)).replace({"nan": ""}).tolist() if not df_list.empty else []
    semanas = df_list.get("Semana Atendimento", pd.Series([], dtype=str)).replace({"nan": ""}).tolist() if not df_list.empty else []
    status_list = df_list.get("Status Cliente", pd.Series([], dtype=str)).replace({"nan": ""}).tolist() if not df_list.empty else []
    meses = [m for m in meses if norm(m)]
    semanas = [s for s in semanas if norm(s)]
    status_list = [s for s in status_list if norm(s)]

    # base
    df = ws_get_df(ws_base)
    if df.empty:
        flash("A aba BASE está vazia.", "err")
        return render_template_string(BASE_HTML, title="Dashboard", subtitle="Base vazia", logged=True,
                                      user_login=session.get("user_login"), user_type=session.get("user_type"),
                                      body="<div class='card'>Sem dados na BASE.</div>")

    # colunas essenciais
    key_col = pick_col(df, ["Codigo Grupo Cliente","Código Grupo Cliente","Codigo Cliente","Código Cliente","COD_CLIENTE","Cliente"])
    rep_col = pick_col(df, ["Codigo Representante","Código Representante","CODIGO REPRESENTANTE","COD_REP"])
    sup_col = pick_col(df, ["Supervisor","Código Supervisor","Codigo Supervisor","COD_SUP"])
    cor_col = pick_col(df, ["STATUS COR","Status Cor","COR","Status COR"])

    if not key_col or not rep_col:
        flash("BASE precisa ter uma coluna chave (cliente/grupo) e Codigo Representante.", "err")
        return redirect(url_for("logout"))

    # aplica filtro por permissão
    if not is_admin():
        rep_code = session.get("rep_code")
        df = df[df[rep_col].astype(str) == str(rep_code)]

    # filtros admin
    sup_sel = norm(request.args.get("sup", ""))
    rep_sel = norm(request.args.get("rep", ""))
    q = norm(request.args.get("q", ""))

    if is_admin() and sup_col and sup_sel:
        df = df[df[sup_col].astype(str) == sup_sel]
    if is_admin() and rep_sel:
        df = df[df[rep_col].astype(str) == rep_sel]

    if q:
        # busca em colunas comuns
        candidates = [key_col, rep_col]
        for c in ["Grupo Cliente", "Cidade", "Cliente"]:
            cc = pick_col(df, [c])
            if cc:
                candidates.append(cc)
        mask = False
        for c in list(dict.fromkeys(candidates)):
            mask = mask | df[c].astype(str).str.contains(q, case=False, na=False)
        df = df[mask]

    # carrega edições e aplica por cima (última vence)
    ed_headers = ["timestamp","user_type","user_login","rep_code","client_key",
                  "Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    ensure_headers(ws_ed, ed_headers)
    df_ed = ws_get_df(ws_ed)

    latest = {}
    if not df_ed.empty and "client_key" in df_ed.columns:
        for _, r in df_ed.iterrows():
            ck = norm(r.get("client_key",""))
            if ck:
                latest[ck] = {
                    "Data Agenda Visita": norm(r.get("Data Agenda Visita","")),
                    "Mês": norm(r.get("Mês","")),
                    "Semana Atendimento": norm(r.get("Semana Atendimento","")),
                    "Status Cliente": norm(r.get("Status Cliente","")),
                }

    def apply_edit(row):
        ck = norm(row.get(key_col,""))
        if ck in latest:
            for k,v in latest[ck].items():
                row[k] = v
        return row

    # garante colunas editáveis
    for c in ["Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]:
        if c not in df.columns:
            df[c] = ""

    df = df.apply(apply_edit, axis=1)

    # listas de filtro admin
    sup_list = sorted(df[sup_col].dropna().astype(str).unique().tolist()) if (is_admin() and sup_col) else []
    rep_list = sorted(df[rep_col].dropna().astype(str).unique().tolist()) if is_admin() else []

    # paginação simples
    page_size = 200
    rows = df.head(page_size).to_dict(orient="records")

    # monta tabela HTML
    cols_show = [
        key_col, rep_col
    ]
    if sup_col:
        cols_show.append(sup_col)
    for c in ["Grupo Cliente","Cidade"]:
        cc = pick_col(df, [c])
        if cc and cc not in cols_show:
            cols_show.append(cc)

    cols_show += ["Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    if cor_col and cor_col not in cols_show:
        cols_show.append(cor_col)

    def opt_html(options, selected):
        out = ["<option value=''></option>"]
        for o in options:
            sel = "selected" if norm(o) == norm(selected) else ""
            out.append(f"<option value='{o}' {sel}>{o}</option>")
        return "\n".join(out)

    table_rows = []
    for r in rows:
        ck = norm(r.get(key_col,""))
        repc = norm(r.get(rep_col,""))
        corv = norm(r.get(cor_col,"")) if cor_col else ""
        klass = row_color_class(corv)

        dav = norm(r.get("Data Agenda Visita",""))
        mes = norm(r.get("Mês",""))
        sem = norm(r.get("Semana Atendimento",""))
        stc = norm(r.get("Status Cliente",""))

        # inputs editáveis
        row_html = f"""
        <tr class="{klass}">
          <td>{ck}</td>
          <td>{repc}</td>
        """
        if sup_col:
            row_html += f"<td>{norm(r.get(sup_col,''))}</td>"

        gc = pick_col(df, ["Grupo Cliente"])
        cd = pick_col(df, ["Cidade"])
        if gc:
            row_html += f"<td>{norm(r.get(gc,''))}</td>"
        if cd:
            row_html += f"<td>{norm(r.get(cd,''))}</td>"

        row_html += f"""
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
              <select name="Semana Atendimento" style="min-width:140px;">
                {opt_html(semanas, sem)}
              </select>
          </td>
          <td>
              <select name="Status Cliente" style="min-width:160px;">
                {opt_html(status_list, stc)}
              </select>
              <button type="submit">Gravar</button>
            </form>
          </td>
        """
        if cor_col:
            row_html += f"<td>{corv}</td>"
        row_html += "</tr>"
        table_rows.append(row_html)

    body = f"""
    <div class="card">
      <form method="get">
        <div class="grid">
          {"".join([
            f"""
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
            """ if is_admin() else ""
          ])}
          <div>
            <label>Buscar</label>
            <input name="q" value="{q}" placeholder="cliente/grupo/cidade...">
          </div>
          <div style="display:flex;align-items:flex-end;gap:8px;">
            <button type="submit">Aplicar</button>
            <a href="{url_for('dashboard')}"><button type="button" class="secondary">Limpar</button></a>
            {"<a href='"+url_for("auditoria")+"'><button type='button' class='secondary'>Auditoria</button></a>" if is_admin() else ""}
          </div>
        </div>
      </form>
    </div>

    <div class="card" style="overflow:auto; max-height:72vh;">
      <div style="margin-bottom:10px;">
        <b>Total exibido:</b> {len(df)} <span class="small">(mostrando até {page_size})</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>{key_col}</th>
            <th>{rep_col}</th>
            {f"<th>{sup_col}</th>" if sup_col else ""}
            {f"<th>Grupo Cliente</th>" if pick_col(df, ['Grupo Cliente']) else ""}
            {f"<th>Cidade</th>" if pick_col(df, ['Cidade']) else ""}
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
        subtitle="Carteira",
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

    # representante só pode gravar no que for dele
    if user_type == "rep":
        if rep_code_form != session.get("rep_code"):
            flash("Você não pode gravar alterações em clientes de outro representante.", "err")
            return redirect(url_for("dashboard"))

    payload = {
        "Data Agenda Visita": norm(request.form.get("Data Agenda Visita", "")),
        "Mês": norm(request.form.get("Mês", "")),
        "Semana Atendimento": norm(request.form.get("Semana Atendimento", "")),
        "Status Cliente": norm(request.form.get("Status Cliente", "")),
    }

    sh = connect_gs()
    ws_ed = sh.worksheet(WS_EDICOES)

    headers = ["timestamp","user_type","user_login","rep_code","client_key",
               "Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    ensure_headers(ws_ed, headers)

    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        user_type,
        user_login,
        rep_code_form,
        client_key,
        payload["Data Agenda Visita"],
        payload["Mês"],
        payload["Semana Atendimento"],
        payload["Status Cliente"]
    ]
    ws_ed.append_row(row)

    flash("Alteração gravada com sucesso.", "ok")
    return redirect(url_for("dashboard"))

@app.route("/auditoria", methods=["GET"])
def auditoria():
    if not require_login() or not is_admin():
        return redirect(url_for("dashboard"))

    sh = connect_gs()
    ws_ed = sh.worksheet(WS_EDICOES)
    df_ed = ws_get_df(ws_ed)

    if df_ed.empty:
        body = "<div class='card'>Sem alterações registradas ainda.</div>"
        return render_template_string(BASE_HTML, title="Auditoria", subtitle="Admin", logged=True,
                                      user_login=session.get("user_login"), user_type=session.get("user_type"),
                                      body=body)

    # mostra últimas 500
    df_ed = df_ed.tail(500)

    cols = ["timestamp","user_type","user_login","rep_code","client_key",
            "Data Agenda Visita","Mês","Semana Atendimento","Status Cliente"]
    for c in cols:
        if c not in df_ed.columns:
            df_ed[c] = ""

    rows = df_ed[cols].to_dict(orient="records")
    tr = []
    for r in rows:
        tr.append("<tr>" + "".join([f"<td>{norm(r.get(c,''))}</td>" for c in cols]) + "</tr>")

    body = f"""
    <div class="card">
      <b>Auditoria (últimas 500 alterações)</b>
      <div class="small">Tudo fica gravado na aba EDICOES.</div>
    </div>
    <div class="card" style="overflow:auto; max-height:72vh;">
      <table>
        <thead>
          <tr>{"".join([f"<th>{c}</th>" for c in cols])}</tr>
        </thead>
        <tbody>
          {"".join(tr)}
        </tbody>
      </table>
    </div>
    """
    return render_template_string(BASE_HTML, title="Auditoria", subtitle="Admin", logged=True,
                                  user_login=session.get("user_login"), user_type=session.get("user_type"),
                                  body=body)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)