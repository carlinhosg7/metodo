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

        # listas da aba de validação
        meses = DEFAULT_MESES[:]
        semanas = DEFAULT_SEMANAS[:]
        status_list = DEFAULT_STATUS[:]

        try:
            ws_listas = sh.worksheet(WS_LISTAS)
            lista_rows = safe_get_all_records(ws_listas)
            meses = unique_list([r.get("Mês", "") for r in lista_rows]) or DEFAULT_MESES
            semanas = unique_list([r.get("Semana Atendimento", "") for r in lista_rows]) or DEFAULT_SEMANAS
            status_list = unique_list([r.get("Status Cliente", "") for r in lista_rows]) or DEFAULT_STATUS
        except Exception:
            pass

        sup_sel = norm(request.args.get("sup", ""))
        rep_sel = norm(request.args.get("rep", ""))

        sup_list = unique_list([r.get(sup_col, "") for r in base_rows]) if sup_col else []
        rep_list = unique_list([r.get(rep_col, "") for r in base_rows]) if rep_col else []

        filtered_rows = []
        for idx_base, r in enumerate(base_rows, start=2):
            if sup_sel and sup_col and norm(r.get(sup_col, "")) != sup_sel:
                continue
            if rep_sel and rep_col and norm(r.get(rep_col, "")) != rep_sel:
                continue

            row_copy = dict(r)
            row_copy["_base_row_number"] = idx_base

            status_cor_final, row_class, _ = resolve_status_cor_from_base(
                row_copy,
                status_cor_col=status_cor_col,
                cliente_novo_col=cliente_novo_col
            )
            row_copy["_status_cor"] = status_cor_final
            row_copy["_row_class"] = row_class

            filtered_rows.append(row_copy)

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

        # =========================
        # TOP 10 2026
        # =========================
        ranking_2026 = []
        if grupo_col and t2026_col:
            for r in filtered_rows:
                nome = norm(r.get(grupo_col, ""))
                valor = parse_number_br(r.get(t2026_col, ""))
                if nome and valor > 0:
                    ranking_2026.append({
                        "grupo": nome,
                        "valor": valor,
                        "status_cor": r.get("_status_cor", ""),
                        "row_class": r.get("_row_class", "")
                    })
            ranking_2026.sort(key=lambda x: x["valor"], reverse=True)
            ranking_2026 = ranking_2026[:10]

        # =========================
        # TOP 10 2025
        # =========================
        ranking_2025 = []
        if grupo_col and t2025_col:
            for r in filtered_rows:
                nome = norm(r.get(grupo_col, ""))
                valor = parse_number_br(r.get(t2025_col, ""))
                if nome and valor > 0:
                    ranking_2025.append({
                        "grupo": nome,
                        "valor": valor,
                        "status_cor": r.get("_status_cor", ""),
                        "row_class": r.get("_row_class", "")
                    })
            ranking_2025.sort(key=lambda x: x["valor"], reverse=True)
            ranking_2025 = ranking_2025[:10]

        # =========================
        # CLIENTES SEM COMPRA
        # =========================
        clientes_sem_compra = []
        if key_col and grupo_col and t2026_col:
            for r in filtered_rows:
                v2026 = parse_number_br(r.get(t2026_col, ""))
                if v2026 == 0:
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
                        "row_class": r.get("_row_class", "")
                    })

            clientes_sem_compra.sort(
                key=lambda x: (x["t2025"], x["t2024"], x["grupo"]),
                reverse=True
            )

        # =========================
        # AGENDA EDITÁVEL
        # =========================
        agenda_rows = []
        for r in filtered_rows:
            agenda_rows.append({
                "codigo": norm(r.get(key_col, "")) if key_col else "",
                "grupo": norm(r.get(grupo_col, "")) if grupo_col else "",
                "cidade": norm(r.get(cidade_col, "")) if cidade_col else "",
                "representante": norm(r.get(nome_rep_col, "")) if nome_rep_col else "",
                "data": norm(r.get(data_agenda_col, "")) if data_agenda_col else "",
                "mes": norm(r.get(mes_col, "")) if mes_col else "",
                "semana": norm(r.get(semana_col, "")) if semana_col else "",
                "status": norm(r.get(status_cliente_col, "")) if status_cliente_col else "",
                "obs": norm(r.get(observacoes_col, "")) if observacoes_col else "",
                "rep_code": norm(r.get(rep_col, "")) if rep_col else "",
                "base_row_number": r.get("_base_row_number", ""),
                "row_class": r.get("_row_class", "")
            })

        def agenda_sort_key(x):
            data_txt = x.get("data", "")
            m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", data_txt)
            if m:
                dd, mm, yyyy = m.groups()
                data_ord = f"{yyyy}{mm}{dd}"
            else:
                data_ord = "99999999"
            return (
                data_ord,
                x.get("mes", ""),
                x.get("semana", ""),
                x.get("grupo", "")
            )

        agenda_rows.sort(key=agenda_sort_key)

        total_gold = 0
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

        # =========================
        # HTML TOP 10 2026
        # =========================
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

        # =========================
        # HTML TOP 10 2025
        # =========================
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

        # =========================
        # HTML CLIENTES SEM COMPRA
        # =========================
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

        # =========================
        # HTML AGENDA EDITÁVEL
        # =========================
        def opt_html(options, selected):
            out = ["<option value=''></option>"]
            for o in options:
                sel = "selected" if norm(o) == norm(selected) else ""
                out.append(f"<option value='{h(o)}' {sel}>{h(o)}</option>")
            return "\n".join(out)

        agenda_table_rows = []
        for idx, item in enumerate(agenda_rows[:12], start=1):
            form_id = f"agenda_form_{idx}"

            hidden_filters = ""
            if sup_sel:
                hidden_filters += f'<input type="hidden" name="sup" value="{h(sup_sel)}">'
            if rep_sel:
                hidden_filters += f'<input type="hidden" name="rep" value="{h(rep_sel)}">'

            agenda_table_rows.append(f"""
            <tr class="{h(item['row_class'])}">
              <td>{h(item['codigo'])}</td>
              <td>{h(item['grupo'])}</td>
              <td>{h(item['cidade'])}</td>

              <td>
                <form id="{form_id}" method="post" action="{url_for('salvar')}">
                  <input type="hidden" name="client_key" value="{h(item['codigo'])}">
                  <input type="hidden" name="rep_code" value="{h(item['rep_code'])}">
                  <input type="hidden" name="base_row_number" value="{h(item['base_row_number'])}">
                  {hidden_filters}
                </form>
                <input type="date"
                       name="Data Agenda Visita"
                       value="{h(to_input_date(item['data']))}"
                       form="{form_id}"
                       style="min-width:125px;">
              </td>

              <td>
                <select name="Mês" form="{form_id}" style="min-width:100px;">
                  {opt_html(meses, item['mes'])}
                </select>
              </td>

              <td>
                <select name="Semana Atendimento" form="{form_id}" style="min-width:120px;">
                  {opt_html(semanas, item['semana'])}
                </select>
              </td>

              <td>
                <select name="Status Cliente" form="{form_id}" style="min-width:160px;">
                  {opt_html(status_list, item['status'])}
                </select>
              </td>

              <td>
                <div style="display:flex; gap:6px; align-items:center;">
                  <input type="text"
                         name="Observações"
                         value="{h(item['obs'])}"
                         form="{form_id}"
                         placeholder="Observações..."
                         style="min-width:160px;">
                  <button type="submit" form="{form_id}" style="white-space:nowrap;">Gravar</button>
                </div>
              </td>
            </tr>
            """)

        agenda_html = f"""
        <div style="max-height:245px; overflow:auto;">
          <table class="dash-table-big">
            <thead>
              <tr>
                <th>Código</th>
                <th>Grupo</th>
                <th>Cidade</th>
                <th>Data</th>
                <th>Mês</th>
                <th>Semana</th>
                <th>Status</th>
                <th>Observações</th>
              </tr>
            </thead>
            <tbody>
              {''.join(agenda_table_rows) if agenda_table_rows else '<tr><td colspan="8" style="text-align:center;">Nenhum registro encontrado.</td></tr>'}
            </tbody>
          </table>
        </div>
        """

        # =========================
        # MAPA
        # =========================
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
            erro_txt = norm(str(e))
            if "This operation is not supported for this document" in erro_txt:
                erro_txt = (
                    "O arquivo informado em MUNICIPIOS_SHEET_ID não é uma planilha Google Sheets válida. "
                    "Converta o arquivo para Google Sheets e use o ID da planilha convertida."
                )

            mapa_svg_html = f"""
            <div class="dash-map-placeholder">
              Erro ao montar mapa.<br><br>
              {h(erro_txt)}
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
                      <div class="dash-gold-box">
                        Total Clientes Gold: <b style="margin-left:6px;">{h(total_gold)}</b>
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

                  <div class="dash-panel">
                    <div class="dash-panel-title">Agenda</div>
                    <div class="dash-panel-body">
                      {agenda_html}
                    </div>
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
              <div class="line"><b>AGENDA ROWS:</b> {h(len(agenda_rows))}</div>
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
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)