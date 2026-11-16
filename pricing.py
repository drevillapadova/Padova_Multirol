"""
Pricing por tramos (método de la reunión del 12-oct-2026 con Bruce Arce).

Idea en simple:
- El tramo de una venta lo define su ORDEN de venta, no la fecha: la venta n° 31
  de Orué cae en el tramo 2 si el tramo 1 es 1-30.
- Cada depa tiene un precio objetivo distinto en cada tramo:
      precio_tramo(depa, k) = área(depa) × S/ m² plan del tramo k × factor(depa)
  factor(depa) = S/ m² de lista del depa ÷ S/ m² de lista promedio del proyecto,
  para que un piso alto o un 1D sigan costando más por m² que el promedio, pero
  el conjunto quede al S/ m² del plan.
- Recaudación proyectada = lo vendido (a precio real) + lo que falta vender al
  precio del tramo en que le tocaría salir. Como no sabemos qué depa sale
  primero, en vez del orden aleatorio del Excel se usa el valor esperado: cada
  depa disponible puede salir en cualquiera de los turnos que quedan, así que su
  precio esperado usa el S/ m² promedio de esos turnos.
- Sobrerrecaudación = proyectada − requerida. Si se sigue vendiendo barato
  cuando ya tocaba subir, este número baja.

Los valores por defecto están en PARAMS; el plan de tramos, las metas y el
tramo por obra se editan desde el dashboard (ver pricing_config.py).
"""
from datetime import datetime, date
import math
import re

import pandas as pd

# ─── Parámetros por proyecto (fáciles de cambiar) ─────────────────────────────
PARAMS = {
    "DOMINGO ORUE": {
        # El precio de lista de Evolta está inflado este % sobre el precio final
        # esperado: lo que pase de aquí es descuento extra.
        "dscto_esperado": 0.07,
        # ¿Las separaciones ocupan cupo del tramo? (decisión pendiente)
        "cupo_cuenta_separaciones": False,
        # Inicio real del plan. None = fecha de la primera venta.
        "inicio_plan": None,
        # Umbrales de alerta
        "alerta_cupo_pct": 0.85,       # amarillo: tramo al 85% de su cupo
        "alerta_pm2_bajo_plan": 0.02,  # rojo: S/ m² vendido >2% bajo el plan
        "alerta_ritmo_pct": 0.70,      # rojo: ritmo <70% del plan 2 meses seguidos
        "alerta_ticket_pct": 0.10,     # amarillo: ticket >10% bajo el plan
        # Lista sugerida para Evolta: se redondea a este múltiplo (S/)
        "redondeo_lista": 100,
        # Estados del stock a los que se les sugiere cambiar la lista (a una
        # separación o un "pagando sin minuta" ya se le pactó el precio).
        "estados_con_lista": ["DISPONIBLE", "BLOQUEADO"],
    },
}


# ─── Utilidades ───────────────────────────────────────────────────────────────
def _num(v):
    s = str(v if v is not None else "").replace(",", "").strip()
    try:
        f = float(s)
        return f if math.isfinite(f) else 0.0
    except ValueError:
        return 0.0


def _nro(v):
    return re.sub(r"\.0+$", "", str(v or "").strip())


def _corr(v):
    m = re.search(r"(\d+)", str(v or ""))
    return int(m.group(1)) if m else 0


def _meses_entre(a, b):
    """Meses (con decimales) entre dos fechas."""
    return (b - a).days / 30.4375


# ─── Cálculo principal ────────────────────────────────────────────────────────
def calcular_pricing(proyecto, ventas, stock, plan, hoy=None, params=None):
    """ventas/stock: registros del cache (dicts de strings). `ventas` ya debe
    venir solo con EstadoOC = Activo. Devuelve un dict listo para jsonify."""
    proyecto = proyecto.upper().strip()
    P = dict(PARAMS.get(proyecto, PARAMS["DOMINGO ORUE"]))
    P.update(params or {})
    hoy = hoy or date.today()
    dscto = P["dscto_esperado"]
    tramos = [dict(t) for t in plan["tramos"]]
    if not tramos:
        return {"error": "No hay tramos en la configuración de pricing"}

    # ── Stock: una fila por depa (sin las filas históricas DEVUELTA) ──
    unidades = []
    for r in stock:
        if str(r.get("Proyecto", "")).upper().strip() != proyecto:
            continue
        if "DEPARTAMENTO" not in str(r.get("TipoInmueble", "")).upper():
            continue
        estado = str(r.get("Estado", "")).upper().strip()
        if estado == "DEVUELTA":
            continue
        area = _num(r.get("AreaTechada")) + 0.5 * _num(r.get("AreaLibre"))
        lista = _num(r.get("PrecioLista"))
        unidades.append({
            "edificio": _nro(r.get("Edificio")), "nro": _nro(r.get("NroInmuebleActual")),
            "modelo": str(r.get("Modelo", "")).strip(), "piso": _nro(r.get("NroPiso")),
            "dorm": int(_num(r.get("NroDormitorios"))), "area": area,
            "estado": estado, "dni": str(r.get("NroDocumento", "")).strip(),
            "lista_hoy": lista, "final_hoy": lista * (1 - dscto),
        })
    if not unidades:
        return {"error": f"Sin stock de departamentos para {proyecto}"}

    total_und = len(unidades)
    # El último tramo absorbe cualquier diferencia entre el stock real y el plan
    # (Orué: 341 depas en stock vs 340 en el plan).
    tramos[-1]["hasta"] = max(tramos[-1]["hasta"], total_und)

    def tramo_de(k):
        """Tramo por CUPO: según el n° de orden de la venta."""
        for t in tramos:
            if t["desde"] <= k <= t["hasta"]:
                return t
        return tramos[-1]

    # Tramo por OBRA: lo elige el equipo desde el dashboard (Pricing >
    # Configuración). Cada cambio queda con su fecha; sin fecha rige desde hoy.
    por_n = {t["n"]: t for t in tramos}
    hoy_ts = pd.Timestamp(hoy)
    obra = sorted(((pd.Timestamp(o["desde"]) if o["desde"] else hoy_ts, o["tramo"])
                   for o in plan.get("obra", []) if o["tramo"] in por_n), key=lambda x: x[0])

    def tramo_obra(f):
        n = 0
        for d, t in obra:
            if d <= f:
                n = max(n, t)
        return n

    def tramo_vigente(k, f):
        """Se sube por lo que pase primero: que se llene el cupo o que avance la obra."""
        t = tramo_de(k)
        n_ob = tramo_obra(f)
        return por_n[n_ob] if n_ob > t["n"] else t

    # Factor de cada depa: su S/ m² de lista frente al promedio del proyecto.
    area_tot = sum(u["area"] for u in unidades)
    pm2_lista_prom = sum(u["lista_hoy"] for u in unidades) / area_tot if area_tot else 0
    for u in unidades:
        pm2_u = u["lista_hoy"] / u["area"] if u["area"] else 0
        u["factor"] = pm2_u / pm2_lista_prom if pm2_lista_prom else 1.0
        u["precio_tramo"] = {t["n"]: u["area"] * t["pm2"] * u["factor"] for t in tramos}

    por_key = {(u["nro"], u["dni"]): u for u in unidades if u["dni"]}

    # ── Ventas activas en orden (fecha y hora; desempate CorrelativoOC) ──
    vs = []
    for r in ventas:
        if str(r.get("Proyecto", "")).upper().strip() != proyecto:
            continue
        if "DEPARTAMENTO" not in str(r.get("TipoInmueble", "")).upper():
            continue
        f = pd.to_datetime(r.get("FechaVenta") or r.get("FechaEntrega_Minuta"), errors="coerce")
        vs.append((f if pd.notna(f) else pd.Timestamp.max, _corr(r.get("CorrelativoOC")), r))
    vs.sort(key=lambda x: (x[0], x[1]))

    vendidos = []
    usados = set()
    for i, (f, corr, r) in enumerate(vs, start=1):
        u = por_key.get((_nro(r.get("NroInmueble")), str(r.get("NroDocumentoTitular", "")).strip()))
        t = tramo_vigente(i, f if f is not pd.Timestamp.max else hoy_ts)
        precio = _num(r.get("PrecioVentaSoles")) or _num(r.get("PrecioVenta"))
        lista_dia = _num(r.get("PrecioLista"))
        desc_lista = _num(r.get("DescuentoLista"))
        objetivo = u["precio_tramo"][t["n"]] if u else None
        if u:
            usados.add(id(u))
        vendidos.append({
            "orden": i, "fecha": f.strftime("%Y-%m-%d %H:%M") if f is not pd.Timestamp.max else "",
            "correlativo": str(r.get("CorrelativoOC", "")), "tramo": t["n"],
            "edificio": u["edificio"] if u else "", "nro": _nro(r.get("NroInmueble")),
            "modelo": str(r.get("Modelo", "")).strip(), "dorm": u["dorm"] if u else None,
            "area": round(u["area"], 2) if u else None,
            "factor": u["factor"] if u else None,
            "precio": precio, "lista_dia": lista_dia,
            "dscto": (desc_lista / lista_dia) if lista_dia else None,
            "pm2": (precio / u["area"]) if u and u["area"] else None,
            "pm2_plan": t["pm2"],
            "objetivo": objetivo,
            "dif_objetivo": (precio - objetivo) if objetivo else None,
            "canal": str(r.get("ComoSeEntero", "")).strip(),
            "_f": f,
        })
    n_vend = len(vendidos)

    # ── Cupo usado y tramo actual ──
    seps = [u for u in unidades if id(u) not in usados and
            ("SEPARAC" in u["estado"] or "PAGANDO" in u["estado"])]
    n_cupo = n_vend + (len(seps) if P["cupo_cuenta_separaciones"] else 0)
    disponibles = [u for u in unidades if id(u) not in usados]
    proxima = n_cupo + 1
    t_cupo = tramo_de(min(proxima, total_und))
    n_obra_hoy = tramo_obra(hoy_ts)
    t_actual = tramo_vigente(min(proxima, total_und), hoy_ts)
    motivo = "obra" if n_obra_hoy > t_cupo["n"] else "cupo"

    # ── Proyección (valor esperado) ──
    # Turnos que quedan: de la próxima venta (n_vend+1) hasta total_und. Ningún
    # turno puede salir por debajo del tramo en que ya está la obra; las fases
    # futuras de obra no se adivinan (se suben por cupo hasta que el equipo
    # registre el nuevo tramo por obra).
    turnos = list(range(n_vend + 1, total_und + 1))
    pm2_turnos = [tramo_vigente(k, hoy_ts)["pm2"] for k in turnos]
    pm2_esperado = sum(pm2_turnos) / len(pm2_turnos) if pm2_turnos else 0
    vendido_monto = sum(v["precio"] for v in vendidos)
    por_vender_plan = sum(u["area"] * u["factor"] * pm2_esperado for u in disponibles)
    por_vender_lista = sum(u["final_hoy"] for u in disponibles)
    requerida = plan["requerida"]
    proy_plan = vendido_monto + por_vender_plan
    proy_lista = vendido_monto + por_vender_lista

    # Desfase de la lista de hoy frente al precio que toca en el tramo actual
    obj_actual = sum(u["precio_tramo"][t_actual["n"]] for u in disponibles)
    desfase_lista = (por_vender_lista / obj_actual - 1) if obj_actual else None

    # ── Tabla por tramo ──
    tabla_tramos = []
    for t in tramos:
        vt = [v for v in vendidos if v["tramo"] == t["n"]]
        con_area = [v for v in vt if v["area"]]
        area_v = sum(v["area"] for v in con_area)
        pm2_real = sum(v["precio"] for v in con_area) / area_v if area_v else None
        obj = sum(v["objetivo"] for v in con_area)
        dif = sum(v["dif_objetivo"] for v in con_area) if con_area else 0
        ancho = t["hasta"] - t["desde"] + 1
        usados_t = max(0, min(n_cupo, t["hasta"]) - t["desde"] + 1)
        fechas = [v["_f"] for v in vt if v["_f"] is not pd.Timestamp.max]
        f_ini, f_fin = (min(fechas), max(fechas)) if fechas else (None, None)
        meses = max(_meses_entre(f_ini, f_fin if usados_t >= ancho else pd.Timestamp(hoy)), 0.5) if f_ini is not None else None
        if usados_t >= ancho:
            estado = "Cerrado"
        elif t is t_actual:
            estado = "En curso"
        elif t["n"] < t_actual["n"]:
            estado = "Cerrado por obra"   # la obra avanzó antes de llenar el cupo
        else:
            estado = "Pendiente"
        tabla_tramos.append({
            "tramo": t["n"], "etapa": t["etapa"], "fechas_plan": t["fechas"],
            "desde": t["desde"], "hasta": t["hasta"], "ancho": ancho,
            "vendidas": len(vt), "cupo_usado": usados_t, "avance": usados_t / ancho if ancho else 0,
            "pm2_plan": t["pm2"],
            "pm2_real": pm2_real,
            "desv_pm2": (pm2_real / t["pm2"] - 1) if pm2_real else None,
            "monto_vendido": sum(v["precio"] for v in vt),
            "objetivo": obj, "dif_objetivo": dif,
            "ritmo_plan": t["velocidad"],
            "ritmo_real": (len(vt) / meses) if meses else None,
            "inicio": f_ini.strftime("%Y-%m-%d") if f_ini is not None else None,
            "fin": f_fin.strftime("%Y-%m-%d") if (f_fin is not None and usados_t >= ancho) else None,
            "estado": estado,
        })

    # ── Ritmo mensual real vs. plan (perfil anclado a la fecha de inicio) ──
    fechas_v = [v["_f"] for v in vendidos if v["_f"] is not pd.Timestamp.max]
    inicio = pd.Timestamp(P["inicio_plan"]) if P["inicio_plan"] else (min(fechas_v) if fechas_v else pd.Timestamp(hoy))
    def perfil_acum(meses):
        """Ventas acumuladas que el plan esperaba tras `meses` desde el inicio."""
        acum, resto = 0.0, meses
        for t in tramos:
            m = t["meses"] or (t["unidades"] / t["velocidad"] if t["velocidad"] else 0)
            if resto <= 0 or not m:
                break
            tomar = min(resto, m)
            acum += t["unidades"] * tomar / m
            resto -= tomar
        return acum
    por_mes = {}
    for f in fechas_v:
        k = f.strftime("%Y-%m")
        por_mes[k] = por_mes.get(k, 0) + 1
    meses_lista = []
    cur = pd.Timestamp(inicio.year, inicio.month, 1)
    fin_mes = pd.Timestamp(hoy.year, hoy.month, 1)
    acum_real = 0
    while cur <= fin_mes:
        k = cur.strftime("%Y-%m")
        sig = cur + pd.offsets.MonthBegin(1)
        corte = min(sig, pd.Timestamp(hoy) + pd.Timedelta(days=1))
        acum_real += por_mes.get(k, 0)
        plan_ini = perfil_acum(max(_meses_entre(inicio, max(cur, inicio)), 0))
        plan_fin = perfil_acum(max(_meses_entre(inicio, corte), 0))
        meses_lista.append({
            "mes": k, "real": por_mes.get(k, 0), "plan": plan_fin - plan_ini,
            "real_acum": acum_real, "plan_acum": plan_fin,
            "parcial": corte < sig,
        })
        cur = sig
    perfil_hoy = perfil_acum(_meses_entre(inicio, pd.Timestamp(hoy)))
    ult90 = [f for f in fechas_v if f >= pd.Timestamp(hoy) - pd.Timedelta(days=90)]
    ritmo_3m = len(ult90) / 3

    # ── Por tipología ──
    tipologias = []
    for d in sorted({u["dorm"] for u in unidades}):
        us = [u for u in unidades if u["dorm"] == d]
        vd = [v for v in vendidos if v["dorm"] == d]
        area_v = sum(v["area"] for v in vd if v["area"])
        tipologias.append({
            "dorm": d, "total": len(us), "vendidas": len(vd),
            "stock": len([u for u in disponibles if u["dorm"] == d]),
            "avance": len(vd) / len(us) if us else 0,
            "area_prom": sum(u["area"] for u in us) / len(us),
            "ticket_real": (sum(v["precio"] for v in vd) / len(vd)) if vd else None,
            "pm2_real": (sum(v["precio"] for v in vd if v["area"]) / area_v) if area_v else None,
            "pm2_lista_final_hoy": sum(u["final_hoy"] for u in us) / sum(u["area"] for u in us),
        })
    ticket_real = vendido_monto / n_vend if n_vend else None

    # ── Descuento real contra el esperado ──
    dsctos = [v["dscto"] for v in vendidos if v["dscto"] is not None]
    dscto_prom = sum(dsctos) / len(dsctos) if dsctos else None
    sobre_dscto = [v for v in vendidos if v["dscto"] is not None and v["dscto"] > dscto + 0.0005]

    # ── Precios por depa para Evolta ──
    # Lista sugerida = precio objetivo del tramo ÷ (1 − descuento esperado),
    # porque la lista de Evolta va inflada ese % sobre el precio final.
    # Recuperación (lo que explicó Bruce): si lo proyectado no llega a lo
    # requerido, lo que falta se reparte como un % extra sobre todo lo que queda
    # por vender. Es el % que habría que sumar a los tramos que vienen.
    idx_act = next(i for i, t in enumerate(tramos) if t is t_actual)
    t_sig = tramos[min(idx_act + 1, len(tramos) - 1)]
    falta = requerida - vendido_monto
    recuperacion = (falta / por_vender_plan - 1) if por_vender_plan else 0.0
    recuperacion = max(recuperacion, 0.0)
    red = P["redondeo_lista"] or 1
    def _lista(final):
        return round(final / (1 - dscto) / red) * red
    for u in disponibles:
        u["lista_sug_act"] = _lista(u["precio_tramo"][t_actual["n"]])
        u["lista_sug_sig"] = _lista(u["precio_tramo"][t_sig["n"]])
        u["lista_sug_act_rec"] = _lista(u["precio_tramo"][t_actual["n"]] * (1 + recuperacion))
        u["lista_sug_sig_rec"] = _lista(u["precio_tramo"][t_sig["n"]] * (1 + recuperacion))
        u["cambia_lista"] = any(e in u["estado"] for e in P["estados_con_lista"])

    # ── Alertas ──
    alertas = []
    if not obra:
        alertas.append({"nivel": "amarillo", "tipo": "obra",
                        "texto": "No hay tramo por obra definido (Pricing > Configuración): el tramo se está subiendo solo por cupo vendido."})
    elif motivo == "obra":
        sin_usar = t_cupo["hasta"] - n_cupo
        alertas.append({"nivel": "amarillo", "tipo": "obra",
                        "texto": f"La obra está en el tramo {t_actual['n']} ({t_actual['etapa']}) y por ventas recién iba en el {t_cupo['n']}: "
                                 f"{sin_usar} unidades del cupo del tramo {t_cupo['n']} ya no se venden a ese precio."
                                 + (f" La lista de Evolta está {abs(desfase_lista):.1%} bajo el tramo {t_actual['n']}: cargar la lista sugerida (pestaña Precios por depa)."
                                    if desfase_lista is not None and desfase_lista < -0.0025 else "")})
    ancho_act = t_actual["hasta"] - t_actual["desde"] + 1
    usados_act = max(0, n_cupo - t_actual["desde"] + 1)
    if usados_act / ancho_act >= P["alerta_cupo_pct"]:
        alertas.append({"nivel": "amarillo", "tipo": "cupo",
                        "texto": f"El tramo {t_actual['n']} lleva {usados_act} de {ancho_act} ({usados_act/ancho_act:.0%}). Preparar la subida al tramo {t_actual['n']+1}."})
    if desfase_lista is not None and desfase_lista < -P["alerta_pm2_bajo_plan"]:
        alertas.append({"nivel": "rojo", "tipo": "lista",
                        "texto": f"La lista de hoy (menos {dscto:.0%}) está {abs(desfase_lista):.1%} por debajo del precio del tramo {t_actual['n']} (S/ {t_actual['pm2']:,.0f}/m²). Actualizar Evolta con la lista sugerida del tramo {t_actual['n']} (pestaña Precios por depa)."})
    if recuperacion > 0.0005:
        alertas.append({"nivel": "amarillo", "tipo": "recuperacion",
                        "texto": f"Aun subiendo según el plan faltan S/ {falta - por_vender_plan:,.0f} para la recaudación requerida: habría que sumar +{recuperacion:.1%} a todo lo que queda por vender (o subir más los tramos que vienen)."})
    for tt in tabla_tramos:
        if tt["vendidas"] and tt["desv_pm2"] is not None and tt["desv_pm2"] < -P["alerta_pm2_bajo_plan"]:
            alertas.append({"nivel": "rojo", "tipo": "pm2",
                            "texto": f"Tramo {tt['tramo']}: se vendió a S/ {tt['pm2_real']:,.0f}/m², {abs(tt['desv_pm2']):.1%} bajo el plan (S/ {tt['pm2_plan']:,.0f}). Diferencia: S/ {tt['dif_objetivo']:,.0f}."})
    completos = [m for m in meses_lista if not m["parcial"]]
    if len(completos) >= 2 and all(m["plan"] and m["real"] < P["alerta_ritmo_pct"] * m["plan"] for m in completos[-2:]):
        alertas.append({"nivel": "rojo", "tipo": "ritmo",
                        "texto": f"Ritmo bajo {P['alerta_ritmo_pct']:.0%} del plan en {completos[-2]['mes']} y {completos[-1]['mes']}."})
    if ticket_real and plan["ticket_plan"] and ticket_real < plan["ticket_plan"] * (1 - P["alerta_ticket_pct"]):
        alertas.append({"nivel": "amarillo", "tipo": "ticket",
                        "texto": f"Ticket promedio S/ {ticket_real:,.0f}, {1 - ticket_real/plan['ticket_plan']:.0%} bajo el plan (S/ {plan['ticket_plan']:,.0f}): se venden los depas más chicos."})
    if sobre_dscto:
        alertas.append({"nivel": "amarillo", "tipo": "descuento",
                        "texto": f"{len(sobre_dscto)} de {len(dsctos)} ventas con descuento mayor al {dscto:.0%} esperado (promedio {dscto_prom:.1%})."})

    for v in vendidos:
        v.pop("_f", None)

    return {
        "proyecto": proyecto,
        "params": P,
        "resumen": {
            "unidades": total_und, "unidades_plan": plan["unidades_plan"],
            "vendidas": n_vend, "separaciones": len(seps), "cupo_usado": n_cupo,
            "disponibles": len(disponibles),
            "tramo_actual": t_actual["n"], "tramo_etapa": t_actual["etapa"],
            "tramo_desde": t_actual["desde"], "tramo_hasta": t_actual["hasta"],
            "tramo_quedan": max(t_actual["hasta"] - n_cupo, 0),
            "pm2_tramo_actual": t_actual["pm2"],
            "pm2_lista_final_disp": (por_vender_lista / sum(u["area"] for u in disponibles)) if disponibles else None,
            "desfase_lista": desfase_lista,
            "vendido_monto": vendido_monto,
            "requerida": requerida,
            "proyectada_plan": proy_plan, "sobrerrecaudacion_plan": proy_plan - requerida,
            "proyectada_lista": proy_lista, "sobrerrecaudacion_lista": proy_lista - requerida,
            "pm2_esperado_restante": pm2_esperado,
            "dif_objetivo_total": sum(t["dif_objetivo"] for t in tabla_tramos),
            "ticket_real": ticket_real, "ticket_plan": plan["ticket_plan"],
            "dscto_prom": dscto_prom, "dscto_esperado": dscto,
            "ventas_sobre_dscto": len(sobre_dscto),
            "ritmo_3m": ritmo_3m, "ritmo_plan_actual": t_actual["velocidad"],
            "perfil_a_hoy": perfil_hoy, "inicio_plan": inicio.strftime("%Y-%m-%d"),
            "factor_lista": pm2_lista_prom,
            "tramo_siguiente": t_sig["n"], "pm2_tramo_siguiente": t_sig["pm2"],
            "tramo_cupo": t_cupo["n"], "tramo_obra": n_obra_hoy or None, "motivo_tramo": motivo,
            "obra_historial": [{"tramo": t, "desde": d.strftime("%Y-%m-%d")} for d, t in obra],
            "recuperacion_pct": recuperacion,
        },
        "tramos": tabla_tramos,
        "tipologias": tipologias,
        "meses": meses_lista,
        "ventas": vendidos,
        "por_vender": [{
            "edificio": u["edificio"], "nro": u["nro"], "modelo": u["modelo"],
            "piso": u["piso"], "dorm": u["dorm"], "area": round(u["area"], 2),
            "estado": u["estado"], "lista_hoy": u["lista_hoy"], "final_hoy": u["final_hoy"],
            "factor": u["factor"], "esperado": u["area"] * u["factor"] * pm2_esperado,
            "precio_tramo": {str(k): x for k, x in u["precio_tramo"].items()},
            "lista_sug_act": u["lista_sug_act"], "lista_sug_sig": u["lista_sug_sig"],
            "lista_sug_act_rec": u["lista_sug_act_rec"], "lista_sug_sig_rec": u["lista_sug_sig_rec"],
            "cambia_lista": u["cambia_lista"],
        } for u in sorted(disponibles, key=lambda u: (u["edificio"], int(u["nro"]) if u["nro"].isdigit() else 0))],
        "alertas": alertas,
    }
