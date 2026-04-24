import os, io, requests
from datetime import datetime
from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import pytz

app  = Flask(__name__)
LIMA = pytz.timezone("America/Lima")

# ─── SHEET CONFIG ────────────────────────────────────────────
SHEET_ID = "1JIEEGPxJvCHvmGvVE6Zp9wBPUVXEF-iXy8FNaWr1PPI"

TABS = {
    "ventas":      "0",
    "stock":       "1349464723",
    "prospectos":  "1786726820",
    "visitas":     "865520375",
    "meta_ads":    "1427834245",
    "google_ads":  "457505928",
    "tiktok_ads":  "515829502",
    "mkt_fisico":  "961281144",
    "presupuesto": "485749651",
}

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900',
    'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO'
]

# ─── CACHE ───────────────────────────────────────────────────
_cache = {k: [] for k in TABS}
_cache["updated_at"] = None


# ══════════════════════════════════════════════════════════════
# LECTURA DE SHEETS
# ══════════════════════════════════════════════════════════════

def csv_url(gid):
    return (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            f"/export?format=csv&gid={gid}")


def leer_tab(tab_key):
    """Lee una pestaña del Sheet como lista de dicts."""
    try:
        url  = csv_url(TABS[tab_key])
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        df   = pd.read_csv(io.StringIO(resp.text), low_memory=False)
        df   = df.fillna("").astype(str)
        records = df.to_dict(orient="records")
        print(f"   -> {tab_key}: {len(records):,} registros")
        return records
    except Exception as e:
        print(f"   !! Error leyendo {tab_key}: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# LÓGICA DE NEGOCIO
# ══════════════════════════════════════════════════════════════

def _float(d, *keys):
    for k in keys:
        v = str(d.get(k, "")).replace(",", "").strip()
        try:
            f = float(v)
            if f: return f
        except Exception:
            pass
    return 0.0


def _int(d, *keys):
    return int(_float(d, *keys))


def _str(d, *keys):
    """Lee campo de string probando múltiples nombres de columna (case-insensitive fallback)."""
    for k in keys:
        v = str(d.get(k, "")).strip()
        if v and v not in ("None", "nan", ""):
            return v
    # fallback: búsqueda case-insensitive
    lower_keys = [k.lower() for k in keys]
    for dk, dv in d.items():
        if str(dk).lower() in lower_keys:
            v = str(dv).strip()
            if v and v not in ("None", "nan", ""):
                return v
    return ""


def filtrar_proyecto(lst, proyecto, campo="Proyecto"):
    if not proyecto or proyecto == "TODOS":
        return lst
    return [r for r in lst if str(r.get(campo, "")).upper() == proyecto.upper()]


def calcular_funnel(ventas, prospectos, visitas, stock, proyecto=""):
    """Embudo completo con conversiones y tiempo de respuesta."""
    v  = filtrar_proyecto(ventas,     proyecto)
    p  = filtrar_proyecto(prospectos, proyecto)
    vi = filtrar_proyecto(visitas,    proyecto)
    s  = filtrar_proyecto(stock,      proyecto)

    seps = [r for r in s
            if "separac" in str(r.get("Estado", "")).lower()]

    ventas_conf = [r for r in v
                   if str(r.get("EstadoOC", "")).upper() != "DEVUELTO"
                   and str(r.get("Estado",  "")).upper()
                       not in ["DISPONIBLE", "BLOQUEADO"]]

    monto = sum(_float(r, "PrecioVentaSoles", "PrecioVenta")
                for r in ventas_conf)

    # Tiempo de respuesta mediano (minutos) — acepta nombres ETL y CRM
    tiempos = []
    for r in p:
        try:
            # Si ya tenemos TiempoRespuesta_min calculado, usarlo directamente
            if r.get("TiempoRespuesta_min"):
                try:
                    t = float(str(r["TiempoRespuesta_min"]).replace(",", ""))
                    if t > 0:
                        tiempos.append(t)
                    continue
                except Exception:
                    pass
            f1 = pd.to_datetime(
                r.get("Fecha_Registro_Sistema") or r.get("FechaRegistro") or "",
                dayfirst=True, errors="coerce")
            f2 = pd.to_datetime(
                r.get("FechaProspecto") or r.get("Fecha_PrimeraAccion") or "",
                dayfirst=True, errors="coerce")
            if pd.notna(f1) and pd.notna(f2) and f2 > f1:
                tiempos.append((f2 - f1).total_seconds() / 60)
        except Exception:
            pass
    t_resp = round(sorted(tiempos)[len(tiempos) // 2]) if tiempos else None

    return {
        "prospectos":         len(p),
        "visitas":            len(vi),
        "separaciones":       len(seps),
        "ventas":             len(ventas_conf),
        "monto_soles":        round(monto, 2),
        "t_respuesta_min":    t_resp,
        "conv_prosp_visita":  round(len(vi)  / len(p)   * 100, 1) if p   else 0,
        "conv_visita_sep":    round(len(seps) / len(vi)  * 100, 1) if vi  else 0,
        "conv_sep_venta":     round(len(ventas_conf) / len(seps) * 100, 1) if seps else 0,
        "conv_prosp_venta":   round(len(ventas_conf) / len(p)   * 100, 1) if p   else 0,
    }


def calcular_funnel_por_proyecto():
    """Funnel para cada proyecto + total."""
    resultado = {}
    for proj in TARGET_PROJECTS:
        resultado[proj] = calcular_funnel(
            _cache["ventas"], _cache["prospectos"],
            _cache["visitas"], _cache["stock"], proj
        )
    resultado["TODOS"] = calcular_funnel(
        _cache["ventas"], _cache["prospectos"],
        _cache["visitas"], _cache["stock"]
    )
    return resultado


def calcular_campanas():
    """Agrega campañas digitales + MKT físico + presupuesto."""
    def agg(registros, canal):
        out = []
        for r in registros:
            out.append({
                "canal":       canal,
                "proyecto":    _str(r, "proyecto",    "Proyecto",    "PROYECTO"),
                "campaña":     _str(r, "campaña",     "Campaña",     "Campana",  "campana"),
                "fecha":       _str(r, "fecha",       "Fecha",       "FECHA"),
                "inversion":   _float(r, "inversion", "Inversión",   "Inversion"),
                "leads":       _int(r,   "leads",     "Leads",       "LEADS"),
                "cpl":         _float(r, "cpl",       "CPL",         "Cpl"),
                "ctr":         _float(r, "ctr",       "CTR",         "Ctr"),
                "impresiones": _int(r,   "impresiones","Impresiones","IMPRESIONES"),
            })
        return out

    detalle = (agg(_cache["meta_ads"],   "Meta Ads")
             + agg(_cache["google_ads"], "Google Ads")
             + agg(_cache["tiktok_ads"], "TikTok Ads"))

    # Resumen por canal
    resumen = {}
    for row in detalle:
        c = row["canal"]
        if c not in resumen:
            resumen[c] = {"inversion": 0, "leads": 0, "impresiones": 0}
        resumen[c]["inversion"]   += row["inversion"]
        resumen[c]["leads"]       += row["leads"]
        resumen[c]["impresiones"] += row["impresiones"]

    for c in resumen:
        inv   = resumen[c]["inversion"]
        leads = resumen[c]["leads"]
        resumen[c]["cpl"] = round(inv / leads, 2) if leads else 0

    # Resumen MKT físico
    mkt = _cache["mkt_fisico"]
    mkt_resumen = {}
    for r in mkt:
        tipo = _str(r, "tipo_accion", "Tipo_accion", "Tipo", "tipo") or "otro"
        if tipo not in mkt_resumen:
            mkt_resumen[tipo] = {"costo": 0, "leads": 0, "acciones": 0}
        mkt_resumen[tipo]["costo"]    += _float(r, "costo",            "Costo")
        mkt_resumen[tipo]["leads"]    += _int(r,   "leads_atribuidos", "Leads_atribuidos", "Leads")
        mkt_resumen[tipo]["acciones"] += 1

    # Presupuesto vs real por canal/mes
    presup = {}
    for r in _cache["presupuesto"]:
        mes   = _str(r, "mes",   "Mes",   "MES")
        canal = _str(r, "canal", "Canal", "CANAL")
        key   = f"{mes}|{canal}"
        presup[key] = {
            "mes":               mes,
            "proyecto":          _str(r,   "proyecto",          "Proyecto"),
            "canal":             canal,
            "presupuesto":       _float(r, "presupuesto",       "Presupuesto"),
            "meta_leads":        _int(r,   "meta_leads",        "Meta_leads",   "Meta Leads"),
            "meta_cpl":          _float(r, "meta_cpl",          "Meta_CPL",     "Meta CPL"),
            "meta_separaciones": _int(r,   "meta_separaciones", "Meta_sep",     "Meta Sep"),
        }

    return {
        "detalle":     detalle,
        "resumen":     resumen,
        "mkt_fisico":  {"detalle": mkt, "resumen": mkt_resumen},
        "presupuesto": list(presup.values()),
    }


def calcular_desistimientos():
    """Analiza registros devueltos del CRM."""
    ventas = _cache["ventas"]

    desist = [r for r in ventas
              if str(r.get("EstadoOC", "")).upper() == "DEVUELTO"
              or (str(r.get("FechaDevolucion", "")).strip()
                  not in ["", "None", "nan"])]

    # Por canal origen
    por_canal = {}
    for r in desist:
        canal = str(r.get("ComoSeEntero", "Otro")).strip() or "Otro"
        por_canal[canal] = por_canal.get(canal, 0) + 1

    # Por proyecto
    por_proyecto = {}
    for r in desist:
        proj = str(r.get("Proyecto", "Otro")).strip()
        por_proyecto[proj] = por_proyecto.get(proj, 0) + 1

    # Por mes
    por_mes = {}
    for r in desist:
        fecha_str = str(r.get("FechaDevolucion", "") or r.get("FechaVenta", ""))
        try:
            mes = pd.to_datetime(fecha_str, dayfirst=True,
                                 errors="coerce").strftime("%Y-%m")
            if mes and mes != "NaT":
                por_mes[mes] = por_mes.get(mes, 0) + 1
        except Exception:
            pass

    return {
        "total":        len(desist),
        "por_canal":    por_canal,
        "por_proyecto": por_proyecto,
        "por_mes":      dict(sorted(por_mes.items())),
        "registros":    desist[:300],
    }


def calcular_stock_resumen():
    """Stock disponible, separado, vendido por proyecto y tipología."""
    stock = _cache["stock"]
    resumen = {}
    for r in stock:
        proj  = str(r.get("Proyecto", "")).upper().strip()
        tipo  = str(r.get("TipoInmueble", "")).strip()
        estado= str(r.get("Estado", "")).upper().strip()
        if proj not in resumen:
            resumen[proj] = {}
        if tipo not in resumen[proj]:
            resumen[proj][tipo] = {"disponible": 0, "separado": 0, "vendido": 0, "total": 0}
        resumen[proj][tipo]["total"] += 1
        if "disponib" in estado.lower():
            resumen[proj][tipo]["disponible"] += 1
        elif "separac" in estado.lower():
            resumen[proj][tipo]["separado"] += 1
        elif "vendid" in estado.lower() or "minuta" in estado.lower():
            resumen[proj][tipo]["vendido"] += 1
    return resumen


# ══════════════════════════════════════════════════════════════
# CACHE — actualización
# ══════════════════════════════════════════════════════════════

def actualizar_cache():
    global _cache
    ts = datetime.now(LIMA).strftime("%H:%M:%S")
    print(f"\n[{ts}] Actualizando cache desde Google Sheets...")
    for key in TABS:
        _cache[key] = leer_tab(key)
    _cache["updated_at"] = datetime.now(LIMA).strftime("%d/%m/%Y %H:%M")
    print(f"   -> Cache OK · {_cache['updated_at']}")


# ══════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify({
        **{k: len(_cache[k]) for k in TABS},
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/funnel")
def api_funnel():
    proyecto = request.args.get("proyecto", "").upper()
    if proyecto and proyecto != "TODOS":
        data = {proyecto: calcular_funnel(
            _cache["ventas"], _cache["prospectos"],
            _cache["visitas"], _cache["stock"], proyecto
        )}
    else:
        data = calcular_funnel_por_proyecto()
    return jsonify({"data": data, "updated_at": _cache["updated_at"]})


@app.route("/api/ventas")
def api_ventas():
    proyecto = request.args.get("proyecto", "").upper()
    año      = request.args.get("año", "")
    mes      = request.args.get("mes", "")

    ventas = _cache["ventas"]
    stock  = _cache["stock"]

    ventas = filtrar_proyecto(ventas, proyecto)
    stock  = filtrar_proyecto(stock,  proyecto)

    if año:
        ventas = [r for r in ventas if str(r.get("AÑO", "")) == año]
    if mes:
        ventas = [r for r in ventas
                  if str(r.get("FechaVenta", "")).startswith(mes)
                  or str(r.get("FechaEntrega_Minuta","")).startswith(mes)]

    seps = [r for r in stock
            if "separac" in str(r.get("Estado", "")).lower()]

    return jsonify({
        "ventas":       ventas,
        "separaciones": seps,
        "updated_at":   _cache["updated_at"]
    })


@app.route("/api/stock")
def api_stock():
    proyecto = request.args.get("proyecto", "").upper()
    return jsonify({
        "data":    filtrar_proyecto(_cache["stock"], proyecto),
        "resumen": calcular_stock_resumen(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/prospectos")
def api_prospectos():
    proyecto = request.args.get("proyecto", "").upper()
    return jsonify({
        "data": filtrar_proyecto(_cache["prospectos"], proyecto),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/visitas")
def api_visitas():
    proyecto = request.args.get("proyecto", "").upper()
    return jsonify({
        "data": filtrar_proyecto(_cache["visitas"], proyecto),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/campanas")
def api_campanas():
    return jsonify({
        "data": calcular_campanas(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/desistimientos")
def api_desistimientos():
    return jsonify({
        "data": calcular_desistimientos(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    actualizar_cache()
    return jsonify({"ok": True, "updated_at": _cache["updated_at"]})


# ══════════════════════════════════════════════════════════════
# ARRANQUE
# ══════════════════════════════════════════════════════════════

actualizar_cache()

scheduler = BackgroundScheduler(timezone=LIMA)
scheduler.add_job(actualizar_cache, "interval", hours=1)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)