import os, io, re, requests, math, time, json
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import pandas as pd
import pytz

app  = Flask(__name__)
LIMA = pytz.timezone("America/Lima")

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_ACCESS_TOKEN_MIRANO_BM = os.getenv("META_ACCESS_TOKEN_MIRANO_BM", "")
META_API_VERSION = "v21.0"
META_ADS_PROJECTS = [
    {"id": "322739664119423",  "label": "HELIO - SANTA BEATRIZ", "token": META_ACCESS_TOKEN},
    {"id": "992112534609798",  "label": "LOMAS DE CARABAYLLO",    "token": META_ACCESS_TOKEN},
    {"id": "1715327522558274", "label": "SUNNY",                  "token": META_ACCESS_TOKEN_MIRANO_BM},
    {"id": "980901671201759",  "label": "DOMINGO ORUE",           "token": META_ACCESS_TOKEN_MIRANO_BM},
    {"id": "474169308224886",  "label": "LITORAL 900",            "token": META_ACCESS_TOKEN},
]
_meta_ads_cache = {}
META_ADS_CACHE_TTL = 300  # 5 min
META_LEAD_ACTION_TYPES = ["onsite_conversion.lead_grouped", "lead"]
META_WHATSAPP_ACTION_TYPES = ["onsite_conversion.messaging_conversation_started_7d"]


META_ESTADO_ES = {
    "ACTIVE": "Activa", "PAUSED": "Pausada", "ARCHIVED": "Archivada",
    "DELETED": "Eliminada", "PENDING_REVIEW": "En revisión",
    "DISAPPROVED": "Rechazada", "CAMPAIGN_PAUSED": "Pausada",
    "ADSET_PAUSED": "Pausada", "IN_PROCESS": "En proceso",
    "WITH_ISSUES": "Con problemas",
}


def _extract_meta_action(actions, types):
    for t in types:
        for a in actions:
            if a.get("action_type") == t:
                try:
                    return float(a["value"])
                except (TypeError, ValueError):
                    return 0
    return 0


def _meta_paginate(url, params):
    out = []
    while True:
        resp = requests.get(url, params=params, timeout=20)
        j = resp.json()
        if "error" in j:
            raise Exception(j["error"].get("message", "Error Meta API"))
        out.extend(j.get("data", []))
        next_url = j.get("paging", {}).get("next")
        if not next_url:
            break
        url, params = next_url, {}
    return out


def _meta_row_metrics(row):
    actions = row.get("actions", [])
    spend = float(row.get("spend") or 0)
    leads = _extract_meta_action(actions, META_LEAD_ACTION_TYPES)
    whats = _extract_meta_action(actions, META_WHATSAPP_ACTION_TYPES)
    return spend, leads, whats


def _fetch_meta_account(account_id, since, until, token):
    base = f"https://graph.facebook.com/{META_API_VERSION}/act_{account_id}"
    time_range = json.dumps({"since": since, "until": until})

    camp_insights = _meta_paginate(base + "/insights", {
        "level": "campaign",
        "fields": "campaign_id,campaign_name,spend,actions,impressions,clicks",
        "time_range": time_range, "limit": 200, "access_token": token,
    })
    camp_meta_rows = _meta_paginate(base + "/campaigns", {
        "fields": "id,effective_status,daily_budget,lifetime_budget",
        "limit": 200, "access_token": token,
    })
    camp_meta = {c["id"]: c for c in camp_meta_rows}
    ad_insights = _meta_paginate(base + "/insights", {
        "level": "ad",
        "fields": "ad_id,ad_name,spend,actions",
        "time_range": time_range, "limit": 500, "access_token": token,
    })

    campaigns = []
    for row in camp_insights:
        spend, leads, whats = _meta_row_metrics(row)
        impresiones = float(row.get("impressions") or 0)
        clicks = float(row.get("clicks") or 0)
        resultados = leads + whats
        meta = camp_meta.get(row.get("campaign_id"), {})
        daily_budget = meta.get("daily_budget")
        campaigns.append({
            "nombre": row.get("campaign_name", ""),
            "estado": meta.get("effective_status", ""),
            "gasto": spend, "leads": leads, "whatsapp": whats, "resultados": resultados,
            "cpl": (spend / resultados) if resultados else None,
            "impresiones": impresiones, "clicks": clicks,
            "cpm": (spend / impresiones * 1000) if impresiones else None,
            "cpc": (spend / clicks) if clicks else None,
            "daily_budget": (float(daily_budget) / 100) if daily_budget else None,
        })

    ads = []
    for row in ad_insights:
        spend, leads, whats = _meta_row_metrics(row)
        resultados = leads + whats
        ads.append({
            "nombre": row.get("ad_name", ""),
            "gasto": spend, "resultados": resultados,
            "cpl": (spend / resultados) if resultados else None,
        })

    daily_rows = _meta_paginate(base + "/insights", {
        "level": "account",
        "fields": "spend,actions",
        "time_increment": "1",
        "time_range": time_range, "limit": 200, "access_token": token,
    })
    daily = []
    for row in sorted(daily_rows, key=lambda r: r.get("date_start", "")):
        spend, leads, whats = _meta_row_metrics(row)
        resultados = leads + whats
        daily.append({
            "fecha": row.get("date_start", ""),
            "gasto": spend, "resultados": resultados,
            "cpl": (spend / resultados) if resultados else None,
        })

    return campaigns, ads, daily

# ─── SHEET CONFIG ────────────────────────────────────────────
SHEET_ID = "1JIEEGPxJvCHvmGvVE6Zp9wBPUVXEF-iXy8FNaWr1PPI"

TABS = {
    "ventas":      "0",
    "stock":       "1349464723",
    "prospectos":  "1786726820",
    "visitas":     "865520375",
    "meta_ads":    "1427834245",
    "ingreso_deposito": "457505928",
    "inversion":   "515829502",
    "flujo_caja":  "961281144",
    "presupuesto": "485749651",
    "mercado":     "477763204",
}

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900',
    'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO 4',
    'LOMAS DE CARABAYLLO 5',
    'DOMINGO ORUE',
]


_ALLOWED_PROJECTS = {p.upper() for p in TARGET_PROJECTS if 'LOMAS' not in p}

def _normalizar_proyectos(registros):
    """Whitelist de proyectos: renombra Lomas por etapa, descarta todo lo demás."""
    result = []
    for r in registros:
        proj = str(r.get("Proyecto", "")).upper().strip()
        if "LOMAS DE CARABAYLLO" in proj:
            # Buscar número 4 o 5 en el nombre del proyecto o en Etapa
            m = re.search(r'\b(4|5)\b', proj) or re.search(r'\d+', str(r.get("Etapa", "")))
            num = m.group() if m else ""
            if num in ("4", "5"):
                r = dict(r)
                r["Proyecto"] = f"LOMAS DE CARABAYLLO {num}"
                result.append(r)
            else:
                # Sin etapa identificable (prospectos/visitas): conservar genérico
                r = dict(r)
                r["Proyecto"] = "LOMAS DE CARABAYLLO"
                result.append(r)
        elif proj in _ALLOWED_PROJECTS:
            result.append(r)
    return result

# ─── CACHE ───────────────────────────────────────────────────
_cache = {k: [] for k in TABS}
_cache["updated_at"] = None


# ══════════════════════════════════════════════════════════════
# LECTURA DE SHEETS
# ══════════════════════════════════════════════════════════════

def csv_url(gid):
    from time import time
    return (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            f"/export?format=csv&gid={gid}&ts={int(time())}")


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
            if f and math.isfinite(f): return f
        except Exception:
            pass
    return 0.0


def _parse_num(s):
    """Parsea valores tipo 'S/. 22,850,449.54', '$ 1,234.56' o '1.60%'."""
    try:
        v = str(s).replace(",", "").replace("S/.", "").replace("S/", "").replace("$", "").replace("%", "").strip()
        f = float(v)
        return f if math.isfinite(f) else 0.0
    except Exception:
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


def _parse_sol(v):
    """Parsea 'S/.4.550,00' o '4,550.00' → float (formato peruano: . miles, , decimal)."""
    s = re.sub(r'[S/\s]', '', str(v)).strip().lstrip('.')
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def filtrar_proyecto(lst, proyecto, campo="Proyecto"):
    if not proyecto or proyecto == "TODOS":
        return lst
    return [r for r in lst if str(r.get(campo, "")).upper() == proyecto.upper()]


def _parse_coord_pe(v):
    """Corrige Latitud/Longitud de la pestaña TABLEAU: el export de Sheets perdió
    el punto decimal y las mostró como enteros con separador de miles, ej.
    '-120.977.732' en vez de '-12.0977732'. Perú: la parte entera de lat/lon
    siempre tiene 2 dígitos (lat 0-19, lon 68-85), así que se reinserta el
    punto decimal justo después de esos 2 dígitos."""
    s = str(v or "").strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    neg = s.startswith("-")
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) < 3:
        return None
    try:
        num = float(digits[:2] + "." + digits[2:])
    except ValueError:
        return None
    return -num if neg else num


def calcular_mercado():
    """Agrupa el Estudio de Mercado (pestaña TABLEAU, 1 fila = 1 unidad vendida
    de cualquier inmobiliaria) por Inmobiliaria+Proyecto: 1 punto por proyecto
    para el mapa, con unidades vendidas totales, ticket promedio, precio por
    m2 promedio y rango de metraje."""
    rows = _cache.get("mercado", [])
    grupos = {}
    for r in rows:
        inmob = _str(r, "Inmobiliaria")
        proy  = _str(r, "Nombre de Proyecto")
        if not inmob or not proy:
            continue
        key = (inmob, proy)
        if key not in grupos:
            grupos[key] = {
                "inmobiliaria": inmob,
                "proyecto":     proy,
                "distrito":     _str(r, "Distrito"),
                "sector":       _str(r, "Sector"),
                "direccion":    _str(r, "Dirección"),
                "fase_proyecto": _str(r, "Fase de Proyecto"),
                "fecha_entrega": _parse_fecha_mercado(r.get("Fecha de Entrega")),
                "lat":          _parse_coord_pe(r.get("Latitud")),
                "lng":          _parse_coord_pe(r.get("Longitud")),
                "unidades_vendidas": 0,
                "unidades_totales": 0,
                "cantidad_pisos": 0,
                "_precio_sum":   0.0,
                "_precio_n":     0,
                "_precio_m2_sum": 0.0,
                "_precio_m2_n":   0,
                "_area_min":     None,
                "_area_max":     None,
            }
        g = grupos[key]
        g["unidades_vendidas"] += _int(r, "Cantidad de Unidades Vendidas") or 1
        g["unidades_totales"] = max(g["unidades_totales"], _int(r, "Cantidad de Unidades Totales"))
        g["cantidad_pisos"] = max(g["cantidad_pisos"], _int(r, "Cantidad de Pisos"))
        if not g["fase_proyecto"]:
            g["fase_proyecto"] = _str(r, "Fase de Proyecto")
        if not g["fecha_entrega"]:
            g["fecha_entrega"] = _parse_fecha_mercado(r.get("Fecha de Entrega"))
        precio = _float(r, "Precio de Venta Solarizado Neto")
        if precio:
            g["_precio_sum"] += precio
            g["_precio_n"]   += 1
        precio_m2 = _float(r, "Precio por m2 - Venta Solarizado")
        if precio_m2:
            g["_precio_m2_sum"] += precio_m2
            g["_precio_m2_n"]   += 1
        area = _float(r, "Área Total")
        if area:
            g["_area_min"] = area if g["_area_min"] is None else min(g["_area_min"], area)
            g["_area_max"] = area if g["_area_max"] is None else max(g["_area_max"], area)

    out = []
    for g in grupos.values():
        if g["lat"] is None or g["lng"] is None:
            continue
        out.append({
            "inmobiliaria":      g["inmobiliaria"],
            "proyecto":          g["proyecto"],
            "distrito":          g["distrito"],
            "sector":            g["sector"],
            "direccion":         g["direccion"],
            "fase_proyecto":     g["fase_proyecto"],
            "fecha_entrega":     g["fecha_entrega"],
            "cantidad_pisos":    g["cantidad_pisos"],
            "lat":               g["lat"],
            "lng":               g["lng"],
            "unidades_vendidas": g["unidades_vendidas"],
            "unidades_totales":  g["unidades_totales"],
            "precio_promedio":   round(g["_precio_sum"] / g["_precio_n"], 2) if g["_precio_n"] else 0,
            "precio_m2_promedio": round(g["_precio_m2_sum"] / g["_precio_m2_n"], 2) if g["_precio_m2_n"] else 0,
            "area_min":          g["_area_min"],
            "area_max":          g["_area_max"],
        })
    return out


def _parse_fecha_mercado(s):
    """'Fecha de Venta' de TABLEAU viene como '30/05/2026 12:00:00 a. m.'
    (DD/MM/YYYY + hora en español). Solo interesa la fecha -> ISO 'YYYY-MM-DD'."""
    s = str(s or "").strip()
    if not s:
        return None
    fecha_part = s.split(" ")[0].strip()
    try:
        d, m, y = fecha_part.split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except ValueError:
        return None


def calcular_mercado_ventas():
    """Filas del Estudio de Mercado (pestaña TABLEAU) con los campos que
    necesitan los cuadros de Ventas: tendencia mensual y distribución por
    dormitorio. Fecha de Venta ya parseada a ISO; precios/áreas ya limpios."""
    rows = _cache.get("mercado", [])
    out = []
    for r in rows:
        proy = _str(r, "Nombre de Proyecto")
        if not proy:
            continue
        fecha_iso = _parse_fecha_mercado(r.get("Fecha de Venta"))
        if not fecha_iso:
            continue
        out.append({
            "proyecto":          proy,
            "inmobiliaria":      _str(r, "Inmobiliaria"),
            "distrito":          _str(r, "Distrito"),
            "fecha_venta":       fecha_iso,
            "dormitorios":       _int(r, "Cantidad de Dormitorios"),
            "precio_m2_oferta":  _float(r, "Precio por m2 - Oferta Solarizado"),
            "precio_m2_venta":   _float(r, "Precio por m2 - Venta Solarizado"),
            "area_total":        _float(r, "Área Total"),
            "precio_venta":      _float(r, "Precio de Venta Solarizado Neto"),
        })
    return out


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

    detalle = agg(_cache["meta_ads"], "Meta Ads")

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

    # Presupuesto por proyecto — encabezados reales desde fila 5
    presup_proyectos = []
    for r in _cache["presupuesto"]:
        proyecto = _str(r, "Proyectos Venta", "PROYECTO", "Proyecto", "proyecto")
        if not proyecto or proyecto in ("nan", "None"):
            continue
        # Col J: header cambia cada mes ("PPTO EJECUTADO ACTUAL ABR 26", etc.)
        ejecutado_key = None
        for k in r:
            if "EJECUTADO" in str(k).upper():
                ejecutado_key = k
                break
        asignado  = _parse_num(r.get("PPTO MKT ASIGNADO SIN IGV", ""))
        ejecutado = _parse_num(r.get(ejecutado_key, "")) if ejecutado_key else 0.0
        # Litoral y La Molina manejan presupuesto en USD → convertir a soles (×4)
        proy_up = proyecto.upper()
        if "LITORAL" in proy_up or "MOLINA" in proy_up or "SUNNY" in proy_up:
            asignado  *= 4
            ejecutado *= 4
        presup_proyectos.append({
            "proyecto":        proyecto,
            "asignado":        asignado,
            "perfil_pct":      _parse_num(r.get("PPTO PERFIL", "")),
            "ejecutado":       ejecutado,
            "costo_venta_pct": _parse_num(r.get("Costo de Venta Prom.", "")),
        })

    return {
        "detalle":               detalle,
        "resumen":               resumen,
        "presupuesto":           list(presup.values()),
        "presupuesto_proyectos": presup_proyectos,
    }


_MES_NUM = {
    'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
    'julio':7,'agosto':8,'septiembre':9,'setiembre':9,
    'octubre':10,'noviembre':11,'diciembre':12,
}

def parsear_inversion():
    """Convierte la pestaña inversión (tabla pivote) a lista de {proyecto, canal, mes, monto}."""
    registros = _cache.get("inversion", [])
    if not registros:
        return []
    all_keys = list(registros[0].keys())
    # Detectar columnas de proyecto y canal (pueden ser PROYECTO/CANAL o Unnamed: 0/1)
    proj_key  = next((k for k in all_keys if str(k).upper() in ('PROYECTO', 'UNNAMED: 0')), all_keys[0])
    canal_key = next((k for k in all_keys if str(k).upper() in ('CANAL', 'UNNAMED: 1')), all_keys[1] if len(all_keys)>1 else '')
    mes_cols  = [k for k in all_keys if k not in (proj_key, canal_key)]
    result = []
    for r in registros:
        proyecto = str(r.get(proj_key, '')).strip()
        canal    = str(r.get(canal_key, '')).strip()
        if not proyecto or proyecto.upper() in ('PROYECTO', 'TOTAL', 'TOTALES'):
            continue
        for col in mes_cols:
            monto = _parse_sol(r.get(col, ''))
            if monto > 0:
                result.append({
                    'proyecto': proyecto,
                    'canal':    canal,
                    'mes':      col.strip(),
                    'monto':    monto,
                })
    return result


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

TABS_CON_PROYECTO = {"ventas", "stock", "prospectos", "visitas", "flujo_caja", "ingreso_deposito"}

def leer_tab_header(tab_key, header_row):
    """Lee una pestaña usando una fila específica como encabezado (0-indexed)."""
    try:
        url  = csv_url(TABS[tab_key])
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        df   = pd.read_csv(io.StringIO(resp.text), header=header_row, low_memory=False)
        df   = df.fillna("").astype(str)
        records = df.to_dict(orient="records")
        print(f"   -> {tab_key} (header={header_row+1}): {len(records):,} registros")
        return records
    except Exception as e:
        print(f"   !! Error leyendo {tab_key}: {e}")
        return []


def actualizar_cache():
    global _cache
    ts = datetime.now(LIMA).strftime("%H:%M:%S")
    print(f"\n[{ts}] Actualizando cache desde Google Sheets...")
    for key in TABS:
        if key == "presupuesto":
            # Encabezados en fila 5 (0-indexed = 4), datos desde fila 6
            raw = leer_tab_header(key, header_row=4)
        else:
            raw = leer_tab(key)
        _cache[key] = _normalizar_proyectos(raw) if key in TABS_CON_PROYECTO else raw
    _cache["ventas"] = [r for r in _cache["ventas"]
                        if str(r.get("EstadoOC", "")).strip() == "Activo"]
    # FechaVisita es el campo canónico confirmado en el Google Sheets de visitas
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


@app.route("/api/inversion")
def api_inversion():
    return jsonify({
        "data": parsear_inversion(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/desistimientos")
def api_desistimientos():
    return jsonify({
        "data": calcular_desistimientos(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/ingreso_deposito")
def api_ingreso_deposito():
    proyecto = request.args.get("proyecto", "").upper()
    return jsonify({
        "data": filtrar_proyecto(_cache["ingreso_deposito"], proyecto),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/flujo_caja")
def api_flujo_caja():
    proyecto = request.args.get("proyecto", "").upper()
    return jsonify({
        "data": filtrar_proyecto(_cache["flujo_caja"], proyecto),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/mercado")
def api_mercado():
    return jsonify({
        "data": calcular_mercado(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/mercado_ventas")
def api_mercado_ventas():
    return jsonify({
        "data": calcular_mercado_ventas(),
        "updated_at": _cache["updated_at"]
    })


@app.route("/api/meta_ads")
def api_meta_ads():
    if not META_ACCESS_TOKEN and not META_ACCESS_TOKEN_MIRANO_BM:
        return jsonify({"error": "META_ACCESS_TOKEN no configurado"}), 500
    desde = request.args.get("desde", "")
    hasta = request.args.get("hasta", "")
    if not desde or not hasta:
        hasta_dt = datetime.now(LIMA)
        desde_dt = hasta_dt - timedelta(days=30)
        desde = desde or desde_dt.strftime("%Y-%m-%d")
        hasta = hasta or hasta_dt.strftime("%Y-%m-%d")
    cache_key = f"{desde}_{hasta}"
    cached = _meta_ads_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < META_ADS_CACHE_TTL:
        return jsonify(cached["data"])
    result = []
    for proj in META_ADS_PROJECTS:
        try:
            if not proj["token"]:
                raise Exception("Token no configurado para esta cuenta")
            campaigns, ads, daily = _fetch_meta_account(proj["id"], desde, hasta, proj["token"])
            gasto       = sum(c["gasto"] for c in campaigns)
            leads       = sum(c["leads"] for c in campaigns)
            whatsapp    = sum(c["whatsapp"] for c in campaigns)
            impresiones = sum(c["impresiones"] for c in campaigns)
            clicks      = sum(c["clicks"] for c in campaigns)
            total = leads + whatsapp
            cpl = (gasto / total) if total else None
            cpm = (gasto / impresiones * 1000) if impresiones else None
            cpc = (gasto / clicks) if clicks else None
            presupuesto_dia = sum(
                c["daily_budget"] for c in campaigns
                if c["daily_budget"] and c["estado"] == "ACTIVE"
            )

            campanas_out = sorted([{
                "nombre": c["nombre"],
                "estado": META_ESTADO_ES.get(c["estado"], c["estado"] or "—"),
                "gasto": round(c["gasto"], 2),
                "resultados": int(c["resultados"]),
                "cpl": round(c["cpl"], 2) if c["cpl"] is not None else None,
                "cpm": round(c["cpm"], 2) if c["cpm"] is not None else None,
                "cpc": round(c["cpc"], 2) if c["cpc"] is not None else None,
            } for c in campaigns if c["gasto"] > 0], key=lambda x: x["gasto"], reverse=True)

            # Top 5 anuncios: mejor CPL entre los que tienen resultados y gasto relevante
            top_candidatos = [a for a in ads if a["resultados"] > 0 and a["gasto"] >= 5]
            top_ads = sorted(top_candidatos, key=lambda a: a["cpl"])[:5]

            # A revisar: gasto relevante sin resultados, o CPL muy por encima del promedio del proyecto
            def _riesgo_key(a):
                if a["resultados"] == 0:
                    return (0, -a["gasto"])
                return (1, -a["cpl"])
            revisar_candidatos = [
                a for a in ads if a["gasto"] >= 10 and
                (a["resultados"] == 0 or (cpl and a["cpl"] > cpl * 1.3))
            ]
            ads_revisar = sorted(revisar_candidatos, key=_riesgo_key)[:5]

            result.append({
                "label": proj["label"], "account_id": proj["id"],
                "gasto": round(gasto, 2), "leads_formulario": int(leads),
                "leads_whatsapp": int(whatsapp), "total_resultados": int(total),
                "cpl": round(cpl, 2) if cpl is not None else None,
                "impresiones": int(impresiones), "clicks": int(clicks),
                "cpm": round(cpm, 2) if cpm is not None else None,
                "cpc": round(cpc, 2) if cpc is not None else None,
                "presupuesto_dia": round(presupuesto_dia, 2) if presupuesto_dia else None,
                "campanas": campanas_out,
                "top_ads": [{
                    "nombre": a["nombre"], "cpl": round(a["cpl"], 2),
                    "resultados": int(a["resultados"]), "gasto": round(a["gasto"], 2),
                } for a in top_ads],
                "ads_revisar": [{
                    "nombre": a["nombre"],
                    "cpl": round(a["cpl"], 2) if a["cpl"] is not None else None,
                    "resultados": int(a["resultados"]), "gasto": round(a["gasto"], 2),
                } for a in ads_revisar],
                "evolucion_diaria": [{
                    "fecha": d["fecha"], "gasto": round(d["gasto"], 2),
                    "resultados": int(d["resultados"]),
                    "cpl": round(d["cpl"], 2) if d["cpl"] is not None else None,
                } for d in daily],
            })
        except Exception as e:
            result.append({"label": proj["label"], "account_id": proj["id"], "error": str(e)})
    payload = {"desde": desde, "hasta": hasta, "data": result}
    _meta_ads_cache[cache_key] = {"data": payload, "ts": time.time()}
    return jsonify(payload)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    _meta_ads_cache.clear()
    actualizar_cache()
    return jsonify({"ok": True, "updated_at": _cache["updated_at"]})


def _extraer_texto(msg):
    # Con Sonnet 5 (y en general con thinking activo) content[0] suele ser un
    # ThinkingBlock, no el TextBlock con la respuesta final — hay que buscarlo.
    for block in msg.content:
        if block.type == "text":
            return block.text
    return ""


@app.route("/api/analizar_ia", methods=["POST"])
def api_analizar_ia():
    try:
        import anthropic
        data = request.get_json(force=True)
        panel     = data.get("panel","director")
        periodo_a = data.get("periodo_a","")
        periodo_b = data.get("periodo_b")
        ventas_a  = data.get("ventas_a",0)
        ventas_b  = data.get("ventas_b")
        monto_a   = data.get("monto_a",0)
        proyecto  = data.get("proyecto","Todos")

        if panel == "funnel":
            datos_actual   = data.get("datos_actual")
            datos_anterior = data.get("datos_anterior")

            # Compatibilidad con payloads antiguos (sin datos_actual estructurado)
            if not datos_actual:
                datos_actual = {
                    "leads_totales": data.get("leads_total", 0),
                    "leads_con_dni": data.get("leads_dni", 0),
                    "leads_digitales": data.get("leads_digital", 0),
                    "contactados": data.get("prospectos", 0),
                    "visitas": data.get("visitas", 0),
                    "separaciones": data.get("separaciones", 0),
                    "ventas": data.get("ventas", 0),
                    "tiempo_respuesta_mediana_min": data.get("tiempo_respuesta_median"),
                    "canal_lider": data.get("canal_top", "—"),
                    "asesor_destacado": data.get("asesor_top", "—"),
                }

            tiene_comparacion = bool(periodo_b) and bool(datos_anterior)

            datos_actual_json = json.dumps(datos_actual, ensure_ascii=False, indent=2)
            bloque_anterior = (
                f"\n\nDATOS DEL PERÍODO ANTERIOR ({periodo_b}):\n"
                f"{json.dumps(datos_anterior, ensure_ascii=False, indent=2)}"
                if tiene_comparacion else ""
            )
            comparacion_txt = (
                f", comparado con el período anterior {periodo_b}"
                if tiene_comparacion else
                " (no hay período de comparación activo: basa la severidad de las alertas en "
                "benchmarks razonables del sector inmobiliario, ej. dropout >80% en una etapa es crítico)"
            )

            prompt = f"""Eres un consultor senior de marketing y ventas inmobiliario. Te paso el panorama completo de Marketing del período {periodo_a} para el proyecto {proyecto}{comparacion_txt}:

DATOS DEL PERÍODO ACTUAL ({periodo_a}):
{datos_actual_json}{bloque_anterior}

Genera el análisis en este formato exacto:

## 🚨 ALERTAS CRÍTICAS
Lista cada problema real (no genérico) con:
- Qué está pasando (con el número exacto)
- Por qué es grave (impacto en el negocio)
- Severidad: 🔴 Crítico / 🟡 Atención / 🟢 Monitorear
Ordena de más a menos grave. Si no hay comparación previa, basa la severidad en benchmarks razonables del sector inmobiliario (ej. dropout >80% en una etapa es crítico).

## 📈 QUÉ MEJORÓ O FUNCIONA BIEN
Solo lo que tenga evidencia numérica clara, con el dato que lo respalda.

## 🔍 DIAGNÓSTICO CRUZADO
Conecta al menos 2 pestañas distintas para explicar una causa-efecto (ej: canal X trae volumen pero Y% es de baja calidad, o gasto en campaña Z subió pero CPL empeoró).

## ✅ PLAN DE ACCIÓN
Máximo 3 acciones, cada una con:
- Qué hacer específicamente (no genérico tipo 'mejorar seguimiento')
- Quién debería ejecutarlo si el dato lo permite (ej. asesor, canal)
- Impacto esperado si se corrige

Sé directo, usa los números reales, nunca inventes causas sin dato que las respalde. Si falta información para confirmar una hipótesis, dilo explícitamente en vez de asumir. Cada fila de datos (ej. un mes en evolucion_mensual, o una entrada de por_proyecto) tiene varios campos numéricos seguidos (leads, visitas, separaciones, ventas): antes de citar un número, verifica el NOMBRE exacto del campo — nunca lo asumas por su posición en la fila. Máximo 350 palabras total."""
        else:
            comp_txt = ""
            if periodo_b and ventas_b is not None:
                delta = ventas_a - ventas_b
                comp_txt = f"\nComparación: Período B ({periodo_b}): {ventas_b} ventas. Δ = {'+' if delta>=0 else ''}{delta} unidades."
            prompt = f"""Eres analista comercial inmobiliario. Analiza los siguientes datos del dashboard y entrega un resumen ejecutivo conciso en español.

Panel: {panel.upper()}
Proyecto: {proyecto}
Período A: {periodo_a} → {ventas_a} ventas · S/ {monto_a:,}{comp_txt}

Entrega:
1. Observación principal (1-2 oraciones)
2. Punto positivo destacado
3. Riesgo o alerta (si aplica)
4. Recomendación concreta

Sé directo y práctico. Máximo 150 palabras."""

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY",""))
        create_kwargs = dict(
            # El panel funnel maneja JSON denso (varios campos numéricos por fila,
            # ej. leads/visitas/separaciones/ventas) donde Haiku puede confundir
            # columnas adyacentes; Sonnet es más confiable citando el campo correcto.
            model="claude-sonnet-5" if panel == "funnel" else "claude-haiku-4-5-20251001",
            max_tokens=1600 if panel == "funnel" else 500,
            messages=[{"role":"user","content":prompt}]
        )
        if panel == "funnel":
            create_kwargs["output_config"] = {"effort": "medium"}
        msg = client.messages.create(**create_kwargs)
        return jsonify({"analisis": _extraer_texto(msg)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat_ia", methods=["POST"])
def api_chat_ia():
    try:
        import anthropic
        data       = request.get_json(force=True)
        proyecto   = data.get("proyecto", "Todos")
        periodo_a  = data.get("periodo_a", "")
        periodo_b  = data.get("periodo_b")
        datos_actual   = data.get("datos_actual")
        datos_anterior = data.get("datos_anterior")
        analisis_previo          = data.get("analisis_previo")
        analisis_previo_periodo  = data.get("analisis_previo_periodo")
        history  = data.get("history", [])
        question = data.get("question", "")

        # Compatibilidad con el formato viejo (contexto en texto plano)
        context_legacy = data.get("context", "")
        if not datos_actual and context_legacy:
            datos_actual = {"resumen": context_legacy}

        tiene_comparacion = bool(periodo_b) and bool(datos_anterior)

        datos_actual_json = json.dumps(datos_actual or {}, ensure_ascii=False, indent=2)
        bloque_anterior = (
            f"\n\nDATOS DEL PERÍODO ANTERIOR ({periodo_b}):\n"
            f"{json.dumps(datos_anterior, ensure_ascii=False, indent=2)}"
            if tiene_comparacion else ""
        )
        bloque_analisis_previo = (
            f"\n\nANÁLISIS PREVIO GENERADO EN ESTA SESIÓN (período: {analisis_previo_periodo or periodo_a}):\n"
            f"{analisis_previo}"
            if analisis_previo else ""
        )

        system = f"""Eres el mismo consultor senior de marketing y ventas inmobiliario. Responde la pregunta del usuario usando SOLO los datos reales del contexto que tienes (las 5 pestañas + comparación si existe). Si el usuario pregunta algo que los datos no pueden responder, dilo explícitamente en vez de inventar. Da respuestas concretas con números, no genéricas. Si es relevante, sugiere una acción concreta al final. Responde en español, directo, sin relleno.

Antes de citar una cifra, ubica exactamente en qué nivel del JSON está (agregado de "Todos los proyectos" vs. una entrada específica dentro de por_proyecto) y usa ese número tal cual — nunca mezcles el agregado de todos los proyectos con el de un proyecto puntual, y nunca repitas un número distinto al que diste antes para la misma pregunta sin explicar por qué cambió.

Cada fila de datos (ej. un mes en evolucion_mensual) tiene varios campos numéricos seguidos (leads, visitas, separaciones, ventas): antes de responder, verifica el NOMBRE exacto del campo que te preguntan y usa su valor — nunca asumas un número por su posición en la fila ni por cercanía con otro campo.

Proyecto: {proyecto} | Período: {periodo_a}

DATOS DEL PERÍODO ACTUAL ({periodo_a}):
{datos_actual_json}{bloque_anterior}{bloque_analisis_previo}"""

        # Historial completo de la sesión (tope alto solo como salvaguarda ante
        # una conversación anormalmente larga, no como recorte funcional).
        messages = [{"role": h["role"], "content": h["content"]} for h in history[-40:]]
        messages.append({"role": "user", "content": question})

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY",""))
        msg = client.messages.create(
            # Mismo motivo que en /api/analizar_ia: el JSON trae varios campos
            # numéricos por fila y Sonnet es más confiable citando el correcto.
            model="claude-sonnet-5",
            max_tokens=700,
            system=system,
            messages=messages,
            output_config={"effort": "medium"},
        )
        return jsonify({"reply": _extraer_texto(msg)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════
# ARRANQUE
# ══════════════════════════════════════════════════════════════

import threading
threading.Thread(target=actualizar_cache, daemon=True).start()

scheduler = BackgroundScheduler(timezone=LIMA)
scheduler.add_job(actualizar_cache, "interval", hours=1)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)