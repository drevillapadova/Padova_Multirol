"""
Configuración del pricing por proyecto, editable desde el dashboard.

Reemplaza a la pestaña PRECIOS del Sheet: plan de tramos (etapa, unidades,
ritmo, S/ m²), recaudación requerida, ticket del plan, descuento esperado,
tramo por obra (con historial) y umbrales de alerta.

Se guarda en un archivo JSON. En Railway la ruta debe apuntar a un volumen
persistente (variable PRICING_CONFIG_PATH, p. ej. /data/pricing_config.json);
si no, los cambios se pierden en cada despliegue y se vuelve a DEFAULTS.
"""
import copy
import json
import os
import threading
from datetime import datetime

import pytz

LIMA = pytz.timezone("America/Lima")
RUTA = os.getenv("PRICING_CONFIG_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "pricing_config.json")
_lock = threading.Lock()

# Valores de partida: los que tenía la pestaña PRECIOS al 16-nov-2026.
DEFAULTS = {
    "DOMINGO ORUE": {
        "requerida": 102367500.0,
        "tc": 3.45,
        "unidades_plan": 340,
        "ticket_plan": 301081.0,
        "area_plan": 13649.0,
        "dscto_esperado": 0.07,
        "cupo_cuenta_separaciones": False,
        "redondeo_lista": 100,
        "alerta_cupo_pct": 0.85,
        "alerta_pm2_bajo_plan": 0.02,
        "alerta_ritmo_pct": 0.70,
        "alerta_ticket_pct": 0.10,
        "alerta_lista_pct": 0.005,
        "dscto_maximo": 0.10,
        "tramos": [
            {"n": 1, "etapa": "Lanzamiento",       "unidades": 30, "velocidad": 6,  "meses": 5, "pm2": 6700},
            {"n": 2, "etapa": "En planos",         "unidades": 50, "velocidad": 10, "meses": 5, "pm2": 7070},
            {"n": 3, "etapa": "Excavación",        "unidades": 50, "velocidad": 8,  "meses": 6, "pm2": 7200},
            {"n": 4, "etapa": "Casco",             "unidades": 70, "velocidad": 8,  "meses": 9, "pm2": 7500},
            {"n": 5, "etapa": "En construcción",   "unidades": 80, "velocidad": 9,  "meses": 9, "pm2": 7650},
            {"n": 6, "etapa": "Entrega inmediata", "unidades": 60, "velocidad": 10, "meses": 6, "pm2": 7800},
        ],
        # Historial del tramo por obra: se agrega una entrada cada vez que el
        # equipo cambia la fase desde el dashboard.
        "obra": [{"tramo": 2, "desde": "2026-11-16"}],
        "cambios": [],
    },
    # Sunny: correo de Finanzas del 23-nov-2026 (tramos en US$/m² × TC 3.45,
    # ventas en dólares al TC del presupuesto, ratio flat vs dúplex/tríplex 6 a 1). Confirmado: descuento
    # esperado 10%, obra en excavación (T2) y ritmo promedio de 3 ventas al mes
    # (los meses de cada tramo salen de unidades ÷ 3).
    "SUNNY": {
        # Meta del correo de Finanzas (con IGV): 13,303 m² × S/ 7,087 = 94,283,668
        "requerida": 94283668.0,
        "unidades_plan": 110,
        "ticket_plan": 857124.0,
        "area_plan": 13303.0,
        "tc": 3.45,
        "ventas_usd_tc_presupuesto": True,
        "dscto_esperado": 0.10,
        "cupo_cuenta_separaciones": False,
        "redondeo_lista": 100,
        "alerta_cupo_pct": 0.85,
        "alerta_pm2_bajo_plan": 0.02,
        "alerta_ritmo_pct": 0.70,
        "alerta_ticket_pct": 0.10,
        "alerta_lista_pct": 0.005,
        "dscto_maximo": 0.13,
        "tramos": [
            {"n": 1, "etapa": "Preventa",       "unidades": 30, "velocidad": 3, "meses": 10, "pm2": 6555},
            {"n": 2, "etapa": "Excavación",     "unidades": 30, "velocidad": 3, "meses": 10, "pm2": 6900},
            {"n": 3, "etapa": "Medio casco",    "unidades": 20, "velocidad": 3, "meses": 7,  "pm2": 7245},
            {"n": 4, "etapa": "Casco completo", "unidades": 30, "velocidad": 3, "meses": 10, "pm2": 7590},
        ],
        "obra": [{"tramo": 2, "desde": "2026-11-23"}],
        "cambios": [],
    },
}

CAMPOS_NUM = {
    "requerida": (0, None), "unidades_plan": (0, None), "ticket_plan": (0, None), "area_plan": (0, None),
    "dscto_esperado": (0, 0.5), "redondeo_lista": (1, 100000),
    "alerta_cupo_pct": (0, 1), "alerta_pm2_bajo_plan": (0, 1), "alerta_ritmo_pct": (0, 1), "alerta_ticket_pct": (0, 1),
    "alerta_lista_pct": (0, 1), "dscto_maximo": (0, 0.6), "tc": (1, 10),
}


def _ahora():
    return datetime.now(LIMA)


def _leer_archivo():
    try:
        with open(RUTA, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"   !! pricing_config: no se pudo leer {RUTA}: {e}")
        return {}


def _escribir_archivo(data):
    os.makedirs(os.path.dirname(RUTA), exist_ok=True)
    tmp = RUTA + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, RUTA)


def proyectos():
    return sorted(set(DEFAULTS) | set(_leer_archivo()))


def cargar(proyecto):
    """Configuración del proyecto (archivo; si no existe, DEFAULTS)."""
    proyecto = proyecto.upper().strip()
    with _lock:
        data = _leer_archivo()
    cfg = data.get(proyecto)
    base = copy.deepcopy(DEFAULTS.get(proyecto))
    if cfg is None:
        return base
    if base:
        # Campos agregados después de que se guardó el archivo: valor por defecto
        for k, v in base.items():
            cfg.setdefault(k, v)
    return cfg


def guardar(proyecto, nueva):
    """Valida y guarda. Devuelve (cfg_guardada, error)."""
    proyecto = proyecto.upper().strip()
    actual = cargar(proyecto) or {}
    try:
        cfg = _validar(nueva, actual)
    except ValueError as e:
        return None, str(e)
    # Cambio de tramo por obra: se registra con la fecha de hoy.
    hoy = _ahora().strftime("%Y-%m-%d")
    obra_nueva = nueva.get("tramo_obra")
    obra_hist = list(actual.get("obra", []))
    obra_act = obra_hist[-1]["tramo"] if obra_hist else None
    if obra_nueva not in (None, "", obra_act):
        n = int(obra_nueva)
        if not any(t["n"] == n for t in cfg["tramos"]):
            return None, f"El tramo por obra {n} no existe en el plan"
        obra_hist.append({"tramo": n, "desde": hoy})
    if obra_hist and obra_hist[-1]["tramo"] > len(cfg["tramos"]):
        return None, f"La obra está en el tramo {obra_hist[-1]['tramo']} y el plan solo tiene {len(cfg['tramos'])}"
    cfg["obra"] = obra_hist
    resumen = _diferencias(actual, cfg)
    cfg["cambios"] = (actual.get("cambios", []) + ([{"fecha": _ahora().strftime("%Y-%m-%d %H:%M"), "detalle": resumen}] if resumen else []))[-50:]
    cfg["actualizado"] = _ahora().strftime("%Y-%m-%d %H:%M")
    with _lock:
        data = _leer_archivo()
        data[proyecto] = cfg
        _escribir_archivo(data)
    return cfg, None


def _validar(nueva, actual):
    cfg = copy.deepcopy(actual)
    for k, (lo, hi) in CAMPOS_NUM.items():
        if k in nueva:
            try:
                v = float(nueva[k])
            except (TypeError, ValueError):
                raise ValueError(f"Valor no numérico en {k}")
            if v < lo or (hi is not None and v > hi):
                raise ValueError(f"{k} fuera de rango")
            cfg[k] = int(v) if k in ("unidades_plan", "redondeo_lista") else v
    for k in ("cupo_cuenta_separaciones", "ventas_usd_tc_presupuesto"):
        if k in nueva:
            cfg[k] = bool(nueva[k])
    if "tramos" in nueva:
        tramos = []
        for i, t in enumerate(nueva["tramos"], start=1):
            try:
                und = int(float(t.get("unidades")))
                pm2 = float(t.get("pm2"))
                vel = float(t.get("velocidad") or 0)
                meses = float(t.get("meses") or 0)
            except (TypeError, ValueError):
                raise ValueError(f"Tramo {i}: unidades, ritmo, meses y S/ m² deben ser números")
            if und <= 0 or pm2 <= 0 or vel < 0 or meses < 0:
                raise ValueError(f"Tramo {i}: unidades y S/ m² deben ser mayores que cero")
            tramos.append({"n": i, "etapa": str(t.get("etapa") or f"Tramo {i}").strip()[:40],
                           "unidades": und, "velocidad": vel, "meses": meses, "pm2": pm2})
        if not tramos:
            raise ValueError("El plan necesita al menos un tramo")
        cfg["tramos"] = tramos
    return cfg


def _diferencias(a, b):
    """Texto corto de qué cambió (para el historial de cambios)."""
    out = []
    for k in list(CAMPOS_NUM) + ["cupo_cuenta_separaciones", "ventas_usd_tc_presupuesto"]:
        if a.get(k) != b.get(k):
            out.append(f"{k}: {a.get(k)} → {b.get(k)}")
    if a.get("tramos") != b.get("tramos"):
        out.append("plan de tramos")
    if (a.get("obra") or [{}])[-1].get("tramo") != (b.get("obra") or [{}])[-1].get("tramo"):
        out.append(f"tramo por obra → {b['obra'][-1]['tramo']}")
    return "; ".join(out)


def a_plan(cfg):
    """Convierte la configuración al `plan` que usa pricing.calcular_pricing."""
    tramos, acum = [], 0
    for t in cfg["tramos"]:
        tramos.append({**t, "fechas": "", "desde": acum + 1, "hasta": acum + t["unidades"]})
        acum += t["unidades"]
    return {"requerida": cfg["requerida"], "unidades_plan": cfg.get("unidades_plan") or acum,
            "ticket_plan": cfg["ticket_plan"], "area_plan": cfg.get("area_plan", 0),
            "tramos": tramos, "obra": cfg.get("obra", [])}


def a_params(cfg):
    """Parámetros de cálculo que sobrescriben pricing.PARAMS."""
    return {k: cfg[k] for k in ("dscto_esperado", "cupo_cuenta_separaciones", "redondeo_lista",
                                "alerta_cupo_pct", "alerta_pm2_bajo_plan", "alerta_ritmo_pct",
                                "alerta_ticket_pct", "alerta_lista_pct", "dscto_maximo", "tc",
                                "ventas_usd_tc_presupuesto") if k in cfg}
