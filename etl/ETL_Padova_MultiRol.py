import time
import os
import glob
import shutil
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials as ServiceCredentials
import smtplib
import traceback
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# TIPO DE CAMBIO - BCRP
# ============================================================

_TC_CACHE = {}
_TC_BCRP_CARGADO = False

def precargar_tc_bcrp(fecha_inicio="2024-01-01"):
    """Carga todos los TCs desde fecha_inicio hasta hoy en UNA sola llamada."""
    global _TC_BCRP_CARGADO
    if _TC_BCRP_CARGADO:
        return
    TC_RESPALDO = 3.75
    fecha_fin = datetime.now().strftime("%Y-%m-%d")
    try:
        url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha_inicio}/{fecha_fin}/ing"
        r = requests.get(url, timeout=15)
        if not r.text.strip():
            raise ValueError("Respuesta vacía del BCRP")
        data = r.json()
        periodos = data.get("periods", [])
        ultimo_tc = TC_RESPALDO
        for p in periodos:
            try:
                fecha_p = p.get("name", "")
                tc_val  = float(p["values"][0])
                # BCRP devuelve fechas como "22-Ene-24", normalizar a YYYY-MM-DD
                fecha_dt = datetime.strptime(fecha_p, "%d-%b-%y") if "-" in fecha_p else None
                if fecha_dt:
                    _TC_CACHE[fecha_dt.strftime("%Y-%m-%d")] = tc_val
                    ultimo_tc = tc_val
            except Exception:
                pass
        print(f"   -> [TC] BCRP precargado: {len(_TC_CACHE)} fechas, último TC: S/ {ultimo_tc}")
    except Exception as e:
        print(f"   -> [TC] BCRP no disponible ({e}), usando respaldo S/ {TC_RESPALDO} para todas las fechas")
    _TC_BCRP_CARGADO = True


def get_tipo_cambio(fecha=None):
    TC_RESPALDO = 3.75
    import pandas as _pd
    if fecha is None or (hasattr(_pd, 'isnull') and _pd.isnull(fecha)):
        fecha_dt = datetime.now()
    elif isinstance(fecha, str):
        try: fecha_dt = datetime.strptime(fecha[:10], "%Y-%m-%d")
        except: fecha_dt = datetime.now()
    elif hasattr(fecha, 'strftime'):
        try: fecha_dt = fecha.to_pydatetime() if hasattr(fecha, 'to_pydatetime') else fecha
        except: fecha_dt = datetime.now()
    else:
        fecha_dt = datetime.now()

    for dias in range(0, 8):
        key = (fecha_dt - timedelta(days=dias)).strftime("%Y-%m-%d")
        if key in _TC_CACHE:
            return _TC_CACHE[key]

    _TC_CACHE[fecha_dt.strftime("%Y-%m-%d")] = TC_RESPALDO
    return TC_RESPALDO


def convertir_precios_a_soles(df, col_precio, col_moneda, tc=None, col_fecha=None):
    df = df.copy()
    convertidos = 0
    precios_soles = []
    for idx, row in df.iterrows():
        try: precio = float(str(row[col_precio]).replace(",", "")) if row[col_precio] else 0
        except: precio = 0
        moneda = str(row[col_moneda]).upper().strip()
        es_usd = "DOLAR" in moneda or "USD" in moneda
        if es_usd:
            if col_fecha and col_fecha in df.columns:
                tc_usar = get_tipo_cambio(row[col_fecha])
            elif tc is not None: tc_usar = tc
            else: tc_usar = get_tipo_cambio()
            precios_soles.append(round(precio * tc_usar, 2))
            convertidos += 1
        else:
            precios_soles.append(round(precio, 2))
    df["PrecioVentaSoles"] = precios_soles
    print(f"   -> [TC] {len(df)-convertidos} en soles + {convertidos} en dólares convertidos")
    return df


def corregir_moneda_con_stock(df_ventas, df_stock):
    if df_stock is None or len(df_stock) == 0: return df_ventas
    df_stock = df_stock.copy()
    df_stock.columns = df_stock.columns.str.strip()
    col_proy_s   = next((c for c in df_stock.columns if c.strip() == 'Proyecto'), None)
    col_nro_s    = next((c for c in df_stock.columns if c.strip() == 'NroInmuebleActual'), None) \
                or next((c for c in df_stock.columns if 'NroInmueble' in c), None)
    col_moneda_s = next((c for c in df_stock.columns if c.strip() == 'Moneda'), None)
    if not col_proy_s or not col_nro_s or not col_moneda_s: return df_ventas
    def norm_nro(v):
        s = str(v).strip()
        if s.endswith('.0'): s = s[:-2]
        return s.upper()
    lookup = {}
    for _, row in df_stock.iterrows():
        proy = str(row[col_proy_s]).strip().upper()
        nro  = norm_nro(row[col_nro_s])
        mon  = str(row[col_moneda_s]).strip().upper()
        if proy and nro and nro not in ('', 'NAN', 'NONE'):
            lookup[(proy, nro)] = mon
    print(f"   -> [MONEDA] Lookup stock construido: {len(lookup)} unidades")
    col_proy_v   = 'Proyecto'    if 'Proyecto'    in df_ventas.columns else None
    col_nro_v    = 'NroInmueble' if 'NroInmueble' in df_ventas.columns else None
    col_moneda_v = 'TipoMoneda'  if 'TipoMoneda'  in df_ventas.columns else None
    if not col_proy_v or not col_nro_v or not col_moneda_v: return df_ventas
    df_ventas = df_ventas.copy()
    corregidos = 0
    for idx, row in df_ventas.iterrows():
        moneda_v = str(row[col_moneda_v]).upper().strip()
        if 'DOLAR' not in moneda_v and 'USD' not in moneda_v: continue
        proy_v = str(row[col_proy_v]).strip().upper()
        nro_v  = norm_nro(row[col_nro_v])
        moneda_stock = lookup.get((proy_v, nro_v))
        if moneda_stock and 'DOLAR' not in moneda_stock and 'USD' not in moneda_stock:
            df_ventas.at[idx, col_moneda_v] = moneda_stock
            corregidos += 1
    print(f"   -> [MONEDA] Total corregidos: {corregidos} registros")
    return df_ventas


def corregir_moneda_sunny(df, col_precio='PrecioVenta', col_moneda='TipoMoneda', col_proyecto='Proyecto'):
    UMBRAL = 600_000
    cols_ok = all(c in df.columns for c in [col_precio, col_moneda, col_proyecto])
    if not cols_ok: return df
    df = df.copy()
    corregidos = 0
    for idx, row in df.iterrows():
        if 'SUNNY' not in str(row[col_proyecto]).upper(): continue
        moneda = str(row[col_moneda]).upper().strip()
        if 'DOLAR' in moneda or 'USD' in moneda: continue
        try: precio = float(str(row[col_precio]).replace(',', '')) if row[col_precio] else 0
        except: precio = 0
        if 0 < precio < UMBRAL:
            df.at[idx, col_moneda] = 'DOLAR'
            corregidos += 1
    print(f"   -> [MONEDA] Sunny: {corregidos} registros corregidos a DOLAR")
    return df


# ============================================================
# CONFIGURACIÓN
# ============================================================

USER_CRED  = os.environ.get("EVOLTA_USER", "calopez")
PASS_CRED  = os.environ.get("EVOLTA_PASS", "")
EMAIL_FROM = "sistema.padova@gmail.com"
EMAIL_TO   = "yleon@padovasac.com, carrunategui@constructorapadova.pe"
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")

# URLs Evolta
URL_LOGIN              = "https://v4.evolta.pe/Login/Acceso/Index"
URL_REPORTE_STOCK      = "https://v4.evolta.pe/Reportes/RepCargaStock/IndexNuevoRepStock"
URL_REPORTE_VENTAS     = "https://v4.evolta.pe/Reportes/RepVenta/Index"
URL_REPORTE_PROSPECTOS = "https://v4.evolta.pe/Reportes/RepHiloProspectos/IndexProspecto"  # NUEVO
URL_REPORTE_VISITAS    = "https://v4.evolta.pe/Reportes/RepVisita/IndexVisita"              # NUEVO

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900', 'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO', 'DOMINGO ORUE'
]

IS_CLOUD = os.name != 'nt'

# Directorios
if IS_CLOUD:
    DOWNLOAD_DIR            = "/tmp/evolta_stock"
    DOWNLOAD_DIR_VENTAS     = "/tmp/evolta_ventas"
    DOWNLOAD_DIR_PROSPECTOS = "/tmp/evolta_prospectos"   # NUEVO
    DOWNLOAD_DIR_VISITAS    = "/tmp/evolta_visitas"      # NUEVO
else:
    DOWNLOAD_DIR            = r"C:\Users\MKT\Documents\EVOLTA\descargas_stock"
    DOWNLOAD_DIR_VENTAS     = r"C:\Users\MKT\Documents\EVOLTA\descargas_ventas"
    DOWNLOAD_DIR_PROSPECTOS = r"C:\Users\MKT\Documents\EVOLTA\descargas_prospectos"  # NUEVO
    DOWNLOAD_DIR_VISITAS    = r"C:\Users\MKT\Documents\EVOLTA\descargas_visitas"     # NUEVO

ONEDRIVE_OUTPUT_DIR = None if IS_CLOUD else r"C:\Users\MKT\OneDrive - PADOVA SAC\PADOVA - MKT - MIRANO INMOBILIARIA - VENTAS\Dashboards"
ONEDRIVE_FILE_NAME  = "ReporteEvolta.xlsx"

# ── NUEVO Sheet ID (dashboard multi-rol) ──
GSHEETS_SPREADSHEET_ID = "1JIEEGPxJvCHvmGvVE6Zp9wBPUVXEF-iXy8FNaWr1PPI"

for dir_path in [DOWNLOAD_DIR, DOWNLOAD_DIR_VENTAS, DOWNLOAD_DIR_PROSPECTOS, DOWNLOAD_DIR_VISITAS]:
    os.makedirs(dir_path, exist_ok=True)

AÑOS_VENTAS = [2024, 2025, 2026]


def _load_gsheets_credentials():
    import base64, json, tempfile
    b64 = os.environ.get("GSHEETS_CREDENTIALS_B64", "")
    if b64:
        creds_dict = json.loads(base64.b64decode(b64).decode("utf-8"))
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(creds_dict, tmp)
        tmp.flush()
        return tmp.name
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evoltareportes-00ffe1b337be.json")
    if os.path.exists(local_path): return local_path
    raise FileNotFoundError("No se encontraron credenciales de Google.")

GSHEETS_CREDENTIALS_FILE = _load_gsheets_credentials()

# ============================================================
# COLUMNAS MAESTRAS
# ============================================================

COLUMNAS_BASE = [
    'CorrelativoOC','FechaVenta','FechaPreminuta','FechaEntrega_Minuta',
    'Fecha_Registro_Sistema','Fecha_Primera_Visita','FechaProspecto','FechaDevolucion',
    'Estado','EstadoOC','TipoDocumentoTitular','NroDocumentoTitular','NombresTitular',
    'CorreoElectronico','CorreoElectronico2','TelefonoCasa','TelefonoCelular',
    'TelefonoCelular2','Genero','Estado_Civil','Provincia_Procedencia',
    'Distrito_Procedencia','Direccion','RangoEdad','NivelInteres','ComoSeEntero',
    'FormaContacto','PerfilCrediticio','Institucion','NivelIngresos','MotivoCompra',
    'Promocion','ContenidoPromocion','ValorTotalCombo','ReferidoPor'
]

def generar_columnas_inmueble(n):
    return [f'T/M_{n}',f'TipoInmueble_{n}',f'Modelo_{n}',f'NroInmueble_{n}',
            f'NroPiso_{n}',f'Vista_{n}',f'PrecioBase_{n}',f'PrecioLista_{n}',
            f'DescuentoLista_{n}',f'TotalLista_{n}',f'PrioridadOC_{n}',f'Orden_{n}']

COLUMNAS_INMUEBLES = []
for i in range(1, 9):
    COLUMNAS_INMUEBLES.extend(generar_columnas_inmueble(i))

COLUMNAS_FINALES = [
    'CargaFamiliar','Proyecto','Etapa','SubTotal','MontoDescuento','PrecioVenta',
    'MontoSeparacion','BonoVerde','TipodeBono','MontoBono','MontoPagadoBono',
    'PorcentajePagado','EstadoBono','MontoCuotaInicial','MontoPagadoCI',
    'PorcetanjePagadoCI','Estado_CI','MontoFinanciamiento','MontoDesembolsado',
    'PorcetanjePagado_SF','EstadoSF','TipoMoneda','TipoCambio','TipoFinanciamiento',
    'EntidadFinanciamiento','Vendedor','utm_medium','utm_source','utm_campaign',
    'utm_term','utm_content','Es_Cotizador_Evolta','Es_Formulario_Evolta',
    'Es_Cotizador_y_Formulario_Evolta','Ult_Comentario','MigracionMasiva',
    'TotalCuotaInicial','TotalCuotaFinanciar','Areaterreno','TasaInteres',
    'CallCenter','TipoProceso','Puesto','IdProforma','AÑO'
]

COLUMNAS_MAESTRAS = COLUMNAS_BASE + COLUMNAS_INMUEBLES + COLUMNAS_FINALES


# ============================================================
# SELENIUM — utilidades
# ============================================================

def get_driver(download_dir):
    os.makedirs(download_dir, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--log-level=3")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    options.add_experimental_option("prefs", prefs)
    if IS_CLOUD:
        return webdriver.Chrome(options=options)
    else:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)


def clean_environment(directory, extension="*.xlsx"):
    files = glob.glob(os.path.join(directory, extension))
    for f in files:
        try: os.remove(f)
        except: pass


def dismiss_popup(driver):
    try:
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        time.sleep(1)
        driver.execute_script("document.body.click();")
        time.sleep(1)
    except Exception: pass


def robust_login(driver, wait):
    print(">> [LOGIN] Iniciando sesión...")
    driver.get(URL_LOGIN)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    try: user_field = driver.find_element(By.ID, "UserName")
    except:
        try: user_field = driver.find_element(By.NAME, "Usuario")
        except: user_field = driver.find_element(By.XPATH, "//input[@type='text']")
    user_field.clear()
    user_field.send_keys(USER_CRED)
    driver.find_element(By.XPATH, "//input[@type='password']").send_keys(PASS_CRED)
    try: driver.find_element(By.XPATH, "//button[@type='submit'] | //input[@type='submit']").click()
    except: pass
    try: wait.until(EC.url_changes(URL_LOGIN))
    except: pass
    time.sleep(2)
    dismiss_popup(driver)
    print("   -> [OK] Login exitoso")


def esperar_descarga_nueva(download_dir, existing, timeout=120):
    """Espera a que aparezca un archivo NUEVO en el directorio."""
    elapsed = 0
    while elapsed < timeout:
        current = set(glob.glob(os.path.join(download_dir, "*.*")))
        new = [f for f in current - existing
               if not f.endswith('.crdownload') and not f.endswith('.tmp')
               and os.path.getsize(f) > 0]
        if new:
            return new[0]
        if elapsed > 0 and elapsed % 30 == 0:
            print(f"   -> Esperando descarga... ({elapsed}s)")
        time.sleep(1)
        elapsed += 1
    return None


# ============================================================
# EXTRACCIÓN — STOCK
# ============================================================

def execute_stock_extraction(driver):
    print(f"\n>> [STOCK] Navegando a: {URL_REPORTE_STOCK}")
    driver.get(URL_REPORTE_STOCK)
    wait = WebDriverWait(driver, 30)
    time.sleep(3)
    dismiss_popup(driver)
    try:
        try: select_element = wait.until(EC.presence_of_element_located((By.ID, "ProyectoId")))
        except: select_element = driver.find_element(By.TAG_NAME, "select")
        select = Select(select_element)
        try: select.select_by_visible_text("Todos")
        except:
            try: select.select_by_visible_text("TODOS")
            except: select.select_by_index(0)
        time.sleep(1)
    except Exception as e:
        print(f"   !! Warning selector: {e}")

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))
    export_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnExportar")))
    driver.execute_script("arguments[0].click();", export_btn)

    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=480)
    if not archivo: raise Exception("Timeout esperando descarga de Stock")
    print(f"   -> [OK] {os.path.basename(archivo)}")
    return archivo


# ============================================================
# EXTRACCIÓN — VENTAS (por año)
# ============================================================

def execute_ventas_extraction_year(driver, wait, año):
    print(f"\n>> [VENTAS {año}] Procesando...")
    driver.get(URL_REPORTE_VENTAS)
    time.sleep(4)
    dismiss_popup(driver)

    fecha_inicio = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")

    try:
        proyecto_select = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select")))
        Select(proyecto_select).select_by_index(0)
        time.sleep(0.5)
    except Exception: pass

    driver.execute_script(f"""
        var inputs = document.querySelectorAll('input');
        var df = [];
        for(var i=0;i<inputs.length;i++){{
            var v=inputs[i].value||'';
            if(v.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) df.push(inputs[i]);
        }}
        if(df.length>=2){{
            df[0].value='{fecha_inicio}'; df[0].dispatchEvent(new Event('change',{{bubbles:true}}));
            df[1].value='{fecha_fin}';    df[1].dispatchEvent(new Event('change',{{bubbles:true}}));
        }}
    """)
    time.sleep(1)

    for xpath in ["//input[@type='radio'][@value='Csv']","//label[contains(text(),'Csv')]","//*[text()='Csv']"]:
        try:
            el = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", el)
            break
        except Exception: pass
    time.sleep(1)

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))

    for xpath in ["//button[contains(text(),'Exportar')]","//button[@id='btnExportar']","//button[@type='submit']"]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception: pass

    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=480)
    if not archivo:
        print(f"   !! Warning: no se descargó para {año}")
        return None

    ext  = os.path.splitext(archivo)[1].lower()
    dest = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}{ext}")
    os.makedirs(DOWNLOAD_DIR_VENTAS, exist_ok=True)
    if os.path.exists(dest): os.remove(dest)
    shutil.move(archivo, dest)
    print(f"   -> [OK] ReporteVenta{año}{ext}")
    return dest


def execute_ventas_extraction(driver):
    print("\n" + "="*60)
    print(">> [VENTAS] Iniciando descarga por año")
    print("="*60)
    wait = WebDriverWait(driver, 30)
    archivos = {}
    for año in AÑOS_VENTAS:
        try:
            archivos[str(año)] = execute_ventas_extraction_year(driver, wait, año)
            time.sleep(2)
        except Exception as e:
            print(f"   !! Error año {año}: {e}")
    return archivos


# ============================================================
# EXTRACCIÓN — PROSPECTOS (NUEVO)
# ============================================================

def _set_fechas_js(driver, fecha_ini, fecha_fin):
    """Sobreescribe los inputs de fecha dd/mm/yyyy en la página actual."""
    driver.execute_script(f"""
        (function(){{
            var fi='{fecha_ini}', ff='{fecha_fin}';
            var candidates=Array.from(document.querySelectorAll('input')).filter(function(i){{
                return i.value && i.value.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/);
            }});
            if(candidates.length<2){{
                candidates=Array.from(document.querySelectorAll('input')).filter(function(i){{
                    var n=(i.name+i.id+(i.placeholder||'')).toLowerCase();
                    return n.includes('fecha')||n.includes('date');
                }});
            }}
            if(candidates.length>=2){{
                candidates[0].value=fi;
                candidates[0].dispatchEvent(new Event('change',{{bubbles:true}}));
                candidates[0].dispatchEvent(new Event('input',{{bubbles:true}}));
                candidates[candidates.length-1].value=ff;
                candidates[candidates.length-1].dispatchEvent(new Event('change',{{bubbles:true}}));
                candidates[candidates.length-1].dispatchEvent(new Event('input',{{bubbles:true}}));
            }}
        }})();
    """)
    time.sleep(1)


def execute_prospectos_extraction_year(driver, wait, año):
    print(f"\n>> [PROSPECTOS {año}] Procesando...")
    driver.get(URL_REPORTE_PROSPECTOS)
    time.sleep(4)
    dismiss_popup(driver)

    try:
        sels = driver.find_elements(By.TAG_NAME, "select")
        if sels: Select(sels[0]).select_by_index(0)
        time.sleep(0.5)
    except Exception: pass

    fecha_ini = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")
    _set_fechas_js(driver, fecha_ini, fecha_fin)

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))

    for selector in [(By.ID,"btnExportar"),(By.XPATH,"//button[contains(text(),'Exportar')]"),(By.XPATH,"//button[@type='submit']")]:
        try:
            btn = wait.until(EC.element_to_be_clickable(selector))
            driver.execute_script("arguments[0].click();", btn)
            print("   -> Click en Exportar")
            break
        except Exception: pass

    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=240)
    if not archivo:
        print(f"   !! Warning: no se descargó prospectos {año}")
        return None

    ext  = os.path.splitext(archivo)[1].lower()
    os.makedirs(DOWNLOAD_DIR_PROSPECTOS, exist_ok=True)
    dest = os.path.join(DOWNLOAD_DIR_PROSPECTOS, f"ReporteProspectos{año}{ext}")
    if os.path.exists(dest): os.remove(dest)
    shutil.move(archivo, dest)
    print(f"   -> [OK] {os.path.basename(dest)}")
    return dest


def execute_prospectos_extraction(driver, wait):
    print("\n" + "="*60)
    print(">> [PROSPECTOS] Iniciando descarga por año")
    print("="*60)
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_PROSPECTOS, "*.*")):
        try: os.remove(f)
        except: pass
    archivos = {}
    for año in AÑOS_VENTAS:
        try:
            archivos[str(año)] = execute_prospectos_extraction_year(driver, wait, año)
            time.sleep(2)
        except Exception as e:
            print(f"   !! Error prospectos {año}: {e}")
    return archivos


# ============================================================
# EXTRACCIÓN — VISITAS
# ============================================================

def execute_visitas_extraction_year(driver, wait, año):
    print(f"\n>> [VISITAS {año}] Procesando...")
    driver.get(URL_REPORTE_VISITAS)
    time.sleep(4)
    dismiss_popup(driver)

    try:
        sels = driver.find_elements(By.TAG_NAME, "select")
        if sels: Select(sels[0]).select_by_index(0)
        time.sleep(0.5)
    except Exception: pass

    # Visitas tiene Excel por defecto — cambiar a CSV
    for xpath in ["//input[@type='radio'][@value='Csv']", "//label[contains(text(),'Csv')]", "//*[text()='Csv']"]:
        try:
            el = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", el)
            break
        except Exception: pass
    time.sleep(0.5)

    fecha_ini = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")
    _set_fechas_js(driver, fecha_ini, fecha_fin)

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))

    for selector in [(By.ID,"btnExportar"),(By.XPATH,"//button[contains(text(),'Exportar')]"),(By.XPATH,"//button[@type='submit']")]:
        try:
            btn = wait.until(EC.element_to_be_clickable(selector))
            driver.execute_script("arguments[0].click();", btn)
            print("   -> Click en Exportar")
            break
        except Exception: pass

    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=240)
    if not archivo:
        print(f"   !! Warning: no se descargó visitas {año}")
        return None

    ext  = os.path.splitext(archivo)[1].lower()
    os.makedirs(DOWNLOAD_DIR_VISITAS, exist_ok=True)
    dest = os.path.join(DOWNLOAD_DIR_VISITAS, f"ReporteVisitas{año}{ext}")
    if os.path.exists(dest): os.remove(dest)
    shutil.move(archivo, dest)
    print(f"   -> [OK] {os.path.basename(dest)}")
    return dest


def execute_visitas_extraction(driver, wait):
    print("\n" + "="*60)
    print(">> [VISITAS] Iniciando descarga por año")
    print("="*60)
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_VISITAS, "*.*")):
        try: os.remove(f)
        except: pass
    archivos = {}
    for año in AÑOS_VENTAS:
        try:
            archivos[str(año)] = execute_visitas_extraction_year(driver, wait, año)
            time.sleep(2)
        except Exception as e:
            print(f"   !! Error visitas {año}: {e}")
    return archivos


# ============================================================
# TRANSFORMACIÓN — VENTAS
# ============================================================

def normalizar_dataframe(df, año):
    df_norm = df.copy()
    for col in COLUMNAS_MAESTRAS:
        if col not in df_norm.columns: df_norm[col] = pd.NA
    df_norm['AÑO'] = int(año)
    return df_norm[COLUMNAS_MAESTRAS]


def normalizar_ventas_unpivot(df):
    print("   > Iniciando unpivot...")
    stubs = ['T/M','TipoInmueble','Modelo','NroInmueble','NroPiso','Vista',
             'PrecioBase','PrecioLista','DescuentoLista','TotalLista','PrioridadOC','Orden']
    if 'IdProforma' not in df.columns:
        print("   [Warning] Sin IdProforma, saltando unpivot")
        return df
    df = df.copy()
    df['_idx_temp'] = range(len(df))
    try:
        df_long = pd.wide_to_long(df, stubnames=stubs, i=['_idx_temp'],
                                  j='Indice_Inmueble', sep='_', suffix=r'\d+').reset_index()
        df_long = df_long.drop(columns=['_idx_temp'])
        df_long = df_long.dropna(subset=['TipoInmueble'])
        df_long = df_long[df_long['TipoInmueble'] != '']
        print(f"   > Filas finales: {len(df_long):,}")
        return df_long
    except Exception as e:
        print(f"   [Error] Unpivot falló: {e}")
        return df


def process_ventas_data(df_stock=None):
    print("\n>> [TRANSFORM VENTAS] Consolidando...")
    precargar_tc_bcrp("2024-01-01")
    dataframes = {}
    for año in AÑOS_VENTAS:
        ruta = None
        for ext in ['.csv', '.xlsx']:
            r = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}{ext}")
            if os.path.exists(r): ruta = r; break
        if not ruta: continue
        try:
            df = pd.read_csv(ruta, encoding='utf-8', low_memory=False) if ruta.endswith('.csv') else pd.read_excel(ruta)
            dataframes[str(año)] = df
            print(f"   -> {año}: {len(df):,} filas")
        except Exception as e:
            print(f"   !! Error {año}: {e}")

    if not dataframes: return None

    dfs_norm = {a: normalizar_dataframe(df, a) for a, df in dataframes.items()}
    df = pd.concat(dfs_norm.values(), ignore_index=True)
    df = normalizar_ventas_unpivot(df)

    if "TotalLista" in df.columns:
        try:
            mask = df["TotalLista"].notna() & (df["TotalLista"] != 0) & (df["TotalLista"] != "")
            df.loc[mask, "PrecioVenta"] = pd.to_numeric(df.loc[mask, "TotalLista"], errors="coerce")
        except Exception as e:
            print(f"   [Warning] TotalLista: {e}")

    if df_stock is not None and "TipoMoneda" in df.columns:
        df = corregir_moneda_con_stock(df, df_stock)

    if "TipoMoneda" in df.columns:
        df = corregir_moneda_sunny(df, col_moneda='TipoMoneda')

    if "PrecioVenta" in df.columns and "TipoMoneda" in df.columns:
        col_fecha = "FechaVenta" if "FechaVenta" in df.columns else \
                    "FechaEntrega_Minuta" if "FechaEntrega_Minuta" in df.columns else None
        df = convertir_precios_a_soles(df, "PrecioVenta", "TipoMoneda", col_fecha=col_fecha)

    print(f"   -> Total: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — STOCK
# ============================================================

def process_stock_data(df_ventas=None):
    print("\n>> [TRANSFORM STOCK] Procesando...")
    list_of_files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsx'))
    if not list_of_files: raise Exception("No se encontró archivo de stock")
    latest_file = max(list_of_files, key=os.path.getctime)
    df = pd.read_excel(latest_file)
    df.columns = df.columns.str.strip()
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
    if "Moneda" in df.columns:
        df = corregir_moneda_sunny(df, col_moneda='Moneda')
        col_fecha_stock = "FechaSepDefinitiva" if "FechaSepDefinitiva" in df.columns else None
        col_precio_stock = "PrecioVenta" if "PrecioVenta" in df.columns else "PrecioLista"
        if col_precio_stock in df.columns:
            df = convertir_precios_a_soles(df, col_precio_stock, "Moneda", col_fecha=col_fecha_stock)
    print(f"   -> {len(df):,} filas")

    output_filename = os.path.join(DOWNLOAD_DIR, f"Reporte_Stock_{datetime.now().strftime('%Y%m%d')}.xlsx")
    try:
        writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Stock')
        if df_ventas is not None and len(df_ventas) > 0:
            df_ventas.to_excel(writer, index=False, sheet_name='VENTAS')
        writer.close()
    except Exception as e:
        print(f"!! Error formato: {e}")
        df.to_excel(output_filename, index=False)
    return output_filename, df


# ============================================================
# GOOGLE SHEETS — subida
# ============================================================

def clean_df_for_sheets(df):
    def _clean(x):
        if x is None: return ""
        try:
            if pd.isna(x): return ""
        except (TypeError, ValueError): pass
        if isinstance(x, float) and (x != x or x == float('inf') or x == float('-inf')): return ""
        return str(x)
    return pd.concat([df[col].apply(_clean) for col in df.columns], axis=1)


def subir_tab(spreadsheet, tab_name, df, rows=10000, cols=200):
    if df is None or len(df) == 0:
        print(f"   !! Sin data para {tab_name}")
        return
    try:
        try: ws = spreadsheet.worksheet(tab_name); ws.clear()
        except: ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        df_c = clean_df_for_sheets(df)
        data = [df_c.columns.tolist()] + df_c.values.tolist()
        ws.update(data, value_input_option="RAW")
        print(f"   -> [OK] {tab_name}: {len(df):,} filas")
    except Exception as e:
        print(f"   !! Error subiendo {tab_name}: {e}")


def upload_to_gsheets(df_ventas, df_stock, df_prospectos=None, df_visitas=None):
    print("\n>> [GOOGLE SHEETS] Actualizando dashboard multi-rol...")
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        creds  = ServiceCredentials.from_service_account_file(GSHEETS_CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        sp     = client.open_by_key(GSHEETS_SPREADSHEET_ID)

        subir_tab(sp, "VENTAS",      df_ventas)
        subir_tab(sp, "STOCK",       df_stock)
        subir_tab(sp, "PROSPECTOS",  df_prospectos)  # NUEVO
        subir_tab(sp, "VISITAS",     df_visitas)     # NUEVO

        print(f"   -> Dashboard: https://docs.google.com/spreadsheets/d/{GSHEETS_SPREADSHEET_ID}")
        return True
    except Exception as e:
        print(f"!! GOOGLE SHEETS ERROR: {e}")
        traceback.print_exc()
        return False


# ============================================================
# EMAIL
# ============================================================

def dispatch_report(file_path):
    print("\n>> [EMAIL] Enviando correo...")
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To']   = EMAIL_TO
    msg['Subject'] = f"REPORTE EVOLTA MULTI-ROL - {datetime.now().strftime('%d/%m/%Y')}"
    body = f"<html><body><p>Reporte actualizado al {datetime.now().strftime('%d/%m/%Y %H:%M')}.</p></body></html>"
    msg.attach(MIMEText(body, 'html'))
    with open(file_path, 'rb') as f:
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(file_path)}"')
        msg.attach(part)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.send_message(msg)
        print("   -> [OK] Correo enviado")
    except Exception as e:
        print(f"!! SMTP ERROR: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("="*70)
    print("   ETL PADOVA — MULTI-ROL DASHBOARD")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    clean_environment(DOWNLOAD_DIR, "*.xlsx")
    clean_environment(DOWNLOAD_DIR_VENTAS, "*.csv")
    clean_environment(DOWNLOAD_DIR_VENTAS, "*.xlsx")

    driver = get_driver(DOWNLOAD_DIR)
    wait   = WebDriverWait(driver, 30)

    archivos_ventas   = {}
    try:
        # 1. Login
        robust_login(driver, wait)

        # 2. Stock
        execute_stock_extraction(driver)

        # 3. Ventas por año
        archivos_ventas = execute_ventas_extraction(driver)

        # 4. Prospectos por año
        execute_prospectos_extraction(driver, wait)

        # 5. Visitas por año
        execute_visitas_extraction(driver, wait)

    except Exception as e:
        print(f"!! CRITICAL ERROR: {e}")
    finally:
        driver.quit()

    # Leer stock crudo para corrección de moneda
    df_stock_crudo = None
    try:
        list_stock = glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsx'))
        if list_stock:
            df_stock_crudo = pd.read_excel(max(list_stock, key=os.path.getctime))
            df_stock_crudo.columns = df_stock_crudo.columns.str.strip()
    except Exception as e:
        print(f"!! Warning stock crudo: {e}")

    # Transformar ventas
    df_ventas = None
    try: df_ventas = process_ventas_data(df_stock=df_stock_crudo)
    except Exception as e: print(f"!! VENTAS ERROR: {e}")

    # Transformar stock
    final_file, df_stock_gs = None, None
    try: final_file, df_stock_gs = process_stock_data(df_ventas)
    except Exception as e: print(f"!! STOCK ERROR: {e}")

    def _leer_por_año(dir_path, prefijo):
        dfs = []
        for año in AÑOS_VENTAS:
            for ext in ['.csv', '.xlsx']:
                ruta = os.path.join(dir_path, f"{prefijo}{año}{ext}")
                if os.path.exists(ruta):
                    try:
                        df = pd.read_csv(ruta, encoding='utf-8', low_memory=False) if ext == '.csv' else pd.read_excel(ruta)
                        df.columns = df.columns.str.strip()
                        dfs.append(df)
                        print(f"   -> {prefijo}{año}: {len(df):,} filas")
                    except Exception as e:
                        print(f"   !! Error leyendo {prefijo}{año}: {e}")
                    break
        if not dfs: return None
        df_all = pd.concat(dfs, ignore_index=True)
        if 'Proyecto' in df_all.columns:
            df_all = df_all[df_all['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
        return df_all

    # Cargar prospectos
    df_prospectos = None
    try:
        df_prospectos = _leer_por_año(DOWNLOAD_DIR_PROSPECTOS, "ReporteProspectos")
        if df_prospectos is not None:
            print(f"   -> PROSPECTOS total: {len(df_prospectos):,} filas")
    except Exception as e:
        print(f"!! PROSPECTOS ERROR: {e}")

    # Cargar visitas
    df_visitas = None
    try:
        df_visitas = _leer_por_año(DOWNLOAD_DIR_VISITAS, "ReporteVisitas")
        if df_visitas is not None:
            print(f"   -> VISITAS total: {len(df_visitas):,} filas")
    except Exception as e:
        print(f"!! VISITAS ERROR: {e}")

    if final_file:
        # Email
        try: dispatch_report(final_file)
        except Exception as e: print(f"!! EMAIL ERROR: {e}")

        # OneDrive (solo local)
        if not IS_CLOUD and ONEDRIVE_OUTPUT_DIR:
            try:
                os.makedirs(ONEDRIVE_OUTPUT_DIR, exist_ok=True)
                shutil.copy2(final_file, os.path.join(ONEDRIVE_OUTPUT_DIR, ONEDRIVE_FILE_NAME))
                print(f"   -> [OK] OneDrive actualizado")
            except Exception as e:
                print(f"!! ONEDRIVE ERROR: {e}")

        # Google Sheets — sube las 4 pestañas
        try:
            upload_to_gsheets(df_ventas, df_stock_gs, df_prospectos, df_visitas)
        except Exception as e:
            print(f"!! GSHEETS ERROR: {e}")

    print("\n" + "="*70)
    print("   PIPELINE COMPLETADO")
    print("="*70)


if __name__ == "__main__":
    main()