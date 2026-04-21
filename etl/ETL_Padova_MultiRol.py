import time, os, glob, shutil, base64, json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import gspread
from google.oauth2.service_account import Credentials

# ─── CREDENCIALES ────────────────────────────────────────────
USER_CRED = os.environ.get("EVOLTA_USER", "calopez")
PASS_CRED = os.environ.get("EVOLTA_PASS", "")

# ─── URLS EVOLTA ─────────────────────────────────────────────
URL_LOGIN      = "https://v4.evolta.pe/Login/Acceso/Index"
URL_VENTAS     = "https://v4.evolta.pe/Reportes/RepVenta/Index"
URL_STOCK      = "https://v4.evolta.pe/Reportes/RepCargaStock/IndexNuevoRepStock"
URL_PROSPECTOS = "https://v4.evolta.pe/Reportes/RepHiloProspectos/IndexProspecto"
URL_VISITAS    = "https://v4.evolta.pe/Reportes/RepVisita/IndexVisita"

# ─── GOOGLE SHEETS ───────────────────────────────────────────
SPREADSHEET_ID = os.environ.get("GSHEETS_SPREADSHEET_ID", "")
AÑOS_VENTAS    = [2024, 2025, 2026]
TARGET_PROJECTS = ['SUNNY','LITORAL 900','HELIO - SANTA BEATRIZ','LOMAS DE CARABAYLLO']

# ─── DIRECTORIOS ─────────────────────────────────────────────
BASE_DIR       = os.path.join(os.path.expanduser("~"), "Documents", "EVOLTA", "MultiRol")
DIR_VENTAS     = os.path.join(BASE_DIR, "ventas")
DIR_STOCK      = os.path.join(BASE_DIR, "stock")
DIR_PROSPECTOS = os.path.join(BASE_DIR, "prospectos")
DIR_VISITAS    = os.path.join(BASE_DIR, "visitas")

for d in [DIR_VENTAS, DIR_STOCK, DIR_PROSPECTOS, DIR_VISITAS]:
    os.makedirs(d, exist_ok=True)

# ─── NOMBRES DE PESTAÑAS EN GOOGLE SHEETS ────────────────────
TAB_VENTAS     = "VENTAS"
TAB_STOCK      = "STOCK"
TAB_PROSPECTOS = "PROSPECTOS"
TAB_VISITAS    = "VISITAS"


# ══════════════════════════════════════════════════════════════
# GOOGLE SHEETS — cliente
# ══════════════════════════════════════════════════════════════

def get_gsheets_client():
    """Inicializa cliente de Google Sheets desde variable de entorno base64."""
    creds_b64 = os.environ.get("GSHEETS_CREDENTIALS_B64", "")
    if creds_b64:
        creds_json = json.loads(base64.b64decode(creds_b64).decode())
    else:
        # Fallback local: archivo JSON en disco
        creds_json = json.load(open("evoltareportes-00ffe1b337be.json"))

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


def subir_df_a_sheet(client, spreadsheet_id, tab_name, df):
    """Sube un DataFrame a una pestaña del Google Sheet (reemplaza todo)."""
    sh = client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=1, cols=1)

    df = df.fillna("").astype(str)
    data = [df.columns.tolist()] + df.values.tolist()
    ws.clear()
    ws.update(data, value_input_option="USER_ENTERED")
    print(f"   -> [OK] {tab_name}: {len(df):,} filas subidas a Google Sheets")


# ══════════════════════════════════════════════════════════════
# SELENIUM — driver y utilidades
# ══════════════════════════════════════════════════════════════

def get_driver(download_dir):
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    })
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


def dismiss_popup(driver):
    try:
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        time.sleep(0.8)
    except Exception:
        pass


def login(driver, wait):
    print(">> [LOGIN] Iniciando sesión en Evolta...")
    driver.get(URL_LOGIN)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    try:
        u = driver.find_element(By.ID, "UserName")
    except Exception:
        u = driver.find_element(By.XPATH, "//input[@type='text']")
    u.clear()
    u.send_keys(USER_CRED)
    driver.find_element(By.XPATH, "//input[@type='password']").send_keys(PASS_CRED)
    try:
        driver.find_element(By.XPATH, "//button[@type='submit'] | //input[@type='submit']").click()
    except Exception:
        pass
    try:
        wait.until(EC.url_changes(URL_LOGIN))
    except Exception:
        pass
    time.sleep(2)
    dismiss_popup(driver)
    print("   -> [OK] Login exitoso")


def esperar_descarga(download_dir, timeout=180):
    """Espera a que aparezca un archivo nuevo en el directorio."""
    existing = set(glob.glob(os.path.join(download_dir, "*.*")))
    elapsed = 0
    while elapsed < timeout:
        current = set(glob.glob(os.path.join(download_dir, "*.*")))
        new = [f for f in current - existing
               if not f.endswith('.crdownload') and not f.endswith('.tmp')
               and os.path.getsize(f) > 0]
        if new:
            return max(new, key=os.path.getctime)
        time.sleep(1)
        elapsed += 1
    return None


def limpiar_dir(d):
    for f in glob.glob(os.path.join(d, "*.*")):
        try: os.remove(f)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# EXTRACCIÓN — STOCK
# ══════════════════════════════════════════════════════════════

def extraer_stock(driver, wait):
    print("\n>> [STOCK] Descargando reporte de stock...")
    limpiar_dir(DIR_STOCK)
    driver.get(URL_STOCK)
    time.sleep(3)
    dismiss_popup(driver)

    # Seleccionar todos los proyectos
    try:
        sel = wait.until(EC.presence_of_element_located((By.ID, "ProyectoId")))
        try: Select(sel).select_by_visible_text("Todos")
        except Exception:
            try: Select(sel).select_by_visible_text("TODOS")
            except Exception: Select(sel).select_by_index(0)
        time.sleep(1)
    except Exception as e:
        print(f"   !! Warning selector proyecto: {e}")

    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnExportar")))
    driver.execute_script("arguments[0].click();", btn)

    archivo = esperar_descarga(DIR_STOCK)
    if not archivo:
        raise Exception("Timeout esperando descarga de Stock")
    print(f"   -> [OK] Descargado: {os.path.basename(archivo)}")
    return archivo


# ══════════════════════════════════════════════════════════════
# EXTRACCIÓN — VENTAS (por año)
# ══════════════════════════════════════════════════════════════

def extraer_ventas_año(driver, wait, año):
    print(f"\n>> [VENTAS {año}] Descargando...")
    driver.get(URL_VENTAS)
    time.sleep(4)
    dismiss_popup(driver)

    fecha_ini = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")

    # Establecer fechas via JS
    driver.execute_script(f"""
        var inputs = document.querySelectorAll('input');
        var df = [];
        for(var i=0;i<inputs.length;i++){{
            var v=inputs[i].value||'';
            if(v.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) df.push(inputs[i]);
        }}
        if(df.length>=2){{
            df[0].value='{fecha_ini}'; df[0].dispatchEvent(new Event('change',{{bubbles:true}}));
            df[1].value='{fecha_fin}'; df[1].dispatchEvent(new Event('change',{{bubbles:true}}));
        }}
    """)
    time.sleep(1)

    # Seleccionar CSV
    for xpath in [
        "//input[@type='radio'][@value='Csv']",
        "//input[@type='radio'][@value='csv']",
        "//label[contains(text(),'Csv')]",
    ]:
        try:
            el = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", el)
            break
        except Exception:
            pass
    time.sleep(1)

    existing = set(glob.glob(os.path.join(DIR_VENTAS, "*.*")))

    # Click exportar
    for xpath in ["//button[contains(text(),'Exportar')]","//button[@id='btnExportar']","//button[@type='submit']"]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception:
            pass

    time.sleep(5)
    archivo = esperar_descarga(DIR_VENTAS, timeout=120)
    if not archivo:
        print(f"   !! Warning: no se descargó archivo para {año}")
        return None

    dest = os.path.join(DIR_VENTAS, f"ventas_{año}.csv")
    if os.path.exists(dest): os.remove(dest)
    shutil.move(archivo, dest)
    print(f"   -> [OK] ventas_{año}.csv")
    return dest


# ══════════════════════════════════════════════════════════════
# EXTRACCIÓN — PROSPECTOS
# ══════════════════════════════════════════════════════════════

def extraer_prospectos(driver, wait):
    print("\n>> [PROSPECTOS] Descargando reporte de prospectos...")
    limpiar_dir(DIR_PROSPECTOS)
    driver.get(URL_PROSPECTOS)
    time.sleep(3)
    dismiss_popup(driver)

    # Intentar seleccionar todos los proyectos si hay selector
    try:
        sels = driver.find_elements(By.TAG_NAME, "select")
        if sels:
            try: Select(sels[0]).select_by_index(0)
            except Exception: pass
        time.sleep(1)
    except Exception: pass

    # Fechas: desde 01/01/2024 hasta hoy
    fecha_ini = "01/01/2024"
    fecha_fin = datetime.now().strftime("%d/%m/%Y")
    driver.execute_script(f"""
        var inputs = document.querySelectorAll('input');
        var df = [];
        for(var i=0;i<inputs.length;i++){{
            var v=inputs[i].value||'';
            if(v.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) df.push(inputs[i]);
        }}
        if(df.length>=2){{
            df[0].value='{fecha_ini}'; df[0].dispatchEvent(new Event('change',{{bubbles:true}}));
            df[1].value='{fecha_fin}'; df[1].dispatchEvent(new Event('change',{{bubbles:true}}));
        }}
    """)
    time.sleep(1)

    # Click exportar
    for selector in [
        (By.ID, "btnExportar"),
        (By.XPATH, "//button[contains(text(),'Exportar')]"),
        (By.XPATH, "//button[@type='submit']"),
    ]:
        try:
            btn = wait.until(EC.element_to_be_clickable(selector))
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception:
            pass

    archivo = esperar_descarga(DIR_PROSPECTOS)
    if not archivo:
        raise Exception("Timeout esperando descarga de Prospectos")
    print(f"   -> [OK] Descargado: {os.path.basename(archivo)}")
    return archivo


# ══════════════════════════════════════════════════════════════
# EXTRACCIÓN — VISITAS
# ══════════════════════════════════════════════════════════════

def extraer_visitas(driver, wait):
    print("\n>> [VISITAS] Descargando reporte de visitas...")
    limpiar_dir(DIR_VISITAS)
    driver.get(URL_VISITAS)
    time.sleep(3)
    dismiss_popup(driver)

    try:
        sels = driver.find_elements(By.TAG_NAME, "select")
        if sels:
            try: Select(sels[0]).select_by_index(0)
            except Exception: pass
        time.sleep(1)
    except Exception: pass

    fecha_ini = "01/01/2024"
    fecha_fin = datetime.now().strftime("%d/%m/%Y")
    driver.execute_script(f"""
        var inputs = document.querySelectorAll('input');
        var df = [];
        for(var i=0;i<inputs.length;i++){{
            var v=inputs[i].value||'';
            if(v.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) df.push(inputs[i]);
        }}
        if(df.length>=2){{
            df[0].value='{fecha_ini}'; df[0].dispatchEvent(new Event('change',{{bubbles:true}}));
            df[1].value='{fecha_fin}'; df[1].dispatchEvent(new Event('change',{{bubbles:true}}));
        }}
    """)
    time.sleep(1)

    for selector in [
        (By.ID, "btnExportar"),
        (By.XPATH, "//button[contains(text(),'Exportar')]"),
        (By.XPATH, "//button[@type='submit']"),
    ]:
        try:
            btn = wait.until(EC.element_to_be_clickable(selector))
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception:
            pass

    archivo = esperar_descarga(DIR_VISITAS)
    if not archivo:
        raise Exception("Timeout esperando descarga de Visitas")
    print(f"   -> [OK] Descargado: {os.path.basename(archivo)}")
    return archivo


# ══════════════════════════════════════════════════════════════
# TRANSFORMACIÓN — VENTAS
# ══════════════════════════════════════════════════════════════

STUBS = ['T/M','TipoInmueble','Modelo','NroInmueble','NroPiso','Vista',
         'PrecioBase','PrecioLista','DescuentoLista','TotalLista','PrioridadOC','Orden']

def transformar_ventas(archivos):
    print("\n>> [TRANSFORM VENTAS] Consolidando y normalizando...")
    dfs = []
    for año, ruta in archivos.items():
        if not ruta or not os.path.exists(ruta): continue
        try:
            df = pd.read_csv(ruta, encoding='utf-8', low_memory=False) if ruta.endswith('.csv') \
                 else pd.read_excel(ruta)
            df['AÑO'] = int(año)
            dfs.append(df)
            print(f"   -> {año}: {len(df):,} filas")
        except Exception as e:
            print(f"   !! Error {año}: {e}")

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Unpivot inmuebles
    if 'IdProforma' in df.columns:
        df['_idx'] = range(len(df))
        stubs_presentes = [s for s in STUBS if any(f"{s}_1" in c for c in df.columns)]
        if stubs_presentes:
            try:
                df = pd.wide_to_long(df, stubnames=stubs_presentes,
                                     i=['_idx'], j='Indice', sep='_', suffix=r'\d+').reset_index()
                df = df.dropna(subset=['TipoInmueble'])
                df = df[df['TipoInmueble'] != '']
            except Exception as e:
                print(f"   !! Warning unpivot: {e}")
        df = df.drop(columns=['_idx'], errors='ignore')

    # Filtrar proyectos target
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]

    # Excluir devueltos
    if 'EstadoOC' in df.columns:
        df = df[df['EstadoOC'].str.upper() != 'DEVUELTO']

    print(f"   -> Total consolidado: {len(df):,} filas")
    return df


# ══════════════════════════════════════════════════════════════
# TRANSFORMACIÓN — STOCK
# ══════════════════════════════════════════════════════════════

def transformar_stock(archivo):
    print("\n>> [TRANSFORM STOCK] Procesando...")
    df = pd.read_excel(archivo)
    df.columns = df.columns.str.strip()
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
    print(f"   -> {len(df):,} filas")
    return df


# ══════════════════════════════════════════════════════════════
# TRANSFORMACIÓN — PROSPECTOS
# ══════════════════════════════════════════════════════════════

def transformar_prospectos(archivo):
    print("\n>> [TRANSFORM PROSPECTOS] Procesando...")
    ext = os.path.splitext(archivo)[1].lower()
    df = pd.read_csv(archivo, encoding='utf-8', low_memory=False) if ext == '.csv' \
         else pd.read_excel(archivo)
    df.columns = df.columns.str.strip()
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
    print(f"   -> {len(df):,} filas")
    return df


# ══════════════════════════════════════════════════════════════
# TRANSFORMACIÓN — VISITAS
# ══════════════════════════════════════════════════════════════

def transformar_visitas(archivo):
    print("\n>> [TRANSFORM VISITAS] Procesando...")
    ext = os.path.splitext(archivo)[1].lower()
    df = pd.read_csv(archivo, encoding='utf-8', low_memory=False) if ext == '.csv' \
         else pd.read_excel(archivo)
    df.columns = df.columns.str.strip()
    if 'Proyecto' in df.columns:
        df = df[df['Proyecto'].str.upper().isin(TARGET_PROJECTS)]
    print(f"   -> {len(df):,} filas")
    return df


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("   ETL PADOVA — MULTI-ROL DASHBOARD")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    driver = get_driver(DIR_STOCK)
    wait   = WebDriverWait(driver, 30)

    archivos_ventas  = {}
    archivo_stock    = None
    archivo_prosp    = None
    archivo_visitas  = None

    try:
        # 1. Login único
        login(driver, wait)

        # 2. Stock
        archivo_stock = extraer_stock(driver, wait)

        # 3. Ventas por año
        driver.execute_cdp_cmd("Page.setDownloadBehavior",
                               {"behavior": "allow", "downloadPath": DIR_VENTAS})
        for año in AÑOS_VENTAS:
            archivos_ventas[str(año)] = extraer_ventas_año(driver, wait, año)
            time.sleep(2)

        # 4. Prospectos
        driver.execute_cdp_cmd("Page.setDownloadBehavior",
                               {"behavior": "allow", "downloadPath": DIR_PROSPECTOS})
        archivo_prosp = extraer_prospectos(driver, wait)

        # 5. Visitas
        driver.execute_cdp_cmd("Page.setDownloadBehavior",
                               {"behavior": "allow", "downloadPath": DIR_VISITAS})
        archivo_visitas = extraer_visitas(driver, wait)

    except Exception as e:
        print(f"\n!! ERROR EXTRACCIÓN: {e}")
    finally:
        driver.quit()

    # ─── Transformar ─────────────────────────────────────────
    df_ventas    = transformar_ventas(archivos_ventas)
    df_stock     = transformar_stock(archivo_stock)    if archivo_stock   else pd.DataFrame()
    df_prosp     = transformar_prospectos(archivo_prosp)  if archivo_prosp   else pd.DataFrame()
    df_visitas   = transformar_visitas(archivo_visitas)   if archivo_visitas else pd.DataFrame()

    # ─── Subir a Google Sheets ────────────────────────────────
    if not SPREADSHEET_ID:
        print("\n!! SPREADSHEET_ID no configurado — saltando subida a Sheets")
    else:
        print("\n>> [SHEETS] Subiendo datos...")
        try:
            gc = get_gsheets_client()
            if not df_ventas.empty:   subir_df_a_sheet(gc, SPREADSHEET_ID, TAB_VENTAS,     df_ventas)
            if not df_stock.empty:    subir_df_a_sheet(gc, SPREADSHEET_ID, TAB_STOCK,      df_stock)
            if not df_prosp.empty:    subir_df_a_sheet(gc, SPREADSHEET_ID, TAB_PROSPECTOS, df_prosp)
            if not df_visitas.empty:  subir_df_a_sheet(gc, SPREADSHEET_ID, TAB_VISITAS,    df_visitas)
        except Exception as e:
            print(f"!! Error subiendo a Sheets: {e}")

    print("\n" + "=" * 65)
    print("   ETL COMPLETADO")
    print("=" * 65)


if __name__ == "__main__":
    main()