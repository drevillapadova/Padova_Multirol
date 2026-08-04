# Padova MultiRol Dashboard

## Descripción

Este proyecto es un dashboard interactivo desarrollado para Padova SAC, una empresa inmobiliaria en Perú. Proporciona una visión completa del rendimiento de ventas, stock de inmuebles, prospectos, visitas, campañas de marketing digital y físico, así como análisis de desistimientos y presupuestos.

La aplicación utiliza datos en tiempo real desde Google Sheets y presenta métricas clave como:
- Embudo de ventas (prospectos → visitas → separaciones → ventas)
- Análisis de stock por proyecto y tipología
- Rendimiento de campañas publicitarias (Meta Ads, Google Ads, TikTok Ads)
- Marketing físico y presupuestos
- Análisis de desistimientos

## Características Principales

- **Dashboard Web Interactivo**: Interfaz moderna y responsiva con navegación por secciones
- **Actualización Automática**: Cache actualizado cada hora desde Google Sheets
- **Filtros por Proyecto**: Análisis específico para proyectos como SUNNY, LITORAL 900, HELIO - SANTA BEATRIZ, LOMAS DE CARABAYLLO
- **API REST**: Endpoints para integración con otros sistemas
- **ETL Automatizado**: Procesamiento de datos desde múltiples fuentes

## Instalación

### Prerrequisitos

- Python 3.8+
- pip

### Instalación Local

1. Clona el repositorio:
   ```bash
   git clone <url-del-repositorio>
   cd padova-multirol
   ```

2. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

3. Para el módulo ETL (opcional):
   ```bash
   cd etl
   pip install -r requirements_etl.txt
   ```

4. Ejecuta la aplicación:
   ```bash
   python app.py
   ```

5. Abre tu navegador en `http://localhost:5000`

## Uso

### Interfaz Web

La aplicación cuenta con las siguientes secciones principales:

- **Dashboard Principal**: Vista general con métricas clave
- **Ventas**: Análisis detallado de ventas por proyecto, año y mes
- **Stock**: Inventario de inmuebles disponibles, separados y vendidos
- **Prospectos**: Gestión de leads y tiempo de respuesta
- **Visitas**: Registro de visitas a proyectos
- **Campañas**: Rendimiento de marketing digital y físico
- **Desistimientos**: Análisis de cancelaciones y devoluciones

### API Endpoints

La aplicación expone una API REST para acceder a los datos:

- `GET /api/status`: Estado del cache y conteo de registros
- `GET /api/funnel?proyecto=<PROYECTO>`: Datos del embudo de ventas
- `GET /api/ventas?proyecto=<PROYECTO>&año=<AÑO>&mes=<MES>`: Registros de ventas
- `GET /api/stock?proyecto=<PROYECTO>`: Datos de stock
- `GET /api/prospectos?proyecto=<PROYECTO>`: Lista de prospectos
- `GET /api/visitas?proyecto=<PROYECTO>`: Registros de visitas
- `GET /api/campanas`: Datos de campañas de marketing
- `GET /api/desistimientos`: Análisis de desistimientos
- `POST /api/refresh`: Forzar actualización del cache

### Parámetros de Consulta

- `proyecto`: Filtrar por proyecto específico (ej: "SUNNY", "TODOS")
- `año`: Filtrar ventas por año
- `mes`: Filtrar ventas por mes (formato YYYY-MM)

## Estructura del Proyecto

```
padova-multirol/
├── app.py                          # Aplicación principal Flask
├── Procfile                        # Configuración para despliegue en Heroku
├── requirements.txt                # Dependencias Python para la aplicación web
├── etl/
│   ├── ETL_Padova_MultiRol.py      # Script ETL para procesamiento de datos
│   └── requirements_etl.txt        # Dependencias para el módulo ETL
└── templates/
    └── index.html                  # Plantilla HTML del dashboard
```

### Descripción de Archivos

#### app.py
Aplicación Flask que:
- Lee datos desde Google Sheets usando la API de exportación CSV
- Implementa cache en memoria con actualización automática cada hora
- Proporciona endpoints REST para datos
- Sirve la interfaz web del dashboard
- Calcula métricas de negocio como embudos de ventas, conversiones y tiempos de respuesta

#### ETL_Padova_MultiRol.py
Script de Extracción, Transformación y Carga que:
- Procesa datos desde múltiples fuentes (Google Sheets, APIs externas)
- Realiza web scraping usando Selenium para datos adicionales
- Calcula tipo de cambio desde BCRP
- Envía reportes por email
- Maneja credenciales y configuraciones de servicios externos

#### index.html
Interfaz de usuario del dashboard con:
- Diseño responsivo y moderno
- Navegación lateral con secciones
- Gráficos y tablas interactivas
- Filtros por proyecto y fechas
- Tema oscuro con variables CSS personalizadas

#### requirements.txt
Dependencias principales:
- Flask: Framework web
- pandas: Manipulación de datos
- requests: Llamadas HTTP
- APScheduler: Tareas programadas
- pytz: Manejo de zonas horarias

#### requirements_etl.txt
Dependencias para ETL:
- selenium: Automatización web
- gspread: Interacción con Google Sheets
- webdriver-manager: Gestión de drivers de navegador

## Despliegue

### Heroku

1. Crea una aplicación en Heroku
2. Configura variables de entorno necesarias
3. Despliega usando el Procfile incluido:
   ```bash
   web: gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120
   ```

### Docker

Para despliegue con Docker, crea un Dockerfile basado en Python 3.8+ e instala las dependencias desde requirements.txt.

## Configuración

La aplicación requiere configuración de:
- ID de Google Sheet (configurado en app.py)
- Credenciales para servicios externos (en el módulo ETL)
- Variables de entorno para producción

## Tecnologías Utilizadas

- **Backend**: Python, Flask
- **Frontend**: HTML5, CSS3, JavaScript (vanilla)
- **Datos**: Google Sheets API, pandas
- **Automatización**: APScheduler, Selenium
- **Despliegue**: Gunicorn, Heroku

## Licencia

Este proyecto es propiedad de Padova SAC. Todos los derechos reservados.