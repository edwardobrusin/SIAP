import streamlit as st
import pandas as pd
import os
import time
import gc
import platform
import glob
import io
import re
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

# ==========================================
# 1. LÓGICA DEL SCRAPER (Tu Clase Validada)
# ==========================================
# NOTA: Mantén aquí tu clase ScraperSIAP completa tal como funcionó en la versión anterior.
# Por brevedad en la respuesta, asumo que copias y pegas la clase ScraperSIAP aquí.
class ScraperSIAP:
    def __init__(self, download_dir="temp_downloads", headless=False):
        self.base_url = "https://nube.agricultura.gob.mx/avance_agricola/"
        self.download_dir = os.path.abspath(download_dir)
        
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            
        options = webdriver.ChromeOptions()
        
        if os.path.exists("/usr/bin/chromium"):
            options.binary_location = "/usr/bin/chromium"
        elif os.path.exists("/usr/bin/chromium-browser"):
            options.binary_location = "/usr/bin/chromium-browser"
            
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        
        # CAMBIO CLAVE 1: No esperar a que carguen trackers u hojas de estilo lentas
        options.page_load_strategy = 'eager' 
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage") # Usa /tmp en vez de RAM
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-features=NetworkService")
        
        # CAMBIOS CLAVE 2: Estabilización Headless y Memoria
        options.add_argument("--window-size=1920,1080") # Evita que los menús colapsen
        options.add_argument("--disable-extensions")
        options.add_argument("--blink-settings=imagesEnabled=false") # No carga imágenes (Ahorra RAM)
        
        if platform.system() == "Linux" or headless:
            options.add_argument("--headless")
        
        try:
            if platform.system() == "Linux":
                service = Service("/usr/bin/chromedriver")
                self.driver = webdriver.Chrome(service=service, options=options)
            else:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            print(f"Error crítico iniciando ChromeDriver: {e}")
            raise e
            
        # CAMBIO CLAVE 3: Timeout duro para evitar cuelgues infinitos en la red
        self.driver.set_page_load_timeout(60) 
        self.wait = WebDriverWait(self.driver, 20)

    def esperar_desbloqueo_ui(self, timeout=30):
        """Espera ampliada a 30s. El servidor del SIAP puede ser muy lento en consultas pesadas."""
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.invisibility_of_element_located((By.CLASS_NAME, "blockOverlay"))
            )
            time.sleep(0.2) # Breve respiro para que el JS adjunte los eventos al DOM
        except TimeoutException: 
            pass

    def click_robusto(self, elemento):
        try:
            self.esperar_desbloqueo_ui()
            elemento.click()
        except ElementClickInterceptedException:
            self.driver.execute_script("arguments[0].click();", elemento)

    def esperar_elemento(self, locator_type, locator_value, condicion="clickable", timeout=20):
        try:
            self.esperar_desbloqueo_ui()
            wait = WebDriverWait(self.driver, timeout)
            if condicion == "clickable": return wait.until(EC.element_to_be_clickable((locator_type, locator_value)))
            elif condicion == "visible": return wait.until(EC.visibility_of_element_located((locator_type, locator_value)))
        except TimeoutException: return None

    def iniciar_navegador(self):
        self.driver.get(self.base_url)
        btn_cultivo = self.esperar_elemento(By.ID, "tipo-cult", condicion="clickable", timeout=30)
        if btn_cultivo:
            self.click_robusto(btn_cultivo)
            if not self.esperar_elemento(By.ID, "anioagric", condicion="visible"):
                raise Exception("Menú de años no apareció.")
        else:
            raise Exception("Botón inicial no encontrado.")

    def seleccionar_opcion(self, element_id, value):
        try:
            self.esperar_desbloqueo_ui()
            elem = self.esperar_elemento(By.ID, element_id, condicion="clickable")
            if not elem: return False
            select = Select(elem)
            options_text = [o.get_attribute("value") for o in select.options]
            if str(value) not in options_text: return False
            select.select_by_value(str(value))
            time.sleep(0.3) # Reducido de 0.5 a 0.3 para agilizar, pero suficiente para React/Angular
            return True
        except Exception: return False

    # (Conserva tu método procesar_archivo_final exactamente igual aquí)
    def procesar_archivo_final(self, ruta_archivo, meta_info):
        try:
            with open(ruta_archivo, 'rb') as f: content = f.read()
            html_content = ""
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try: html_content = content.decode(encoding); break
                except: continue
            if not html_content: return pd.DataFrame()
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception: return pd.DataFrame()

        datos_totales = []
        tablas = soup.find_all('table')

        for tabla in tablas:
            anio_agricola = "SIN_DATO"
            texto_reciente = "" 
            contador_pasos = 0
            for elem in tabla.previous_elements:
                contador_pasos += 1
                if contador_pasos > 200: break 
                if isinstance(elem, str):
                    texto_actual = elem.strip()
                    if not texto_actual: continue
                    contexto_unido = texto_actual + " " + texto_reciente
                    if "año agrícola" in contexto_unido.lower():
                        match = re.search(r'(\d{4})', contexto_unido)
                        if match: anio_agricola = match.group(1); break 
                    texto_reciente = texto_actual

            try:
                df = pd.read_html(io.StringIO(str(tabla)), header=None)[0].astype(str)
            except ValueError: continue

            for idx, row in df.iterrows():
                cells = [str(c).strip() for c in row.values if str(c).lower() != 'nan']
                row_str = " ".join(cells).lower()
                if "total" in row_str or "sembrada" in row_str: continue
                
                col_cultivo = -1; numeros = []
                for col_idx, cell in enumerate(row.values):
                    val = str(cell).strip().replace(',', '')
                    es_numero = False
                    try: float(val); es_numero = True
                    except: pass
                    if not es_numero and len(val) > 2 and col_cultivo == -1:
                        if val.lower() not in ["superficie", "producción", "rendimiento", "volumen"]: col_cultivo = col_idx
                    if es_numero and col_cultivo != -1 and col_idx > col_cultivo: numeros.append(float(val))
                
                if col_cultivo != -1 and len(numeros) >= 1:
                    datos_totales.append({
                        'Año Reporte': meta_info['year'],
                        'Mes Reporte': meta_info['month'],
                        'Estado': meta_info['state_name'],
                        'Ciclo': meta_info.get('ciclo_name'),
                        'Modalidad': meta_info.get('modalidad_name'),
                        'Año Agrícola': anio_agricola,
                        'Cultivo': row.values[col_cultivo].strip(),
                        'Sembrada': numeros[0],
                        'Cosechada': numeros[1] if len(numeros) > 1 else 0.0,
                        'Siniestrada': numeros[2] if len(numeros) > 2 else 0.0,
                        'Produccion': numeros[3] if len(numeros) > 3 else 0.0,
                        'Rendimiento': numeros[4] if len(numeros) > 4 else 0.0
                    })
        
        # CAMBIO CLAVE 4: Destruir el árbol DOM para liberar RAM
        soup.decompose()
        return pd.DataFrame(datos_totales)

    def descargar_y_procesar(self, meta_info):
        # 1. Consultar
        btn_consultar = self.esperar_elemento(By.ID, "Consultar", condicion="clickable")
        if btn_consultar:
            self.click_robusto(btn_consultar)
            self.esperar_desbloqueo_ui(timeout=40) # Aquí la página suele tardar mucho
        else: 
            return None # Retorna None (Fallo técnico), NO DataFrame vacío

        # 2. Limpiar
        files = glob.glob(os.path.join(self.download_dir, "*"))
        for f in files: 
            try: os.remove(f)
            except: pass

        # 3. Generar Excel
        btn_generar = self.esperar_elemento(By.ID, "Excel", condicion="clickable")
        if not btn_generar: return None
        self.click_robusto(btn_generar)

        # 4. Descargar
        tiempo_max = 40 # Aumentado a 40 segs para evitar Timeouts falsos
        start = time.time(); archivo_descargado = None
        while time.time() - start < tiempo_max:
            archivos = [f for f in glob.glob(os.path.join(self.download_dir, "*")) if not f.endswith('.crdownload') and not f.endswith('.tmp')]
            if archivos: 
                archivo_descargado = max(archivos, key=os.path.getctime)
                time.sleep(0.5) # Asegurar que el buffer del SO terminó de escribir el archivo
                break
            time.sleep(1)
            
        if not archivo_descargado: return None
        
        # 5. Procesar
        df_result = self.procesar_archivo_final(archivo_descargado, meta_info)
        try: os.remove(archivo_descargado)
        except: pass
        return df_result # Puede retornar pd.DataFrame() si está vacío, lo cual es correcto ("Sin datos reales")

    def cerrar(self):
        try: 
            self.driver.quit()
        except: 
            pass
        finally:
            # Forzar cierre en Linux/Mac para evitar procesos zombie en el servidor
            os.system("pkill -f chromium")
            os.system("pkill -f chrome")
            os.system("pkill -f chromedriver")

# ==========================================
# 2. HELPERS (UTILIDADES DE FECHAS/ESTADOS)
# ==========================================

ESTADOS_DICT = {
    0: "Nacional", 1: "Aguascalientes", 2: "Baja California", 3: "Baja California Sur", 
    4: "Campeche", 5: "Coahuila", 6: "Colima", 7: "Chiapas", 8: "Chihuahua", 
    9: "Ciudad de México", 10: "Durango", 11: "Guanajuato", 12: "Guerrero", 
    13: "Hidalgo", 14: "Jalisco", 15: "México", 16: "Michoacán", 17: "Morelos", 
    18: "Nayarit", 19: "Nuevo León", 20: "Oaxaca", 21: "Puebla", 22: "Querétaro", 
    23: "Quintana Roo", 24: "San Luis Potosí", 25: "Sinaloa", 26: "Sonora", 
    27: "Tabasco", 28: "Tamaulipas", 29: "Tlaxcala", 30: "Veracruz", 
    31: "Yucatán", 32: "Zacatecas"
}

MESES_DICT = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio", 
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

def generar_rango_fechas(inicio_str, fin_str):
    """Genera lista de tuplas (Año, Mes_Num, Mes_Nombre) entre dos fechas MM-YYYY."""
    try:
        start_date = datetime.strptime(inicio_str, "%m-%Y")
        end_date = datetime.strptime(fin_str, "%m-%Y")
    except ValueError:
        return None, "Formato incorrecto. Usa MM-YYYY (ej. 02-2025)."

    if start_date > end_date:
        return None, "La fecha de inicio no puede ser mayor a la final."

    fechas = []
    current = start_date
    while current <= end_date:
        year = current.year
        month = current.month
        fechas.append((year, month, MESES_DICT[month]))
        
        # Avanzar al siguiente mes
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
            
    return fechas, None

# ==========================================
# 3. INTERFAZ GRÁFICA (STREAMLIT)
# ==========================================

st.set_page_config(page_title="SIAP Scraper", page_icon="🚜", layout="wide")
st.title("🚜 Extractor SIAP: Avance de Siembras y Cosechas")

# --- BARRA LATERAL (CONFIGURACIÓN) ---
with st.sidebar:
    # 1. Creamos el formulario. Todo lo que esté dentro no recargará la app hasta el click.
    with st.form("configuracion_scraper"):
        st.header("1. Definir Periodo")
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            fecha_inicio = st.text_input("Inicio (MM-YYYY)", value="01-2024")
        with col_d2:
            fecha_fin = st.text_input("Fin (MM-YYYY)", value="03-2024")

        st.header("2. Seleccionar Entidades")
        tipo_seleccion = st.radio(
            "Modo de selección:",
            ["Todo", "Estados sin Nacional", "Nacional", "Específico"],
            index=2
        )

        # Dejamos el selector siempre visible por las reglas del st.form
        nombres_seleccionados = st.multiselect(
            "📍 Si elegiste 'Específico', selecciona aquí:",
            list(ESTADOS_DICT.values()),
            default=["Aguascalientes"]
        )

        st.divider()
        headless_mode = st.checkbox("Modo Silencioso (Ocultar navegador)", value=False)
        
        # NUEVO: Botón para el autoguardado
        resume_checkpoint = st.checkbox("💾 Retomar extracción pendiente", value=True, help="Si el proceso se interrumpió, continuará desde donde se quedó.")

        # 2. Reemplazamos st.button por st.form_submit_button
        btn_start = st.form_submit_button("🚀 Iniciar Extracción", type="primary", use_container_width=True)

# --- PROCESAMIENTO DE LAS OPCIONES DEL FORMULARIO ---
# Esto se ejecuta de forma segura fuera de la barra lateral
ids_estados_seleccionados = []

if tipo_seleccion == "Todo":
    ids_estados_seleccionados = list(ESTADOS_DICT.keys())
elif tipo_seleccion == "Estados sin Nacional":
    ids_estados_seleccionados = [k for k in ESTADOS_DICT.keys() if k != 0]
elif tipo_seleccion == "Nacional":
    ids_estados_seleccionados = [0]
else: # Específico
    # Mapeo inverso Nombre -> ID
    ids_estados_seleccionados = [k for k, v in ESTADOS_DICT.items() if v in nombres_seleccionados]

# --- LÓGICA DE EJECUCIÓN ---

# =====================================================================
# NUEVO: Inicializar la máquina de estados
# =====================================================================
if 'extrayendo' not in st.session_state:
    st.session_state.extrayendo = False
# =====================================================================

if btn_start:
    # NUEVO: Cuando se presiona el botón, activamos el motor
    st.session_state.extrayendo = True 
    
# NUEVO: Cambiamos el condicional principal
# En lugar de "if btn_start:", ahora dependemos del session_state
if st.session_state.extrayendo:
    lista_fechas, error_msg = generar_rango_fechas(fecha_inicio, fecha_fin)
    
    if error_msg:
        st.error(f"❌ Error en fechas: {error_msg}")
        st.session_state.extrayendo = False # Apagamos si hay error
    elif not ids_estados_seleccionados:
        st.error("❌ Debes seleccionar al menos una entidad.")
        st.session_state.extrayendo = False # Apagamos si hay error
    else:
        st.info("Iniciando motor de extracción robusto... Por favor no cierres esta pestaña.")
        
        total_steps = len(lista_fechas) * len(ids_estados_seleccionados)
        current_step = 0
        progress_bar = st.progress(0, text="Esperando inicio... 0.00%")
        log_container = st.container()
        
        all_data_frames = []
        bot = None
        
        # === CONFIGURACIÓN DE SEGURIDAD ===
        MAX_CONSULTAS_SESION = 33 # Cada 60 descargas reiniciamos el navegador
        MAX_REINTENTOS = 3        # Intentos por cada estado/mes si la web falla
        consultas_realizadas = 0
        
        # NUEVO: Rutas de los archivos de autoguardado
        CHECKPOINT_CSV = "SIAP_Data_Checkpoint.csv"
        CHECKPOINT_LOG = "SIAP_Log_Checkpoint.txt"
        procesados_set = set()
        
        # Variables para el mensaje resumido de saltos
        msg_omitidos = log_container.empty()
        primer_omitido = None
        ultimo_omitido = None
        conteo_omitidos = 0

        # NUEVO: Lógica de recuperación
        if resume_checkpoint:
            if os.path.exists(CHECKPOINT_CSV):
                try:
                    # Leemos solo la primera columna para no gastar RAM
                    df_cp = pd.read_csv(CHECKPOINT_CSV, usecols=[0])
                    registros_previos = len(df_cp)
                    
                    # Destruimos la variable para liberar memoria al instante
                    del df_cp
                    gc.collect()
                    
                    log_container.success(f"📂 Checkpoint detectado: El archivo ya contiene {registros_previos} registros seguros en disco.")
                except Exception as e:
                    log_container.warning("⚠️ No se pudo leer el CSV previo. Iniciando recolección de cero.")
            
            if os.path.exists(CHECKPOINT_LOG):
                with open(CHECKPOINT_LOG, "r") as f:
                    procesados_set = set(f.read().splitlines())
                log_container.info(f"✅ Se detectaron {len(procesados_set)} consultas ya finalizadas. Serán omitidas.")
        else:
            # Si el usuario desmarca la casilla, borramos el historial para empezar limpio
            if os.path.exists(CHECKPOINT_CSV): os.remove(CHECKPOINT_CSV)
            if os.path.exists(CHECKPOINT_LOG): os.remove(CHECKPOINT_LOG)

        # Función auxiliar para inicializar/reiniciar el estado del bot
        def configurar_filtros_base(scraper_bot):
            scraper_bot.seleccionar_opcion("cicloProd", "4") # Año Agrícola
            scraper_bot.seleccionar_opcion("modalidad", "3") # Riego + Temporal

        try:
            bot = ScraperSIAP(headless=headless_mode)
            bot.iniciar_navegador()
            configurar_filtros_base(bot)
            
            fechas_por_anio = {}
            for f in lista_fechas:
                yr = f[0]
                if yr not in fechas_por_anio: fechas_por_anio[yr] = []
                fechas_por_anio[yr].append(f)
            
            for anio, paquete_fechas in fechas_por_anio.items():
                if not bot.seleccionar_opcion("anioagric", anio):
                    log_container.warning(f"⚠️ Año {anio} no disponible.")
                    current_step += len(paquete_fechas) * len(ids_estados_seleccionados)
                    progress_bar.progress(min(current_step / total_steps, 1.0))
                    continue
                
                filas_acumuladas_anio = 0
                
                for (_, mes_num, mes_nombre) in paquete_fechas:
                    if not bot.seleccionar_opcion("mesagric", mes_num):
                        log_container.error(f"❌ {anio} | {mes_nombre}: Error seleccionando mes.")
                        current_step += len(ids_estados_seleccionados)
                        progress_bar.progress(min(current_step / total_steps, 1.0))
                        continue
                        
                    for ent_id in ids_estados_seleccionados:
                        nombre_estado = ESTADOS_DICT[ent_id]
                        
                        # ==========================================================
                        # NUEVO: 0. CHECKPOINT SKIP (Si ya lo procesamos, lo saltamos)
                        # ==========================================================
                        clave_actual = f"{anio}_{mes_nombre}_{nombre_estado}"
                        
                        if clave_actual in procesados_set:
                            conteo_omitidos += 1
                            if not primer_omitido: primer_omitido = f"{mes_nombre} {anio}"
                            ultimo_omitido = f"{mes_nombre} {anio}"
                            current_step += 1
                            
                            # CAMBIO CLAVE 6: Estrangulamiento de la UI. 
                            # Solo actualizamos la barra de progreso en Streamlit cada 20 saltos.
                            # Esto evita que la app colapse por exceso de mensajes WebSocket.
                            if conteo_omitidos % 20 == 0 or current_step == total_steps:
                                msg_omitidos.info(f"⏭️ Omitiendo {conteo_omitidos} registros procesados (Desde {primer_omitido} hasta {ultimo_omitido})...")
                                pct = min(current_step / total_steps, 1.0)
                                progress_bar.progress(pct, text=f"Progreso: {pct*100:.2f}% - Saltando ya procesados...")
                            
                            continue # Brinca al siguiente estado
                            
                        # --- 1. LÓGICA DE REINTENTOS ---
                        meta = {
                            'year': anio, 'month': mes_nombre,
                            'state_name': nombre_estado,
                            'ciclo_name': "Año Agrícola", 'modalidad_name': "Riego + Temporal"
                        }
                        
                        df_chunk = None
                        exito_extraccion = False
                        
                        for intento in range(1, MAX_REINTENTOS + 1):
                            bot.seleccionar_opcion("entidad", ent_id)
                            bot.seleccionar_opcion("cultivo", "0") 
                            
                            df_chunk = bot.descargar_y_procesar(meta)
                            
                            if df_chunk is not None: 
                                exito_extraccion = True
                                break 
                            else:
                                if intento < MAX_REINTENTOS:
                                    log_container.warning(f"⚠️ Fallo temporal en {nombre_estado} ({mes_nombre}). Reintentando {intento}/{MAX_REINTENTOS}...")
                                    bot.driver.refresh()
                                    bot.esperar_desbloqueo_ui()
                                    configurar_filtros_base(bot)
                                    bot.seleccionar_opcion("anioagric", anio)
                                    bot.seleccionar_opcion("mesagric", mes_num)
                        
                        # ==========================================================
                        # NUEVO: 3. EVALUAR RESULTADO Y GUARDAR CHECKPOINT
                        # ==========================================================
                        if exito_extraccion:
                            # 3.1 Guardar los datos en el CSV directo (sin acumular en RAM)
                            if not df_chunk.empty:
                                filas_acumuladas_anio += len(df_chunk)
                                
                                try:
                                    es_nuevo = not os.path.exists(CHECKPOINT_CSV)
                                    df_chunk.to_csv(CHECKPOINT_CSV, mode='a', header=es_nuevo, index=False, encoding='utf-8-sig')
                                    
                                    # Forzar limpieza de memoria RAM
                                    del df_chunk
                                    gc.collect()
                                except Exception as e:
                                    log_container.error(f"Error escribiendo el CSV de Checkpoint: {e}")
                            
                            # 3.2 Anotar en el "diario" (Log) que ya pasamos por aquí para no repetirlo
                            try:
                                with open(CHECKPOINT_LOG, "a") as f:
                                    f.write(clave_actual + "\n")
                                procesados_set.add(clave_actual)
                            except Exception as e:
                                log_container.error(f"Error escribiendo el Log de Checkpoint: {e}")
                                
                        elif not exito_extraccion:
                            log_container.error(f"❌ Omitido {anio}-{mes_nombre}-{nombre_estado} tras {MAX_REINTENTOS} fallos técnicos.")
                        
                        consultas_realizadas += 1
                        current_step += 1

                        # ==========================================================
                        # NUEVO: Lógica de limpieza profunda de Streamlit
                        # ==========================================================
                        if consultas_realizadas >= MAX_CONSULTAS_SESION:
                            bot.cerrar() # Cerramos Chrome
        
                            st.success("Lote completado. Vaciando memoria RAM y recargando UI automáticamente...")
                            time.sleep(2)
                            
                            # Esto recarga la página. Como st.session_state.extrayendo sigue
                            # siendo True, el bloque principal volverá a ejecutarse solo.
                            st.rerun()

                        # Actualización de barra visual (Solo si no reiniciamos)
                        pct = min(current_step / total_steps, 1.0)
                        progress_bar.progress(pct, text=f"Progreso: {pct*100:.2f}% - {mes_nombre} {anio} ({nombre_estado})")
                
                if filas_acumuladas_anio > 0:
                    log_container.success(f"✅ {anio}: Completado ({filas_acumuladas_anio} registros).")
                else:
                    log_container.info(f"ℹ️ {anio}: Finalizado sin registros.")

            # --- FINALIZACIÓN ---
            if os.path.exists(CHECKPOINT_CSV):
                # Leer todo el archivo consolidado solo al final para mostrarlo y descargarlo
                final_df = pd.read_csv(CHECKPOINT_CSV)

                # Limpiamos el Log de tracking
                if os.path.exists(CHECKPOINT_LOG): os.remove(CHECKPOINT_LOG)

                progress_bar.progress(1.0, text="¡Completado! 100%")
                
                # ==========================================================
                # NUEVO: Apagamos el motor al terminar todo el proceso
                # ==========================================================
                st.session_state.extrayendo = False
                
                st.balloons()
                st.success(f"🎉 ¡Extracción Exitosa! Total: {len(final_df)} registros.")
                st.dataframe(final_df.head(10))
                
                csv = final_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="💾 Descargar CSV Consolidado",
                    data=csv,
                    file_name="SIAP_Data_Final.csv",
                    mime="text/csv",
                    type="primary"
                )
                
                # Opcional: Si quieres borrar el checkpoint original después de terminar
                # os.remove(CHECKPOINT_CSV) 
            else:
                progress_bar.progress(1.0, text="Finalizado (Sin datos)")
                st.warning("No se encontraron datos en el rango seleccionado tras procesar todo.")
                st.session_state.extrayendo = False # Apagar
                
        except Exception as e:
            st.error(f"❌ Error Crítico: {e}")
            st.session_state.extrayendo = False # Apagar en caso de error
        finally:
            if bot: bot.cerrar()