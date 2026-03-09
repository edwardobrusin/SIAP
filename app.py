import streamlit as st
import pandas as pd
import os
import time
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
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        options.page_load_strategy = 'normal'
        
        if headless:
            options.add_argument("--headless")
        
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.wait = WebDriverWait(self.driver, 20)

    def esperar_desbloqueo_ui(self):
        try:
            WebDriverWait(self.driver, 5).until(EC.invisibility_of_element_located((By.CLASS_NAME, "blockOverlay")))
        except TimeoutException: pass

    def click_robusto(self, elemento):
        try:
            self.esperar_desbloqueo_ui()
            elemento.click()
        except ElementClickInterceptedException:
            self.driver.execute_script("arguments[0].click();", elemento)

    def esperar_elemento(self, locator_type, locator_value, condicion="clickable"):
        try:
            self.esperar_desbloqueo_ui()
            if condicion == "clickable": return self.wait.until(EC.element_to_be_clickable((locator_type, locator_value)))
            elif condicion == "visible": return self.wait.until(EC.visibility_of_element_located((locator_type, locator_value)))
        except TimeoutException: return None

    def iniciar_navegador(self):
        self.driver.get(self.base_url)
        btn_cultivo = self.esperar_elemento(By.ID, "tipo-cult", condicion="clickable")
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
            time.sleep(0.5) 
            return True
        except Exception: return False

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
        return pd.DataFrame(datos_totales)

    def descargar_y_procesar(self, meta_info):
        # 1. Consultar
        btn_consultar = self.esperar_elemento(By.ID, "Consultar", condicion="clickable")
        if btn_consultar:
            self.click_robusto(btn_consultar)
            self.esperar_desbloqueo_ui() 
            time.sleep(2) 
        else: return None

        # 2. Limpiar
        files = glob.glob(os.path.join(self.download_dir, "*"))
        for f in files: 
            try: os.remove(f)
            except: pass

        # 3. Generar
        btn_generar = self.esperar_elemento(By.ID, "Excel", condicion="clickable")
        if not btn_generar: return None
        self.click_robusto(btn_generar)

        # 4. Descargar
        tiempo_max = 30; start = time.time(); archivo_descargado = None
        while time.time() - start < tiempo_max:
            archivos = [f for f in glob.glob(os.path.join(self.download_dir, "*")) if not f.endswith('.crdownload') and not f.endswith('.tmp')]
            if archivos: archivo_descargado = max(archivos, key=os.path.getctime); break
            time.sleep(1)
            
        if not archivo_descargado: return None
        
        # 5. Procesar
        df_result = self.procesar_archivo_final(archivo_descargado, meta_info)
        try: os.remove(archivo_descargado)
        except: pass
        return df_result

    def cerrar(self):
        try: self.driver.quit()
        except: pass

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

    ids_estados_seleccionados = []
    
    if tipo_seleccion == "Todo":
        ids_estados_seleccionados = list(ESTADOS_DICT.keys())
    elif tipo_seleccion == "Estados sin Nacional":
        ids_estados_seleccionados = [k for k in ESTADOS_DICT.keys() if k != 0]
    elif tipo_seleccion == "Nacional":
        ids_estados_seleccionados = [0]
    else: # Específico
        nombres_seleccionados = st.multiselect(
            "Selecciona los estados:",
            list(ESTADOS_DICT.values()),
            default=["Aguascalientes"]
        )
        # Mapeo inverso Nombre -> ID
        ids_estados_seleccionados = [k for k, v in ESTADOS_DICT.items() if v in nombres_seleccionados]

    st.divider()
    headless_mode = st.checkbox("Modo Silencioso (Ocultar navegador)", value=False)
    btn_start = st.button("🚀 Iniciar Extracción", type="primary", use_container_width=True)

# --- LÓGICA DE EJECUCIÓN ---
if btn_start:
    # 1. Validar Fechas
    lista_fechas, error_msg = generar_rango_fechas(fecha_inicio, fecha_fin)
    
    if error_msg:
        st.error(f"❌ Error en fechas: {error_msg}")
    elif not ids_estados_seleccionados:
        st.error("❌ Debes seleccionar al menos una entidad.")
    else:
        # 2. Preparar Interfaz
        st.info("Iniciando motor de extracción... Por favor no cierres esta pestaña.")
        
        # --- CÁLCULO TOTAL DE PASOS ---
        # Total = (Número de Meses-Año) * (Número de Estados)
        total_steps = len(lista_fechas) * len(ids_estados_seleccionados)
        current_step = 0
        
        # BARRA DE PROGRESO (Con porcentaje visible)
        # Inicializamos en 0% con texto
        progress_bar = st.progress(0, text="Esperando inicio... 0%")
        
        # CONTENEDOR DE LOGS
        log_container = st.container()
        
        all_data_frames = []
        bot = None
        
        try:
            bot = ScraperSIAP(headless=headless_mode)
            bot.iniciar_navegador()
            
            # Filtros Fijos
            bot.seleccionar_opcion("cicloProd", "4") # Año Agrícola
            bot.seleccionar_opcion("modalidad", "3") # Riego + Temporal
            
            # Agrupar fechas por Año para el Log Resumido (pero el progreso sigue siendo individual)
            fechas_por_anio = {}
            for f in lista_fechas:
                yr = f[0]
                if yr not in fechas_por_anio: fechas_por_anio[yr] = []
                fechas_por_anio[yr].append(f)
            
            # --- BUCLE AÑOS ---
            for anio, paquete_fechas in fechas_por_anio.items():
                
                # Intentar cambiar al año
                if not bot.seleccionar_opcion("anioagric", anio):
                    log_container.warning(f"⚠️ Año {anio} no disponible.")
                    # Si falla el año, saltamos todos sus pasos en la barra de progreso
                    pasos_saltados = len(paquete_fechas) * len(ids_estados_seleccionados)
                    current_step += pasos_saltados
                    pct = min(current_step / total_steps, 1.0)
                    progress_bar.progress(pct, text=f"Progreso: {int(pct*100)}%")
                    continue
                
                filas_acumuladas_anio = 0
                
                # --- BUCLE MESES ---
                for (_, mes_num, mes_nombre) in paquete_fechas:
                    
                    if not bot.seleccionar_opcion("mesagric", mes_num):
                        log_container.error(f"❌ {anio} | {mes_nombre}: Error seleccionando mes.")
                        # Saltamos los estados de este mes en la barra
                        current_step += len(ids_estados_seleccionados)
                        pct = min(current_step / total_steps, 1.0)
                        progress_bar.progress(pct, text=f"Progreso: {int(pct*100)}%")
                        continue
                        
                    # --- BUCLE ESTADOS (Aquí actualizamos la barra paso a paso) ---
                    for ent_id in ids_estados_seleccionados:
                        nombre_estado = ESTADOS_DICT[ent_id]
                        
                        # Filtros
                        bot.seleccionar_opcion("entidad", ent_id)
                        bot.seleccionar_opcion("cultivo", "0") 
                        
                        meta = {
                            'year': anio, 'month': mes_nombre,
                            'state_name': nombre_estado,
                            'ciclo_name': "Año Agrícola", 'modalidad_name': "Riego + Temporal"
                        }
                        
                        # Extracción
                        df_chunk = bot.descargar_y_procesar(meta)
                        
                        if df_chunk is not None and not df_chunk.empty:
                            all_data_frames.append(df_chunk)
                            filas_acumuladas_anio += len(df_chunk)
                        else:
                            log_container.warning(f"⚠️ {anio} - {mes_nombre} - {nombre_estado}: Sin datos.")
                        
                        # === ACTUALIZACIÓN DE BARRA DE PROGRESO ===
                        current_step += 1
                        pct = min(current_step / total_steps, 1.0)
                        # El texto muestra el porcentaje y qué está haciendo
                        texto_progreso = f"Progreso: {int(pct*100)}% - Procesando {mes_nombre} {anio} ({nombre_estado})"
                        progress_bar.progress(pct, text=texto_progreso)
                
                # --- FIN DEL AÑO ---
                if filas_acumuladas_anio > 0:
                    log_container.success(f"✅ {anio}: Completado ({filas_acumuladas_anio} registros).")
                else:
                    log_container.info(f"ℹ️ {anio}: Finalizado sin registros.")

            # --- FINALIZACIÓN ---
            if all_data_frames:
                final_df = pd.concat(all_data_frames, ignore_index=True)
                
                # Forzar barra al 100% al final por si hubo redondeos
                progress_bar.progress(1.0, text="¡Completado! 100%")
                
                st.balloons()
                st.success(f"🎉 ¡Extracción Exitosa! Total: {len(final_df)} registros.")
                
                st.subheader("Vista Previa")
                st.dataframe(final_df.head(10))
                
                csv = final_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="💾 Descargar CSV Consolidado",
                    data=csv,
                    file_name="SIAP_Data_Final.csv",
                    mime="text/csv",
                    type="primary"
                )
            else:
                progress_bar.progress(1.0, text="Finalizado (Sin datos)")
                st.warning("No se encontraron datos en el rango seleccionado.")
                
        except Exception as e:
            st.error(f"❌ Error Crítico: {e}")
        finally:
            if bot: bot.cerrar()