import streamlit as st
import pandas as pd

# ==========================================
# 1. CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(page_title="Dashboard SIAP", page_icon="🌾", layout="wide")
st.title("🌾 Dashboard Analítico SIAP: Avance de Siembras y Cosechas")
st.markdown("Consulta rápida de datos consolidados del Servicio de Información Agroalimentaria y Pesquera.")

# ==========================================
# 2. CARGA DE DATOS (CON CACHÉ)
# ==========================================
# st.cache_data evita que Streamlit descargue el archivo en cada clic. 
# El ttl=3600 hace que vuelva a revisar GitHub cada hora por si subiste datos nuevos.
@st.cache_data(ttl=3600)
def cargar_datos_github():
    url_raw = "https://raw.githubusercontent.com/edwardobrusin/SIAP/main/SIAP_Data_Final.csv"
    try:
        # Leemos directo desde la URL de GitHub
        df = pd.read_csv(url_raw)
        
        # Limpieza básica por si hay espacios extra en las columnas de texto
        cols_str = df.select_dtypes(include=['object']).columns
        df[cols_str] = df[cols_str].apply(lambda x: x.str.strip())
        
        return df
    except Exception as e:
        # Si el archivo aún no está subido o hay error, regresamos un DataFrame vacío
        return pd.DataFrame()

with st.spinner("Conectando con la base de datos en GitHub..."):
    df_base = cargar_datos_github()

# ==========================================
# 3. INTERFAZ Y FILTROS
# ==========================================
if df_base.empty:
    st.warning("⚠️ No se encontraron datos. Asegúrate de haber subido el archivo 'SIAP_Data_Final.csv' a la rama 'main' de tu repositorio de GitHub.")
else:
    # --- BARRA LATERAL (FILTROS) ---
    st.sidebar.header("Filtros de Búsqueda")
    
    # Extraemos opciones únicas para los filtros
    anios_disponibles = sorted(df_base['Año Reporte'].unique().tolist(), reverse=True)
    estados_disponibles = sorted(df_base['Estado'].unique().tolist())
    cultivos_disponibles = sorted(df_base['Cultivo'].unique().tolist())
    
    anio_sel = st.sidebar.multiselect("📅 Año de Reporte:", anios_disponibles, default=anios_disponibles[:1])
    
    # Si no seleccionan nada, mostramos todo, si seleccionan, filtramos
    estado_sel = st.sidebar.multiselect("📍 Entidad Federativa:", estados_disponibles, placeholder="Todos los estados...")
    cultivo_sel = st.sidebar.multiselect("🌱 Cultivo:", cultivos_disponibles, placeholder="Todos los cultivos...")

    # --- APLICAR FILTROS ---
    df_filtrado = df_base.copy()
    
    if anio_sel:
        df_filtrado = df_filtrado[df_filtrado['Año Reporte'].isin(anio_sel)]
    if estado_sel:
        df_filtrado = df_filtrado[df_filtrado['Estado'].isin(estado_sel)]
    if cultivo_sel:
        df_filtrado = df_filtrado[df_filtrado['Cultivo'].isin(cultivo_sel)]

    # ==========================================
    # 4. TARJETAS DE MÉTRICAS (KPIs)
    # ==========================================
    st.subheader("📊 Resumen de la Selección")
    
    # Calculamos algunos totales rápidos para darle un toque analítico
    total_sembrada = df_filtrado['Sembrada'].sum() if 'Sembrada' in df_filtrado.columns else 0
    total_produccion = df_filtrado['Produccion'].sum() if 'Produccion' in df_filtrado.columns else 0
    total_registros = len(df_filtrado)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Registros", f"{total_registros:,}")
    col2.metric("Superficie Sembrada Acumulada", f"{total_sembrada:,.2f} ha")
    col3.metric("Producción Acumulada", f"{total_produccion:,.2f} ton")

    st.divider()

    # ==========================================
    # 5. TABLA DE DATOS Y DESCARGA
    # ==========================================
    st.dataframe(
        df_filtrado,
        use_container_width=True,
        hide_index=True,
        height=500
    )
    
    # Botón para descargar solo lo que está filtrado en pantalla
    csv_filtrado = df_filtrado.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Descargar datos filtrados (CSV)",
        data=csv_filtrado,
        file_name="SIAP_Filtrado.csv",
        mime="text/csv",
        type="primary"
    )

    # Nota al pie para el usuario
    st.caption(f"Última actualización de datos desde GitHub: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")