import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
import boto3
from io import StringIO

st.set_page_config(page_title="S3 Log Monitor")

# Refrescar cada 2000 ms (2 segundos)
st_autorefresh(interval=2000, key="refresh_s3")

st.title("☁️ Dashboard Streaming desde AWS S3")

# Función para conectar a S3 usando st.secrets
def load_data_from_s3():
    try:
        # Recuperamos credenciales y configuración desde secrets
        aws_creds = st.secrets["aws"]
        
        # Conexión explícita usando las claves guardadas
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_creds["access_key_id"],
            aws_secret_access_key=aws_creds["secret_access_key"],
            region_name=aws_creds["region_name"]
        )
        
        # Descargar el archivo
        obj = s3.get_object(Bucket=aws_creds["bucket_name"], Key=aws_creds["file_key"])
        csv_string = obj['Body'].read().decode('utf-8')
        
        return pd.read_csv(StringIO(csv_string))
        
    except Exception as e:
        # Mostramos error pero sin detener la app completamente
        st.error(f"Error conectando a S3: {e}")
        return pd.DataFrame()

# Cargar datos
df = load_data_from_s3()

# Visualizar
if not df.empty:
    if "valor" in df.columns:
        # Métricas rápidas
        last_val = df["valor"].iloc[-1]
        st.metric("Último Valor", last_val)
        
        # Gráfico
        st.line_chart(df["valor"])
        
        # Tabla
        st.subheader("Últimos registros")
        st.dataframe(df.tail(5))
    else:
        st.warning("El CSV no tiene la columna 'valor'")
else:
    st.info("Esperando datos o error de conexión...")