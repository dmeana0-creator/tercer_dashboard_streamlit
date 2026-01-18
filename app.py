import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
import boto3
from io import StringIO
import os  # <--- IMPORTANTE: Necesitamos esto

# --- TRUCO DE SEGURIDAD ---
# Estas dos líneas obligan a Boto3 a ignorar cualquier archivo corrupto en tu PC
# y centrarse solo en las claves que le damos en el código.
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = "dummy"
os.environ['AWS_CONFIG_FILE'] = "dummy"
# ---------------------------

st.set_page_config(page_title="S3 Log Monitor", layout="wide")

st_autorefresh(interval=2000, key="refresh_s3")

st.title("☁️ Dashboard Streaming desde AWS S3")

def load_data_from_s3():
    try:
        # 1. Leemos los secretos del archivo .streamlit/secrets.toml
        aws_creds = st.secrets["aws"] 
        
        # 2. Conectamos explícitamente pasando las claves
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_creds["access_key_id"],
            aws_secret_access_key=aws_creds["secret_access_key"],
            region_name=aws_creds["region_name"]
        )
        
        obj = s3.get_object(
            Bucket=aws_creds["bucket_name"], 
            Key=aws_creds["file_key"]
        )
        
        csv_string = obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_string))
        
    except Exception as e:
        st.error(f"Error conectando a S3: {e}")
        return pd.DataFrame()

# Cargar y Visualizar
df = load_data_from_s3()

if not df.empty and "valor" in df.columns:
    col1, col2 = st.columns(2)
    col1.metric("Último Valor", df["valor"].iloc[-1])
    col2.metric("Total Registros", len(df))
    
    st.line_chart(df["valor"])
    st.dataframe(df.tail(5))
elif df.empty:
    st.info("Conectando... (Si esto tarda, revisa que el bucket y el archivo existan)")