with open('data_b64.txt', 'r') as f:
    b64_string = f.read()

app_code = f'''import streamlit as st
import pandas as pd
import gzip
import base64
import io

st.set_page_config(page_title="Dashboard Microdados COVID-19", page_icon="📊", layout="wide")

st.markdown("""
<style>
    .main .block-container {{ padding-top: 2rem; }}
    .metric-card {{
        background-color: #f8f9fc;
        border: 1px solid #e3e6f0;
        border-radius: 0.35rem;
        padding: 1rem;
        box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }}
</style>
""", unsafe_allow_html=True)

st.title("📊 Dashboard Analítico (No-Upload Edition)")
st.markdown("Painel interativo com os dados embutidos diretamente no código-fonte!")

# -------------------------------------------------------------
# OS DADOS DAS 50.000 LINHAS ESTÃO EMBUTIDOS TOTALMENTE AQUI!
# Como GZIP comprimido em Base64 - Dispensa o uso de arquivos CSV
# -------------------------------------------------------------
DATA_B64 = "{b64_string}"

@st.cache_data
def load_data():
    try:
        # Descomprime base64 -> GZIP -> Texto CSV de volta!
        csv_bytes = gzip.decompress(base64.b64decode(DATA_B64))
        csv_text = csv_bytes.decode('latin-1')
        
        # Lê direto da memória
        df_raw = pd.read_csv(io.StringIO(csv_text), sep=';')
        
        colunas_categoricas = ['Municipio', 'FaixaEtaria', 'Sexo', 'Classificacao', 'Evolucao']
        for col in colunas_categoricas:
            if col in df_raw.columns:
                df_raw[col] = df_raw[col].astype('category')
                
        if 'DataNotificacao' in df_raw.columns:
            df_raw['DataNotificacao'] = pd.to_datetime(df_raw['DataNotificacao'], errors='coerce', format='mixed')
                
        if 'Municipio' in df_raw.columns:
            df_raw['Municipio'] = df_raw['Municipio'].astype(str).str.strip().str.upper().astype('category')
            
        return df_raw
    except Exception as e:
        import traceback
        st.error(f"Erro ao descomprimir os dados: {{e}}")
        st.code(traceback.format_exc())
        return pd.DataFrame()

with st.spinner("Descomprimindo e construindo Dashboard... aguarde um segundo."):
    df = load_data()

if df.empty:
    st.warning("Falha na descompressão interna.")
    st.stop()

st.sidebar.header("🔍 Filtros de Análise")

if 'Municipio' in df.columns:
    municipios = [m for m in df['Municipio'].dropna().unique()]
    municipios.sort()
    selected_municipios = st.sidebar.multiselect("Filtrar por Município:", options=municipios)
else:
    selected_municipios = []

if 'Sexo' in df.columns:
    sexos = [s for s in df['Sexo'].dropna().unique()]
    sexos.sort()
    selected_sexos = st.sidebar.multiselect("Filtrar por Sexo:", options=sexos)
else:
    selected_sexos = []

if 'FaixaEtaria' in df.columns:
    idades = [i for i in df['FaixaEtaria'].dropna().unique()]
    idades.sort()
    selected_idades = st.sidebar.multiselect("Filtrar por Faixa Etária:", options=idades)
else:
    selected_idades = []

filtered_df = df.copy()

if selected_municipios:
    filtered_df = filtered_df[filtered_df['Municipio'].isin(selected_municipios)]
if selected_sexos:
    filtered_df = filtered_df[filtered_df['Sexo'].isin(selected_sexos)]
if selected_idades:
    filtered_df = filtered_df[filtered_df['FaixaEtaria'].isin(selected_idades)]

col1, col2, col3 = st.columns(3)
col1.metric("Total de Notificações", f"{{len(filtered_df):,.0f}}".replace(',', '.'))

if 'Evolucao' in filtered_df.columns:
    obitos = filtered_df[filtered_df['Evolucao'].astype(str).str.contains('COVID|bito', case=False, na=False)].shape[0]
    col2.metric("Óbitos Identificados", f"{{obitos:,.0f}}".replace(',', '.'))

if 'Classificacao' in filtered_df.columns:
    confirmados = filtered_df[filtered_df['Classificacao'].astype(str).str.contains('confirmado', case=False, na=False)].shape[0]
    col3.metric("Casos Confirmados", f"{{confirmados:,.0f}}".replace(',', '.'))

st.markdown("---")
col_chart1, col_chart2 = st.columns(2)
with col_chart1:
    st.subheader("Casos por Faixa Etária")
    if 'FaixaEtaria' in filtered_df.columns:
        idade_counts = filtered_df['FaixaEtaria'].value_counts().reset_index()
        idade_counts.columns = ['Faixa Etária', 'Quantidade']
        st.bar_chart(idade_counts.set_index('Faixa Etária'))
        
with col_chart2:
    st.subheader("Distribuição por Sexo")
    if 'Sexo' in filtered_df.columns:
        sexo_counts = filtered_df['Sexo'].value_counts().reset_index()
        sexo_counts.columns = ['Sexo', 'Quantidade']
        st.bar_chart(sexo_counts.set_index('Sexo'))

st.markdown("---")
st.subheader("Evolução Temporal das Notificações")
if 'DataNotificacao' in filtered_df.columns:
    timeline = filtered_df[['DataNotificacao']].dropna()
    timeline['AnoMes'] = timeline['DataNotificacao'].dt.to_period('M').astype(str)
    st.line_chart(timeline.groupby('AnoMes').size())

st.markdown("---")
st.subheader("Explorar Tabela de Dados")
registros_exibir = st.slider("Quantidade de linhas", min_value=10, max_value=5000, value=100)
st.dataframe(filtered_df.head(registros_exibir), use_container_width=True)
'''

with open('app_etl.py', 'w', encoding='utf-8') as f:
    f.write(app_code)
