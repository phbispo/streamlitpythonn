import streamlit as st
import pandas as pd

# Configuração da página
st.set_page_config(page_title="Dashboard Microdados COVID-19", page_icon="📊", layout="wide")

# CSS customizado
st.markdown("""
<style>
    .main .block-container { padding-top: 2rem; }
    .metric-card {
        background-color: #f8f9fc;
        border: 1px solid #e3e6f0;
        border-radius: 0.35rem;
        padding: 1rem;
        box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 Dashboard Analítico (com ETL Integrado)")
st.markdown("Painel interativo com carregamento otimizado (ETL) dos **MICRODADOS.csv**.")

# Caminho do arquivo
FILE_PATH = r"C:\Users\pedro\Downloads\MICRODADOS.csv"

@st.cache_data
def load_data():
    try:
        # Descobre as colunas que baterem para não dar erro
        colunas_uteis = ['Municipio', 'Sexo', 'FaixaEtaria', 'Evolucao', 'Classificacao', 'DataNotificacao']
        colunas_existentes = pd.read_csv(FILE_PATH, sep=';', encoding='latin-1', nrows=0).columns.tolist()
        colunas_usar = [c for c in colunas_uteis if c in colunas_existentes]
        
        # Extração - carrega APENAS o necessário, salvando muita memória RAM!
        df_raw = pd.read_csv(FILE_PATH, sep=';', encoding='latin-1', usecols=colunas_usar)
        
        # Transformação (ETL) - Otimização p/ Streamlit
        colunas_categoricas = ['Municipio', 'FaixaEtaria', 'Sexo', 'Classificacao', 'Evolucao']
        for col in colunas_categoricas:
            if col in df_raw.columns:
                df_raw[col] = df_raw[col].astype('category')
                
        if 'DataNotificacao' in df_raw.columns:
            # Usar apenas a data exata acelerará absurdo o parser de datetime (format='%Y-%m-%d')
            df_raw['DataNotificacao'] = pd.to_datetime(df_raw['DataNotificacao'], errors='coerce', format='mixed')
                
        if 'Municipio' in df_raw.columns:
            df_raw['Municipio'] = df_raw['Municipio'].astype(str).str.strip().str.upper().astype('category')
            
        return df_raw
    except Exception as e:
        import traceback
        st.error(f"Erro no código ETL ao carregar o arquivo CSV: {e}")
        st.code(traceback.format_exc())
        return pd.DataFrame()

with st.spinner("Extraindo e Transformando dados... (Processo de ETL Otimizado - aguarde)"):
    df = load_data()

if df.empty:
    st.warning("Não foi possível processar os dados. Verifique se o arquivo está em C:\\Users\\pedro\\Downloads\\MICRODADOS.csv")
    st.stop()

# --- SIDEBAR: Filtros ---
st.sidebar.header("🔍 Filtros de Análise")

# Convert category/object series info into clean lists avoiding NA values breaking Streamlit
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

# Aplicação dos filtros com performance via mask do pandas
filtered_df = df.copy()

if selected_municipios:
    filtered_df = filtered_df[filtered_df['Municipio'].isin(selected_municipios)]
if selected_sexos:
    filtered_df = filtered_df[filtered_df['Sexo'].isin(selected_sexos)]
if selected_idades:
    filtered_df = filtered_df[filtered_df['FaixaEtaria'].isin(selected_idades)]

st.sidebar.markdown("---")
st.sidebar.write(f"**Total de registros exibidos:** {len(filtered_df):,}")

# --- KPIS PRINCIPAIS ---
st.subheader("Visão Geral")
col1, col2, col3 = st.columns(3)

col1.metric("Total de Notificações", f"{len(filtered_df):,.0f}".replace(',', '.'))

if 'Evolucao' in filtered_df.columns:
    obitos = filtered_df[filtered_df['Evolucao'].astype(str).str.contains('COVID|bito', case=False, na=False)].shape[0]
    col2.metric("Óbitos Identificados", f"{obitos:,.0f}".replace(',', '.'))
else:
    col2.metric("Óbitos Identificados", "N/A")

if 'Classificacao' in filtered_df.columns:
    confirmados = filtered_df[filtered_df['Classificacao'].astype(str).str.contains('confirmado', case=False, na=False)].shape[0]
    col3.metric("Casos Confirmados", f"{confirmados:,.0f}".replace(',', '.'))
else:
    col3.metric("Casos Confirmados", "N/A")

st.markdown("---")

# --- GRÁFICOS ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Casos por Faixa Etária")
    if 'FaixaEtaria' in filtered_df.columns:
        grafico_idade = filtered_df['FaixaEtaria'].value_counts().reset_index()
        grafico_idade.columns = ['Faixa Etária', 'Quantidade']
        st.bar_chart(grafico_idade.set_index('Faixa Etária'))

with col_chart2:
    st.subheader("Distribuição por Sexo")
    if 'Sexo' in filtered_df.columns:
        grafico_sexo = filtered_df['Sexo'].value_counts().reset_index()
        grafico_sexo.columns = ['Sexo', 'Quantidade']
        st.bar_chart(grafico_sexo.set_index('Sexo'))

st.markdown("---")

st.subheader("Evolução Temporal das Notificações")
if 'DataNotificacao' in filtered_df.columns:
    timeline = filtered_df[['DataNotificacao']].dropna()
    timeline['AnoMes'] = timeline['DataNotificacao'].dt.to_period('M').astype(str)
    timeline_grouped = timeline.groupby('AnoMes').size()
    
    if not timeline_grouped.empty:
        st.line_chart(timeline_grouped)
    else:
        st.info("Não há dados temporais válidos suficientes.")
else:
    st.info("Coluna 'DataNotificacao' ausente para linha do tempo.")

st.markdown("---")
st.subheader("Explorar Tabela de Dados (Após ETL)")
registros_exibir = st.slider("Quantidade de linhas para exibir na tabela", min_value=10, max_value=5000, value=100)
st.dataframe(filtered_df.head(registros_exibir), use_container_width=True)
