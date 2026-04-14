"""
ETL Pipeline — Dados COVID-ES (Streamlit)
==========================================
Extract  → Lê o CSV via file_uploader do Streamlit
Transform → Limpa, padroniza e enriquece os dados
Load      → Disponibiliza o CSV tratado para download
"""

import io
import logging
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

st.set_page_config(page_title="ETL COVID-ES", layout="wide")
st.title("⚙️ ETL — Dados COVID-ES")


# ═══════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════
def extract(file) -> pd.DataFrame:
    df = pd.read_csv(file, sep=";", encoding="latin-1", low_memory=False)
    log.info(f"[EXTRACT] {len(df):,} linhas | {df.shape[1]} colunas")
    return df


# ═══════════════════════════════════════════════════════════
# TRANSFORM
# ═══════════════════════════════════════════════════════════
def transform(df: pd.DataFrame, limiar_nulos: float, logs: list) -> pd.DataFrame:

    # 1. Padronizar nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    logs.append("✅ Colunas padronizadas para snake_case.")

    # 2. Remover duplicatas
    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    logs.append(f"✅ Duplicatas removidas: {removidas:,}")

    # 3. Descartar colunas com muitos nulos
    pct_nulos = df.isnull().mean()
    cols_descartar = pct_nulos[pct_nulos > limiar_nulos].index.tolist()
    if cols_descartar:
        df = df.drop(columns=cols_descartar)
        logs.append(f"⚠️ Colunas descartadas (>{limiar_nulos*100:.0f}% nulos): {cols_descartar}")

    # 4. Preencher nulos
    categoricas = df.select_dtypes(include=["object"]).columns
    df[categoricas] = df[categoricas].fillna("NAO_INFORMADO")

    numericas = df.select_dtypes(include=["number"]).columns
    for col in numericas:
        df[col] = df[col].fillna(df[col].median())
    logs.append("✅ Nulos preenchidos (categóricas → 'NAO_INFORMADO', numéricas → mediana).")

    # 5. Converter datas
    cols_data = [c for c in df.columns if "data" in c or c.startswith("dt")]
    for col in cols_data:
        try:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            logs.append(f"✅ Coluna '{col}' convertida para datetime.")
        except Exception:
            pass

    # 6. Padronizar strings
    cols_obj = df.select_dtypes(include=["object"]).columns
    df[cols_obj] = df[cols_obj].apply(lambda s: s.str.strip().str.upper())
    logs.append("✅ Strings padronizadas (strip + upper).")

    # 7. Colunas derivadas
    cols_dt = df.select_dtypes(include=["datetime64[ns]"]).columns
    for col in cols_dt:
        df[f"{col}_ano"] = df[col].dt.year
        df[f"{col}_mes"] = df[col].dt.month
        logs.append(f"✅ Colunas '{col}_ano' e '{col}_mes' criadas.")

    if "classificacao" in df.columns:
        df["caso_confirmado"] = df["classificacao"].str.contains("CONFIRM", na=False).astype(int)
        logs.append("✅ Coluna 'caso_confirmado' criada.")

    return df


# ═══════════════════════════════════════════════════════════
# LOAD — gera CSV em memória para download
# ═══════════════════════════════════════════════════════════
def load(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, sep=";", encoding="utf-8")
    return buffer.getvalue()


# ═══════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ═══════════════════════════════════════════════════════════
uploaded_file = st.file_uploader("📂 Envie o arquivo CSV (MICRODADOS.csv)", type=["csv"])

if uploaded_file:
    limiar = st.slider(
        "Limiar para descartar colunas com muitos nulos (%)",
        min_value=10, max_value=100, value=70, step=5
    ) / 100

    if st.button("▶️ Executar ETL"):
        with st.spinner("Processando..."):
            logs_etl = []

            # ── EXTRACT ──────────────────────────────────
            st.subheader("📥 1. Extract")
            df_raw = extract(uploaded_file)
            st.write(f"**Linhas:** {len(df_raw):,} | **Colunas:** {df_raw.shape[1]}")
            st.dataframe(df_raw.head(3))

            # ── TRANSFORM ────────────────────────────────
            st.subheader("🔄 2. Transform")
            df_tratado = transform(df_raw.copy(), limiar, logs_etl)

            for msg in logs_etl:
                st.write(msg)

            st.write(f"**Shape após transform:** {df_tratado.shape}")

            nulos = df_tratado.isnull().sum()
            nulos = nulos[nulos > 0]
            if not nulos.empty:
                st.warning("Nulos restantes:")
                st.dataframe(nulos.rename("Nulos"))
            else:
                st.success("Nenhum nulo restante!")

            # ── VISUALIZAÇÕES ────────────────────────────
            col1, col2 = st.columns(2)

            if "classificacao" in df_tratado.columns:
                with col1:
                    st.markdown("**Distribuição de Classificação**")
                    freq = df_tratado["classificacao"].value_counts()
                    fig, ax = plt.subplots()
                    freq.plot(kind="bar", ax=ax)
                    ax.set_title("Classificação dos Casos")
                    st.pyplot(fig)
                    plt.close(fig)

            if "municipio" in df_tratado.columns:
                with col2:
                    st.markdown("**Top 10 Municípios**")
                    top10 = df_tratado["municipio"].value_counts().head(10)
                    fig, ax = plt.subplots()
                    top10.sort_values().plot(kind="barh", ax=ax)
                    ax.set_title("Top 10 Municípios")
                    ax.set_xlabel("Quantidade")
                    st.pyplot(fig)
                    plt.close(fig)

            # ── LOAD ─────────────────────────────────────
            st.subheader("💾 3. Load")
            csv_bytes = load(df_tratado)

            st.download_button(
                label="⬇️ Baixar CSV tratado",
                data=csv_bytes,
                file_name="covid_es_tratado.csv",
                mime="text/csv",
            )

            st.success("✅ ETL concluído com sucesso!")

else:
    st.info("⬆️ Envie um arquivo CSV para começar.")
