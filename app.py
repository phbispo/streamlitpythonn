import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="Análise COVID ES", layout="wide")

st.title("📊 Análise de Dados COVID - ES")

uploaded_file = st.file_uploader("📂 Envie o arquivo CSV (MICRODADOS.csv)", type=["csv"])

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, sep=';', encoding='latin-1', low_memory=False)
        st.success("Arquivo carregado com sucesso!")

        st.subheader("🔍 Visão Geral dos Dados")

        col1, col2 = st.columns(2)

        with col1:
            st.write("Shape do DataFrame:")
            st.write(df.shape)

        with col2:
            st.write("Tipos de dados:")
            st.write(df.dtypes)

        st.subheader("⚠️ Valores Nulos")

        nulos = df.isnull().sum()
        nulos = nulos[nulos > 0]
        percentual = (nulos / len(df)) * 100

        resumo_nulos = pd.DataFrame({
            'Nulos': nulos,
            'Percentual (%)': percentual
        })

        if not resumo_nulos.empty:
            st.dataframe(resumo_nulos)
        else:
            st.success("Sem valores nulos relevantes!")

        if 'Classificacao' in df.columns:
            st.subheader("📌 Classificação dos Casos")

            freq = df['Classificacao'].value_counts()
            perc = df['Classificacao'].value_counts(normalize=True) * 100

            res = pd.DataFrame({
                'Frequência': freq,
                'Percentual (%)': perc
            })

            st.dataframe(res)

            fig, ax = plt.subplots()
            freq.plot(kind='bar', ax=ax)
            ax.set_title("Distribuição de Classificação")
            st.pyplot(fig)

        if 'Municipio' in df.columns:
            st.subheader("🏙️ Top 10 Municípios")

            top10 = df['Municipio'].value_counts().head(10)

            fig, ax = plt.subplots()
            top10.sort_values().plot(kind='barh', ax=ax)
            ax.set_title("Top 10 Municípios com Mais Notificações")
            ax.set_xlabel("Quantidade")
            ax.set_ylabel("Município")

            st.pyplot(fig)

        st.subheader("📄 Visualização dos Dados")
        st.dataframe(df.head())

    except Exception as e:
        st.error(f"Erro ao carregar o arquivo: {e}")

else:
    st.info("⬆️ Envie um arquivo CSV para começar")
