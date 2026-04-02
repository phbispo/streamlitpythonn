import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(layout="wide")

@st.cache_data
def load_data():
    if os.path.exists("MICRODADOS.csv"):
        df = pd.read_csv("MICRODADOS.csv", sep=';', encoding='latin-1', low_memory=False)
    else:
        st.warning("Arquivo MICRODADOS.csv não encontrado. Usando dataset vazio.")
        df = pd.DataFrame()
    return df

df = load_data()

st.title("📊 Análise COVID - Dashboard Interativo")

opcao = st.sidebar.selectbox("Escolha o exercício", [
    "Visão Geral",
    "Classificação",
    "Top Municípios",
    "Sexo",
    "Faixa Etária",
    "Letalidade",
    "Sintomas",
    "Comorbidades",
    "Evolução Temporal",
    "Tabela Cruzada"
])

if opcao == "Visão Geral":
    st.header("Visão Geral do Dataset")
    st.write("Shape:", df.shape)
    st.write("Tipos de dados:")
    st.dataframe(df.dtypes)

    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0]
    percentual = (nulos / len(df)) * 100 if len(df) > 0 else 0

    resumo = pd.DataFrame({
        "Nulos": nulos,
        "Percentual (%)": percentual
    }).sort_values(by="Percentual (%)", ascending=False)

    st.dataframe(resumo)

elif opcao == "Classificação":
    st.header("Distribuição por Classificação")
    freq = df['Classificacao'].value_counts()
    perc = df['Classificacao'].value_counts(normalize=True) * 100

    res = pd.DataFrame({
        "Frequência": freq,
        "Percentual (%)": perc
    })

    st.dataframe(res)

    fig, ax = plt.subplots()
    res.sort_values(by='Frequência').plot(kind='barh', legend=False, ax=ax)
    st.pyplot(fig)

elif opcao == "Top Municípios":
    st.header("Top 10 Municípios")
    top10 = df['Municipio'].value_counts().head(10)

    st.dataframe(top10)

    fig, ax = plt.subplots()
    top10.sort_values().plot(kind='barh', ax=ax)
    st.pyplot(fig)

elif opcao == "Sexo":
    st.header("Distribuição por Sexo")
    sexo = df['Sexo'].value_counts()

    st.dataframe(sexo)

    fig, ax = plt.subplots()
    sexo.plot(kind='pie', autopct='%1.1f%%', ax=ax)
    ax.set_ylabel("")
    st.pyplot(fig)

elif opcao == "Faixa Etária":
    st.header("Faixa Etária")
    faixa = df['FaixaEtaria'].value_counts().sort_index()

    st.dataframe(faixa)

    fig, ax = plt.subplots()
    faixa.plot(kind='bar', ax=ax)
    plt.xticks(rotation=45)
    st.pyplot(fig)

elif opcao == "Letalidade":
    st.header("Taxa de Letalidade")
    confirmados = df[df['Classificacao'] == 'Confirmados']
    evolucao = confirmados['Evolucao'].value_counts()

    obitos = evolucao.get('Óbito pelo COVID-19', 0)
    total = len(confirmados)

    taxa = (obitos / total) * 100 if total > 0 else 0

    st.metric("Taxa de Letalidade (%)", f"{taxa:.2f}")

elif opcao == "Sintomas":
    st.header("Sintomas Frequentes")

    sintomas = ['Febre','DificuldadeRespiratoria','Tosse','Coriza','DorGarganta','Diarreia','Cefaleia']

    contagem = {}
    for s in sintomas:
        if s in df.columns:
            contagem[s] = df[s].astype(str).str.upper().eq('SIM').sum()

    serie = pd.Series(contagem).sort_values()

    st.dataframe(serie)

    fig, ax = plt.subplots()
    serie.plot(kind='barh', ax=ax)
    st.pyplot(fig)

elif opcao == "Comorbidades":
    st.header("Comorbidades em Óbitos")

    obitos = df[df['Evolucao'] == 'Óbito pelo COVID-19']

    comorb = [
        'ComorbidadePulmao','ComorbidadeCardio','ComorbidadeRenal',
        'ComorbidadeDiabetes','ComorbidadeTabagismo','ComorbidadeObesidade'
    ]

    cont = {c: obitos[c].eq('Sim').sum() for c in comorb if c in df.columns}
    serie = pd.Series(cont).sort_values()

    st.dataframe(serie)

    fig, ax = plt.subplots()
    serie.plot(kind='barh', ax=ax)
    st.pyplot(fig)

elif opcao == "Evolução Temporal":
    st.header("Evolução Temporal")

    df['DataNotificacao'] = pd.to_datetime(df['DataNotificacao'], errors='coerce')
    df['AnoMes'] = df['DataNotificacao'].dt.to_period('M')

    serie = df.groupby('AnoMes').size()

    st.line_chart(serie)

elif opcao == "Tabela Cruzada":
    st.header("Tabela Cruzada")

    top5 = df[df['Classificacao']=='Confirmados']['Municipio'].value_counts().head(5).index
    filtro = df[df['Municipio'].isin(top5)]

    tabela = pd.crosstab(filtro['Municipio'], filtro['Evolucao'])

    st.dataframe(tabela)
