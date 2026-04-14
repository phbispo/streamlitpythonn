"""
ETL Pipeline — Dados COVID-ES
================================
Extract  → Lê MICRODADOS.csv (sep=';', encoding='latin-1')
Transform → Limpa, padroniza e enriquece os dados
Load      → Salva CSV e Parquet tratados em /output
"""

import os
import logging
import pandas as pd

# ─────────────────────────────────────────
# Configuração de log
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# EXTRACT
# ═══════════════════════════════════════════════════════════
def extract(filepath: str) -> pd.DataFrame:
    """Lê o CSV bruto e retorna um DataFrame."""
    log.info(f"[EXTRACT] Lendo arquivo: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    df = pd.read_csv(
        filepath,
        sep=";",
        encoding="latin-1",
        low_memory=False,
    )

    log.info(f"[EXTRACT] {len(df):,} linhas | {df.shape[1]} colunas carregadas.")
    return df


# ═══════════════════════════════════════════════════════════
# TRANSFORM
# ═══════════════════════════════════════════════════════════
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica todas as transformações e retorna o DataFrame limpo."""
    log.info("[TRANSFORM] Iniciando transformações...")

    df = _padronizar_colunas(df)
    df = _remover_duplicatas(df)
    df = _tratar_nulos(df)
    df = _converter_datas(df)
    df = _padronizar_strings(df)
    df = _criar_colunas_derivadas(df)

    log.info(f"[TRANSFORM] Concluído. Shape final: {df.shape}")
    return df


def _padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços e padroniza os nomes das colunas para snake_case."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    log.info("[TRANSFORM] Colunas padronizadas para snake_case.")
    return df


def _remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas completamente duplicadas."""
    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    if removidas:
        log.warning(f"[TRANSFORM] {removidas:,} linhas duplicadas removidas.")
    return df


def _tratar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estratégia de tratamento de nulos por tipo de coluna:
    - Colunas com > 70% nulos → descartadas
    - Colunas categóricas     → preenchidas com 'NAO_INFORMADO'
    - Colunas numéricas       → preenchidas com mediana
    """
    # Descartar colunas muito vazias
    limiar = 0.70
    pct_nulos = df.isnull().mean()
    cols_descartar = pct_nulos[pct_nulos > limiar].index.tolist()
    if cols_descartar:
        df = df.drop(columns=cols_descartar)
        log.warning(f"[TRANSFORM] Colunas descartadas (>{limiar*100:.0f}% nulos): {cols_descartar}")

    # Preencher categóricas
    categoricas = df.select_dtypes(include=["object"]).columns
    df[categoricas] = df[categoricas].fillna("NAO_INFORMADO")

    # Preencher numéricas com mediana
    numericas = df.select_dtypes(include=["number"]).columns
    for col in numericas:
        mediana = df[col].median()
        df[col] = df[col].fillna(mediana)

    log.info("[TRANSFORM] Nulos tratados.")
    return df


def _converter_datas(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta e converte colunas de data (nomes com 'data' ou 'dt')."""
    cols_data = [c for c in df.columns if "data" in c or c.startswith("dt")]
    for col in cols_data:
        try:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            log.info(f"[TRANSFORM] Coluna '{col}' convertida para datetime.")
        except Exception:
            pass
    return df


def _padronizar_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip e upper em todas as colunas object."""
    cols_obj = df.select_dtypes(include=["object"]).columns
    df[cols_obj] = df[cols_obj].apply(lambda s: s.str.strip().str.upper())
    log.info("[TRANSFORM] Strings padronizadas (strip + upper).")
    return df


def _criar_colunas_derivadas(df: pd.DataFrame) -> pd.DataFrame:
    """Cria colunas extras úteis para análise."""

    # Ano e mês a partir de colunas de data detectadas
    cols_data = df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
    for col in cols_data:
        df[f"{col}_ano"] = df[col].dt.year
        df[f"{col}_mes"] = df[col].dt.month
        log.info(f"[TRANSFORM] Colunas '{col}_ano' e '{col}_mes' criadas.")

    # Flag de caso confirmado (se coluna 'classificacao' existir)
    if "classificacao" in df.columns:
        df["caso_confirmado"] = df["classificacao"].str.contains(
            "CONFIRM", na=False
        ).astype(int)
        log.info("[TRANSFORM] Coluna 'caso_confirmado' criada.")

    return df


# ═══════════════════════════════════════════════════════════
# LOAD
# ═══════════════════════════════════════════════════════════
def load(df: pd.DataFrame, output_dir: str = "output") -> dict:
    """
    Salva o DataFrame tratado em:
    - CSV  → output/covid_es_tratado.csv
    - Parquet → output/covid_es_tratado.parquet
    """
    os.makedirs(output_dir, exist_ok=True)

    paths = {
        "csv":     os.path.join(output_dir, "covid_es_tratado.csv"),
        "parquet": os.path.join(output_dir, "covid_es_tratado.parquet"),
    }

    log.info(f"[LOAD] Salvando CSV em: {paths['csv']}")
    df.to_csv(paths["csv"], index=False, sep=";", encoding="utf-8")

    log.info(f"[LOAD] Salvando Parquet em: {paths['parquet']}")
    df.to_parquet(paths["parquet"], index=False)

    log.info(f"[LOAD] Concluído. {len(df):,} registros salvos.")
    return paths


# ═══════════════════════════════════════════════════════════
# RESUMO PÓS-ETL
# ═══════════════════════════════════════════════════════════
def resumo(df: pd.DataFrame) -> None:
    """Imprime estatísticas rápidas após o pipeline."""
    print("\n" + "=" * 50)
    print("  RESUMO PÓS-ETL")
    print("=" * 50)
    print(f"  Linhas    : {len(df):,}")
    print(f"  Colunas   : {df.shape[1]}")
    print(f"  Nulos totais: {df.isnull().sum().sum()}")

    if "municipio" in df.columns:
        print(f"\n  Top 5 Municípios:")
        print(df["municipio"].value_counts().head(5).to_string())

    if "classificacao" in df.columns:
        print(f"\n  Distribuição de Classificação:")
        print(df["classificacao"].value_counts().to_string())

    print("=" * 50 + "\n")


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════
def run_etl(input_path: str, output_dir: str = "output") -> pd.DataFrame:
    """Executa o pipeline completo ETL e retorna o DataFrame tratado."""
    df_raw       = extract(input_path)
    df_tratado   = transform(df_raw)
    paths        = load(df_tratado, output_dir)
    resumo(df_tratado)

    log.info(f"[ETL] Pipeline finalizado. Arquivos gerados: {list(paths.values())}")
    return df_tratado


if __name__ == "__main__":
    import sys

    # Uso: python etl_covid_es.py <caminho_do_csv> [pasta_saida]
    input_file  = sys.argv[1] if len(sys.argv) > 1 else "MICRODADOS.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output"

    run_etl(input_file, output_path)
