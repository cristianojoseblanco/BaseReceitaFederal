import os
import pandas as pd

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"

PADROES_ARQUIVOS = {
    "Empresas": "EMPRECSV",
    "Estabelecimentos": "ESTABE",
    "Socios": "SOCIOCSV",
    "Cnaes": "CNAECSV",
    "Naturezas": "NATJUCSV",
    "Municipios": "MUNICCSV",
    "Qualificacoes": "QUALCSV",
    "Simples": "SIMPLES",
    "Paises": "PAISCSV",
    "Motivos": "MOTICCSV"
}

def encontrar_arquivos_por_padrao(pasta, padrao):
    arquivos = []
    for nome in os.listdir(pasta):
        if padrao.upper() in nome.upper():
            caminho = os.path.join(pasta, nome)
            if os.path.isfile(caminho):
                arquivos.append(caminho)
    return sorted(arquivos)

def limpar_dataframe(df):
    df.columns = [col.strip() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def ler_e_processar(tipo, caminhos):
    if not caminhos:
        print(f"\n🔍 Processando tipo '{tipo}' (0 arquivos)...")
        print(f"❌ Nenhum arquivo válido encontrado.")
        return

    print(f"\n🔍 Processando tipo '{tipo}' ({len(caminhos)} arquivos)...")
    dataframes = []

    for caminho in caminhos:
        try:
            df = pd.read_csv(caminho, sep=';', encoding='latin1', dtype=str, low_memory=False)
            df = limpar_dataframe(df)
            dataframes.append(df)
            print(f"✅ {os.path.basename(caminho)} lido com {len(df):,} linhas.")
        except Exception as e:
            print(f"❌ Erro ao ler {os.path.basename(caminho)}: {e}")

    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        nome_arquivo = os.path.join(PASTA_PROCESSED, f"{tipo.lower()}.csv")
        df_final.to_csv(nome_arquivo, sep=';', index=False, encoding='utf-8')
        print(f"💾 Arquivo salvo em: {nome_arquivo} ({len(df_final):,} linhas)")

def main():
    print("🚀 Iniciando limpeza e padronização dos dados da Receita Federal...\n")
    os.makedirs(PASTA_PROCESSED, exist_ok=True)

    for tipo, padrao in PADROES_ARQUIVOS.items():
        caminhos = encontrar_arquivos_por_padrao(PASTA_RAW, padrao)
        ler_e_processar(tipo, caminhos)

    print("\n✅ Processamento completo. Arquivos salvos em 'data/processed/'\n")

if __name__ == "__main__":
    main()