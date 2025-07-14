import os
import pandas as pd

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"
CHUNKSIZE = 50_000  # Ajuste conforme sua RAM

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

def limpar_colunas(df):
    df.columns = [col.strip() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def processar_em_blocos(tipo, caminhos):
    destino = os.path.join(PASTA_PROCESSED, f"{tipo.lower()}.csv")
    primeiro_bloco = True
    total_linhas = 0

    for caminho in caminhos:
        print(f"📂 Lendo {os.path.basename(caminho)} em blocos...")
        try:
            for chunk in pd.read_csv(caminho, sep=';', encoding='latin1', dtype=str, chunksize=CHUNKSIZE, low_memory=False):
                chunk = limpar_colunas(chunk)
                chunk.to_csv(destino, mode='w' if primeiro_bloco else 'a', sep=';', index=False, header=primeiro_bloco, encoding='utf-8')
                total_linhas += len(chunk)
                primeiro_bloco = False
        except Exception as e:
            print(f"❌ Erro em {caminho}: {e}")
    
    print(f"✅ {tipo} processado com {total_linhas:,} linhas.")

def main():
    print("🚀 Iniciando processamento eficiente por blocos...\n")
    os.makedirs(PASTA_PROCESSED, exist_ok=True)

    for tipo, padrao in PADROES_ARQUIVOS.items():
        caminhos = encontrar_arquivos_por_padrao(PASTA_RAW, padrao)
        if caminhos:
            processar_em_blocos(tipo, caminhos)
        else:
            print(f"⚠️ Nenhum arquivo de {tipo} encontrado.")

    print("\n✅ Todos os dados foram processados e salvos em 'data/processed/'\n")

if __name__ == "__main__":
    main()