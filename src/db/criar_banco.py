import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# 🔐 Carregar variáveis do .env
load_dotenv()

# ⚙️ CONFIGURAÇÕES DO BANCO usando variáveis de ambiente
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# 📁 Pasta com os arquivos limpos
CAMINHO_PASTA = "data/processed"

def conectar_postgres():
    """Cria a engine de conexão com PostgreSQL"""
    senha_codificada = quote_plus(DB_PASSWORD)
    url = f"postgresql+psycopg2://{DB_USER}:{senha_codificada}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def criar_tabelas_e_inserir_dados(engine):
    """Lê arquivos .csv e insere no banco com nomes de tabela baseados no nome do arquivo"""
    arquivos = [arq for arq in os.listdir(CAMINHO_PASTA) if arq.endswith('.csv')]

    if not arquivos:
        print("❌ Nenhum arquivo CSV encontrado em", CAMINHO_PASTA)
        return

    for arquivo in arquivos:
        caminho = os.path.join(CAMINHO_PASTA, arquivo)
        nome_tabela = os.path.splitext(arquivo)[0].lower()

        print(f"📥 Importando {arquivo} para a tabela '{nome_tabela}'...")

        try:
            # 🧩 Leitura com chunk para arquivos grandes
            for i, chunk in enumerate(pd.read_csv(
                                                caminho,
                                                sep=';',
                                                chunksize=500_000,
                                                encoding='latin1',
                                                dtype=str,
                                                low_memory=False
                                                )):
                chunk.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
                print(f"✅ Chunk {i+1} inserido em '{nome_tabela}'")
        except Exception as e:
            print(f"❌ Erro ao importar {arquivo}: {e}")

def main():
    print("🔌 Conectando ao banco PostgreSQL...")
    engine = conectar_postgres()
    print("✅ Conexão estabelecida.")

    criar_tabelas_e_inserir_dados(engine)
    print("🎉 Processo concluído.")

if __name__ == "__main__":
    main()
