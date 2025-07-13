import os
import requests
from tqdm import tqdm
from zipfile import ZipFile

# Lista de URLs da Receita (adicione ou remova conforme sua necessidade)
URLS = {
    "Empresas": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas.zip",
    "Estabelecimentos": "https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos.zip",
    "Socios": "https://dadosabertos.rfb.gov.br/CNPJ/Socios.zip",
    "Cnaes": "https://dadosabertos.rfb.gov.br/CNPJ/Cnaes.zip",
    "Municipios": "https://dadosabertos.rfb.gov.br/CNPJ/Municipios.zip",
    "NaturezaJuridica": "https://dadosabertos.rfb.gov.br/CNPJ/NaturezaJuridica.zip",
}

PASTA_DESTINO = "data/raw"

def baixar_arquivo(url, destino):
    nome_arquivo = url.split("/")[-1]
    caminho_completo = os.path.join(destino, nome_arquivo)

    if os.path.exists(caminho_completo):
        print(f"✅ {nome_arquivo} já existe. Pulando download.")
        return caminho_completo

    print(f"⬇️ Baixando {nome_arquivo}...")

    resposta = requests.get(url, stream=True)
    tamanho_total = int(resposta.headers.get("content-length", 0))

    with open(caminho_completo, "wb") as f, tqdm(
        desc=nome_arquivo,
        total=tamanho_total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as barra:
        for dados in resposta.iter_content(chunk_size=1024):
            f.write(dados)
            barra.update(len(dados))

    print(f"✅ Download de {nome_arquivo} concluído.")
    return caminho_completo

def extrair_zip(caminho_zip, destino):
    print(f"📦 Extraindo {caminho_zip}...")
    with ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(destino)
    print("✅ Extração concluída.")

def main():
    os.makedirs(PASTA_DESTINO, exist_ok=True)

    for nome, url in URLS.items():
        caminho_zip = baixar_arquivo(url, PASTA_DESTINO)
        extrair_zip(caminho_zip, PASTA_DESTINO)

if __name__ == "__main__":
    main()
