import os
import requests
from tqdm import tqdm
from zipfile import ZipFile, BadZipFile
from bs4 import BeautifulSoup
import re

BASE_DIR_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
PASTA_DESTINO = "data/raw"

def get_latest_directory_url(base_url):
    print(f"🔎 Buscando a pasta mais recente em {base_url}...")
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        latest_date = None
        latest_dir_url = None

        for link in soup.find_all('a'):
            href = link.get('href')
            match = re.match(r'(\d{4}-\d{2})/', href)
            if match:
                dir_name = match.group(1)
                current_date = int(dir_name.replace('-', ''))

                if latest_date is None or current_date > latest_date:
                    latest_date = current_date
                    latest_dir_url = base_url + href
        
        if latest_dir_url:
            print(f"✅ Pasta mais recente encontrada: {latest_dir_url}")
        else:
            print("❌ Nenhuma pasta de data encontrada.")
        return latest_dir_url

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o diretório base {base_url}: {e}")
        return None

def get_zip_urls_from_directory(directory_url):
    print(f"🔎 Buscando arquivos ZIP em {directory_url}...")
    try:
        response = requests.get(directory_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        zip_urls = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.endswith('.zip'):
                full_url = directory_url + href
                zip_urls.append(full_url)
        return zip_urls
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o diretório {directory_url}: {e}")
        return []

def baixar_e_extrair_arquivo(url, destino):
    nome_arquivo = url.split("/")[-1]
    caminho_zip = os.path.join(destino, nome_arquivo)

    print(f"⬇️ Baixando {nome_arquivo}...")

    try:
        resposta = requests.get(url, stream=True)
        resposta.raise_for_status()
        tamanho_total = int(resposta.headers.get("content-length", 0))

        with open(caminho_zip, "wb") as f, tqdm(
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
        
        # Extração
        print(f"📦 Extraindo {nome_arquivo}...")
        with ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(destino)
        print(f"✅ Extraído: {nome_arquivo.replace('.zip', '')}")

        os.remove(caminho_zip)
        print(f"🗑️ Removido ZIP: {nome_arquivo}")

    except BadZipFile:
        print(f"❌ Erro: O arquivo {nome_arquivo} está corrompido.")
        if os.path.exists(caminho_zip):
            os.remove(caminho_zip)
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao baixar {nome_arquivo}: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado com {nome_arquivo}: {e}")

def main():
    os.makedirs(PASTA_DESTINO, exist_ok=True)
    
    latest_dir = get_latest_directory_url(BASE_DIR_URL)
    if latest_dir:
        zip_urls = get_zip_urls_from_directory(latest_dir)
        if zip_urls:
            print("\n🚀 Iniciando download + extração automática dos arquivos ZIP...")
            for url in zip_urls:
                baixar_e_extrair_arquivo(url, PASTA_DESTINO)
        else:
            print("Nenhum arquivo ZIP encontrado na pasta mais recente para download.")
    else:
        print("Não foi possível determinar a pasta mais recente para download.")

if __name__ == "__main__":
    main()