import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import re
import os
import json
import flask
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env (para desenvolvimento local)
load_dotenv()

app = flask.Flask(__name__)

# -------- Função para converter valores textuais para numéricos --------
def converter_para_numero(valor):
    """Converte valores como '5,481 bilhões' para 5481000000"""
    if pd.isna(valor) or valor == '':
        return None
    valor = valor.strip()
    numero_texto = re.search(r'([\d,.]+)', valor)
    if not numero_texto:
        return None
    numero_texto = numero_texto.group(1).replace('.', '').replace(',', '.')
    numero_base = float(numero_texto)
    
    if 'bilhões' in valor or 'bilhão' in valor:
        return int(numero_base * 1000000000)
    elif 'milhões' in valor or 'milhão' in valor:
        return int(numero_base * 1000000)
    elif 'mil' in valor:
        return int(numero_base * 1000)
    else:
        return int(numero_base)

# -------- Função principal --------
def executar_automacao_bigquery():
    # Requisição para site do CTE
    url = 'https://www.cte.fazenda.gov.br/portal/infoEstatisticas.aspx'
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erro ao acessar a página: {response.status_code}")
        return "Erro ao acessar a página", 500
    
    # Extração com BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    CTE = soup.find(id='ctl00_ContentPlaceHolder1_lblValorCTeAutorizada').text
    EMISSOR = soup.find(id='ctl00_ContentPlaceHolder1_lblValorEmissores').text
    DATA = soup.find(id='ctl00_ContentPlaceHolder1_lblDataCTeAutorizada').text
    
    try:
        data_convertida = pd.to_datetime(DATA, format='%d/%m/%Y', errors='raise').strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Erro ao converter data: {e}")
        return f"Erro ao converter data: {e}", 500
    
    # Montar DataFrame
    df = pd.DataFrame([{"Data": data_convertida, "CTe": CTE, "Emissor": EMISSOR}])
    df['Data'] = pd.to_datetime(df['Data']).dt.date
    df['CTE VALOR REAL'] = df['CTe'].apply(converter_para_numero)
    df['EMISSOR VALOR REAL'] = df['Emissor'].apply(converter_para_numero)
    
    print("DataFrame criado:")
    print(df)
    
    # Definir esquema explícito para o BigQuery
    schema = [
        SchemaField("Data", "DATE"),
        SchemaField("CTe", "STRING"),
        SchemaField("Emissor", "STRING"),
        SchemaField("CTE VALOR REAL", "INTEGER"),
        SchemaField("EMISSOR VALOR REAL", "INTEGER")
    ]
    
    # Obter credenciais e configurações do ambiente
    try:
        credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        table_id = os.environ.get('BIGQUERY_TABLE_ID')

        if not credentials_json or not table_id:
            raise ValueError("As variáveis de ambiente GOOGLE_APPLICATION_CREDENTIALS_JSON e BIGQUERY_TABLE_ID devem ser definidas.")

        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        job_config = bigquery.LoadJobConfig(schema=schema)
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Dados inseridos com sucesso na tabela: {table_id}")
        return "Dados inseridos no BigQuery com sucesso!", 200
    except Exception as e:
        error_msg = f"Erro ao inserir dados no BigQuery: {e}"
        print(error_msg)
        return error_msg, 500

@app.route('/')
def home():
    return "API Coletora de Dados do CT-e. Use o endpoint /run para executar.", 200

@app.route('/run', methods=['POST'])
def run():
    # Adicionado verificação de segurança (Cloud Scheduler envia um header específico)
    is_cron = flask.request.headers.get('X-CloudScheduler', False)
    if not is_cron and os.environ.get('FLASK_ENV') == 'production':
         return "Acesso não autorizado.", 403

    return executar_automacao_bigquery()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    # A variável FLASK_ENV pode ser 'development' ou 'production'
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
