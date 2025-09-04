import os
import glob
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment
from datetime import date, datetime
import calendar
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta
import xlrd
from openpyxl.utils.exceptions import InvalidFileException
from datetime import date
from datetime import datetime
import numpy as np
import csv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

# Função auxiliar para converter datas para formato DATE do BigQuery
def preparar_dataframe_para_bigquery(df):
    """Prepara o DataFrame convertendo as colunas de data para o formato correto"""
    df_copy = df.copy()
    
    # Converter colunas de data do formato string para datetime
    if 'DATA_INICIAL' in df_copy.columns:
        df_copy['DATA_INICIAL'] = pd.to_datetime(df_copy['DATA_INICIAL'], format='%d/%m/%Y').dt.date
    if 'DATA_FINAL' in df_copy.columns:
        df_copy['DATA_FINAL'] = pd.to_datetime(df_copy['DATA_FINAL'], format='%d/%m/%Y').dt.date
    
    return df_copy

# NOVA FUNÇÃO PARA VERIFICAR E SUBSTITUIR DADOS
def verificar_e_substituir_dados_bigquery(df, table_id, data_final, project_id, credentials):
    """
    Verifica se já existem dados para a data especificada e substitui se necessário
    
    Args:
        df: DataFrame com os novos dados
        table_id: ID da tabela no BigQuery (formato: project.dataset.table)
        data_final: Data final para verificar (formato: date object)
        project_id: ID do projeto no BigQuery
        credentials: Credenciais do BigQuery
    """
    print(f"\n--- Verificando existência de dados para DATA_FINAL: {data_final} na tabela {table_id.split('.')[-1]} ---")
    
    try:
        # Query para verificar se já existem dados para esta data
        query_verificacao = f"""
        SELECT COUNT(*) as total_registros
        FROM `{table_id}`
        WHERE DATA_FINAL = '{data_final}'
        """
        
        # Executar a query de verificação
        resultado_verificacao = pandas_gbq.read_gbq(query_verificacao, project_id=project_id, credentials=credentials)
        total_registros_existentes = resultado_verificacao['total_registros'].iloc[0]
        
        if total_registros_existentes > 0:
            print(f"   ⚠️  Encontrados {total_registros_existentes} registros existentes para a data {data_final}")
            print("   🔄 Removendo dados existentes para substituir...")
            
            # Query para deletar os registros existentes
            query_delete = f"""
            DELETE FROM `{table_id}`
            WHERE DATA_FINAL = '{data_final}'
            """
            
            # Executar o delete
            client = bigquery.Client(credentials=credentials, project=project_id)
            job_delete = client.query(query_delete)
            job_delete.result()  # Aguardar a conclusão
            
            print(f"   ✅ Dados existentes removidos com sucesso")
        else:
            print(f"   ✅ Nenhum registro existente encontrado para a data {data_final}")
        
        # Inserir os novos dados
        print(f"   📤 Inserindo {len(df)} novos registros...")
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append', credentials=credentials)
        print(f"   ✅ Dados inseridos com sucesso na tabela '{table_id.split('.')[-1]}'")
        
    except Exception as e:
        print(f"   ❌ Erro ao processar dados na tabela '{table_id.split('.')[-1]}': {e}")
        raise e

# NOVA FUNÇÃO AUXILIAR PARA EXTRAIR DATA FINAL
def extrair_data_final(df):
    """
    Extrai a data final dos dados preparados para BigQuery
    """
    if 'DATA_FINAL' in df.columns and len(df) > 0:
        # Pega a primeira data final (assumindo que todas são iguais no lote)
        data_final = df['DATA_FINAL'].iloc[0]
        return data_final
    else:
        raise ValueError("Coluna DATA_FINAL não encontrada ou DataFrame vazio")

# Carrega as variáveis de ambiente do arquivo .env
# Esta linha deve vir no início do seu script
load_dotenv()

# --- CARREGA CONFIGURAÇÕES SENSÍVEIS DO AMBIENTE ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
INPUT_DIR = os.getenv("INPUT_DIR") # Diretório de downloads/entrada
OUTPUT_DIR = os.getenv("OUTPUT_DIR") # Diretório de documentos/saída

# Validação para garantir que as variáveis foram carregadas
if not all([PROJECT_ID, CREDENTIALS_PATH, DATASET_ID, INPUT_DIR, OUTPUT_DIR]):
    raise ValueError("Uma ou mais variáveis de ambiente essenciais não foram definidas no arquivo .env")

# Configurar credenciais a partir do caminho seguro
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Diretório de downloads
diretorio_downloads = os.path.join(os.path.expanduser('~'), 'Downloads')

# Diretório de documentos
diretorio_documentos = os.path.join(os.path.expanduser('~'), 'Documents')

# Padrão de nome do arquivo
padrao_nome_arquivo = '*.xlsx'

# Caminho completo para o arquivo de saída na pasta de documentos
arquivo_saida = os.path.join(diretorio_documentos, 'resumo_semanal_modificado.xlsx')

# Listar todos os arquivos que correspondem ao padrão de nome
arquivos = glob.glob(os.path.join(diretorio_downloads, padrao_nome_arquivo))

# Mapeamento de produtos para tipo de produto
mapa_tipo_produto = {
    'ETANOL HIDRATADO': 'ETANOL HIDRATADO',
    'GASOLINA ADITIVADA': 'GASOLINA',
    'GASOLINA COMUM': 'GASOLINA',
    'OLEO DIESEL': 'OLEO DIESEL',
    'OLEO DIESEL S10': 'OLEO DIESEL',
    'GNV': 'GNV',
    'GLP': 'GLP'
}

# Caminho completo para o arquivo de saída na pasta de documentos
arquivo_saida = os.path.join(diretorio_documentos, 'resumo_semanal_modificado.xlsx')

# Verificar se há arquivos correspondentes
if arquivos:
    # Selecionar o último arquivo pela data de modificação
    arquivo_mais_recente = max(arquivos, key=os.path.getmtime)
    
    # Carregar o arquivo Excel
    workbook = pd.ExcelFile(arquivo_mais_recente)
    
    # Criar um escritor Excel
    with pd.ExcelWriter(arquivo_saida, engine='xlsxwriter') as writer:
        # Ler o conteúdo da primeira aba para obter a data

        df_primeira_aba = workbook.parse(workbook.sheet_names[0], header=None)
        cabecalho_index = df_primeira_aba[df_primeira_aba.iloc[:, 0] == 'DATA INICIAL'].index[0]
        data_atual_base = df_primeira_aba.iloc[cabecalho_index + 1, 0].strftime('%d/%m/%Y')  # Manter a data como está no arquivo original
        data_anterior_base = (pd.to_datetime(data_atual_base, format='%d/%m/%Y') - pd.DateOffset(days=7)).strftime('%d/%m/%Y')
        
        # Imprimir as datas
        print(f'Data atual: {data_atual_base}')
        print(f'Data anterior: {data_anterior_base}')
        
        # Iterar sobre as abas (tabelas)
        for nome_aba in workbook.sheet_names:
            # Ler o conteúdo da aba em um DataFrame
            df_temporario = workbook.parse(nome_aba, header=None)  # Não usar cabeçalho
            cabecalho_index = df_temporario[df_temporario.iloc[:, 0] == 'DATA INICIAL'].index[0]
            df_temporario.columns = df_temporario.iloc[cabecalho_index]  # Usar a linha correta como cabeçalho
            df_temporario = df_temporario.iloc[cabecalho_index+1:]  # Excluir linhas anteriores ao cabeçalho
            
            # Modificar as datas para o formato desejado '%d/%m/%Y'
            df_temporario['DATA INICIAL'] = pd.to_datetime(df_temporario['DATA INICIAL']).dt.strftime('%d/%m/%Y')
            df_temporario['DATA FINAL'] = pd.to_datetime(df_temporario['DATA FINAL']).dt.strftime('%d/%m/%Y')
            
            # Adicionar a coluna 'TIPO PRODUTO' baseada no valor da coluna 'PRODUTO'
            df_temporario['TIPO PRODUTO'] = df_temporario['PRODUTO'].map(mapa_tipo_produto)
            
            # Salvar o DataFrame como uma aba no arquivo Excel
            df_temporario.to_excel(writer, sheet_name=nome_aba, index=False, na_rep='')
    
    print(f'Arquivo salvo com sucesso em {arquivo_saida}')
else:
    print("Nenhum arquivo encontrado com o padrão de nome especificado.")

data_atual = data_atual_base
data_anterior = data_anterior_base

# Caminho do arquivo resumo_semanal_modificado.xlsx
diretorio_documentos = os.path.join(os.path.expanduser('~'), 'Documents')
caminho_resumo_modificado = os.path.join(diretorio_documentos, 'resumo_semanal_modificado.xlsx')

# Carregar os dados das abas
df_capitais = pd.read_excel(caminho_resumo_modificado, sheet_name='CAPITAIS')
df_estados = pd.read_excel(caminho_resumo_modificado, sheet_name='ESTADOS')
df_municipios = pd.read_excel(caminho_resumo_modificado, sheet_name='MUNICIPIOS')
df_regioes = pd.read_excel(caminho_resumo_modificado, sheet_name='REGIOES')
df_brasil = pd.read_excel(caminho_resumo_modificado, sheet_name='BRASIL')

# Mapear as colunas dos DataFrames
df_capitais.columns = [
    'DATA_INICIAL', 'DATA_FINAL', 'ESTADO', 'MUNICIPIO', 'PRODUTO', 
    'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA', 'PRECO_MEDIO_REVENDA',
    'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA', 'PRECO_MAXIMO_REVENDA', 
    'COEF_DE_VARIACAO_REVENDA', 'TIPO_PRODUTO'
]

df_estados.columns = [
    'DATA_INICIAL', 'DATA_FINAL', 'REGIAO', 'ESTADO', 'PRODUTO', 
    'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA', 'PRECO_MEDIO_REVENDA',
    'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA', 'PRECO_MAXIMO_REVENDA', 
    'COEF_DE_VARIACAO_REVENDA', 'TIPO_PRODUTO'
]

df_municipios.columns = [
    'DATA_INICIAL', 'DATA_FINAL', 'ESTADO', 'MUNICIPIO', 'PRODUTO', 
    'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA', 'PRECO_MEDIO_REVENDA',
    'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA', 'PRECO_MAXIMO_REVENDA', 
    'COEF_DE_VARIACAO_REVENDA', 'TIPO_PRODUTO'
]

df_regioes.columns = [
    'DATA_INICIAL', 'DATA_FINAL', 'REGIAO', 'PRODUTO', 
    'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA', 'PRECO_MEDIO_REVENDA',
    'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA', 'PRECO_MAXIMO_REVENDA', 
    'COEF_DE_VARIACAO_REVENDA', 'TIPO_PRODUTO'
]

df_brasil.columns = [
    'DATA_INICIAL', 'DATA_FINAL', 'BRASIL', 'PRODUTO', 
    'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA', 'PRECO_MEDIO_REVENDA',
    'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA', 'PRECO_MAXIMO_REVENDA', 
    'COEF_DE_VARIACAO_REVENDA', 'TIPO_PRODUTO'
]

# AQUI COMEÇA A PARTE MODIFICADA - INSERÇÃO NO BIGQUERY COM VERIFICAÇÃO
print("\n" + "="*80)
print("INSERINDO DADOS NO BIGQUERY COM VERIFICAÇÃO DE DUPLICATAS")
print("="*80)

######################### CAPITAIS ##################################################
try:
    # Preparar DataFrame para BigQuery
    df_capitais_preparado = preparar_dataframe_para_bigquery(df_capitais)
    data_final = extrair_data_final(df_capitais_preparado)
    
    # Verificar e inserir dados
    table_id = f'{PROJECT_ID}.{DATASET_ID}.Capitais'
    verificar_e_substituir_dados_bigquery(df_capitais_preparado, table_id, data_final, PROJECT_ID, credentials)
except Exception as e:
    print(f"Erro ao processar CAPITAIS: {e}")

######################### ESTADOS ##################################################
try:
    # Preparar DataFrame para BigQuery
    df_estados_preparado = preparar_dataframe_para_bigquery(df_estados)
    data_final = extrair_data_final(df_estados_preparado)
    
    # Verificar e inserir dados
    table_id = f'{PROJECT_ID}.{DATASET_ID}.Estados'
    verificar_e_substituir_dados_bigquery(df_estados_preparado, table_id, data_final, PROJECT_ID, credentials)
except Exception as e:
    print(f"Erro ao processar ESTADOS: {e}")

######################### MUNICIPIOS ##################################################
try:
    # Preparar DataFrame para BigQuery
    df_municipios_preparado = preparar_dataframe_para_bigquery(df_municipios)
    data_final = extrair_data_final(df_municipios_preparado)
    
    # Verificar e inserir dados
    table_id = f'{PROJECT_ID}.{DATASET_ID}.Municipios'
    verificar_e_substituir_dados_bigquery(df_municipios_preparado, table_id, data_final, PROJECT_ID, credentials)
except Exception as e:
    print(f"Erro ao processar MUNICIPIOS: {e}")

######################### REGIOES ##################################################
try:
    # Preparar DataFrame para BigQuery
    df_regioes_preparado = preparar_dataframe_para_bigquery(df_regioes)
    data_final = extrair_data_final(df_regioes_preparado)
    
    # Verificar e inserir dados
    table_id = f'{PROJECT_ID}.{DATASET_ID}.Regioes'
    verificar_e_substituir_dados_bigquery(df_regioes_preparado, table_id, data_final, PROJECT_ID, credentials)
except Exception as e:
    print(f"Erro ao processar REGIOES: {e}")

######################### BRASIL ##################################################
try:
    # Preparar DataFrame para BigQuery
    df_brasil_preparado = preparar_dataframe_para_bigquery(df_brasil)
    data_final = extrair_data_final(df_brasil_preparado)
    
    # Verificar e inserir dados
    table_id = f'{PROJECT_ID}.{DATASET_ID}.Brasil'
    verificar_e_substituir_dados_bigquery(df_brasil_preparado, table_id, data_final, PROJECT_ID, credentials)
except Exception as e:
    print(f"Erro ao processar BRASIL: {e}")

print("\n" + "="*80)
print("PROCESSO DE INSERÇÃO CONCLUÍDO")
print("="*80)

# RESTO DO SEU CÓDIGO ORIGINAL CONTINUA AQUI...
# Query SQL para selecionar os dados da tabela específica
query_sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.Brasil`"

try:
    # Primeira versão
    print("Executando a primeira versão...")
    
    # Carregar a aba 'BRASIL' como um DataFrame
    df_brasil = pandas_gbq.read_gbq(query_sql, project_id=PROJECT_ID, credentials=credentials)

    # Converter as colunas 'DATA_INICIAL' e 'DATA FINAL' para datetime e formatá-las de volta para %d/%m/%Y
    df_brasil['DATA_INICIAL'] = pd.to_datetime(df_brasil['DATA_INICIAL'])
    df_brasil['DATA_FINAL'] = pd.to_datetime(df_brasil['DATA_FINAL'])

    # Voltar a formatar para string no formato desejado
    df_brasil['DATA_INICIAL'] = df_brasil['DATA_INICIAL'].dt.strftime('%d/%m/%Y')
    df_brasil['DATA_FINAL'] = df_brasil['DATA_FINAL'].dt.strftime('%d/%m/%Y')

    # Filtrar os dados pela data atual
    df_brasil_atual = df_brasil[df_brasil['DATA_INICIAL'] == data_atual]

    # Selecionar apenas as colunas desejadas
    produtos_combustiveis = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']

    # Ordenar os produtos na ordem desejada
    produtos_ordem = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']
    precos_medios_revenda_brasil = df_brasil_atual[df_brasil_atual['PRODUTO'].isin(produtos_ordem)]

    # Formatar o print desejado
    print("No Brasil o PRECO_MEDIO_REVENDA dos produtos na data ({}) foram:\n".format(data_atual))
    for produto in produtos_ordem:
        preco_medio = precos_medios_revenda_brasil.loc[precos_medios_revenda_brasil['PRODUTO'] == produto, 'PRECO_MEDIO_REVENDA'].iloc[0]
        print(f"{produto:<20} {preco_medio}")

except Exception as e:
    # Caso ocorra um erro, execute a segunda versão
    print(f"Erro encontrado na primeira versão: {e}")
    print("Executando a segunda versão...")

    # Carregar a aba 'BRASIL' como um DataFrame
    df_brasil = pandas_gbq.read_gbq(query_sql, project_id=PROJECT_ID, credentials=credentials)

    # Converter a coluna 'DATA_INICIAL' para o tipo datetime
    df_brasil['DATA_INICIAL'] = pd.to_datetime(df_brasil['DATA_INICIAL'])

    # Filtrar os dados pela data atual
    df_brasil_atual = df_brasil[df_brasil['DATA_INICIAL'] == data_atual]

    # Selecionar apenas as colunas desejadas
    produtos_combustiveis = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']

    # Ordenar os produtos na ordem desejada
    produtos_ordem = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']
    precos_medios_revenda_brasil = df_brasil_atual[df_brasil_atual['PRODUTO'].isin(produtos_ordem)]

    # Formatar o print desejado
    print("No Brasil o PRECO_MEDIO_REVENDA dos produtos na data ({}) foram:\n".format(data_atual))
    for produto in produtos_ordem:
        preco_medio = precos_medios_revenda_brasil.loc[precos_medios_revenda_brasil['PRODUTO'] == produto, 'PRECO_MEDIO_REVENDA'].iloc[0]
        print(f"{produto:<20} {preco_medio}")

# ... RESTO DO SEU CÓDIGO CONTINUA IGUAL ...

# Query SQL para selecionar os dados da tabela específica
query_sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.Capitais`"

try:
    # Primeira versão
    print("Executando a primeira versão...")
    
    # Carregar a aba 'CAPITAIS' como um DataFrame
    df_sp = pandas_gbq.read_gbq(query_sql, project_id=PROJECT_ID, credentials=credentials)

    # Converter as colunas 'DATA_INICIAL' e 'DATA FINAL' para datetime e formatá-las de volta para %d/%m/%Y
    df_sp['DATA_INICIAL'] = pd.to_datetime(df_sp['DATA_INICIAL'])
    df_sp['DATA_FINAL'] = pd.to_datetime(df_sp['DATA_FINAL'])

    # Voltar a formatar para string no formato desejado
    df_sp['DATA_INICIAL'] = df_sp['DATA_INICIAL'].dt.strftime('%d/%m/%Y')
    df_sp['DATA_FINAL'] = df_sp['DATA_FINAL'].dt.strftime('%d/%m/%Y')
    
    # Filtrar os dados pelo estado SÃO PAULO
    df_sp = df_sp[df_sp['ESTADO'].isin(['SAO PAULO'])]

    # Filtrar os dados pela data atual
    df_sp_atual = df_sp[df_sp['DATA_INICIAL'] == data_atual]

    # Selecionar apenas as colunas desejadas
    produtos_combustiveis = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']

    # Ordenar os produtos na ordem desejada
    produtos_ordem = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']
    precos_medios_revenda_sp = df_sp_atual[df_sp_atual['PRODUTO'].isin(produtos_ordem)]

    # Formatar o print desejado
    print("Em SAO PAULO o PRECO_MEDIO_REVENDA dos produtos na data ({}) foram:\n".format(data_atual))
    for produto in produtos_ordem:
        preco_medio = precos_medios_revenda_sp.loc[precos_medios_revenda_sp['PRODUTO'] == produto, 'PRECO_MEDIO_REVENDA'].iloc[0]
        print(f"{produto:<20} {preco_medio}")

except Exception as e:
    # Caso ocorra um erro, execute a segunda versão
    print(f"Erro encontrado na primeira versão: {e}")
    print("Executando a segunda versão...")

    # Carregar a aba 'CAPITAIS' como um DataFrame
    df_sp = pandas_gbq.read_gbq(query_sql, project_id=PROJECT_ID, credentials=credentials)

    # Converter a coluna 'DATA_INICIAL' para o tipo datetime
    df_sp['DATA_INICIAL'] = pd.to_datetime(df_sp['DATA_INICIAL'])
    
    # Filtrar os dados pelo estado SÃO PAULO
    df_sp = df_sp[df_sp['ESTADO'].isin(['SAO PAULO'])]
    
    # Filtrar os dados pela data atual
    df_sp_atual = df_sp[df_sp['DATA_INICIAL'] == data_atual]

    # Selecionar apenas as colunas desejadas
    produtos_combustiveis = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']

    # Ordenar os produtos na ordem desejada
    produtos_ordem = ['OLEO DIESEL', 'OLEO DIESEL S10', 'GASOLINA COMUM', 'GASOLINA ADITIVADA', 'ETANOL HIDRATADO', 'GNV']
    precos_medios_revenda_sp = df_sp_atual[df_sp_atual['PRODUTO'].isin(produtos_ordem)]

    # Formatar o print desejado
    print("Em SAO PAULO o PRECO_MEDIO_REVENDA dos produtos na data ({}) foram:\n".format(data_atual))
    for produto in produtos_ordem:
        preco_medio = precos_medios_revenda_sp.loc[precos_medios_revenda_sp['PRODUTO'] == produto, 'PRECO_MEDIO_REVENDA'].iloc[0]
        print(f"{produto:<20} {preco_medio}")

semana_atual = data_atual_base
semana_anterior = data_anterior_base
print(semana_atual)
print(semana_anterior)
print(type(semana_atual))
print(type(semana_anterior))

# Convertendo as strings para objetos de data
semana_atual_data = datetime.strptime(semana_atual, '%d/%m/%Y')
semana_anterior_data = datetime.strptime(semana_anterior, '%d/%m/%Y')

# Agora, você pode comparar as datas
print(semana_atual)
print(semana_anterior)
print(type(semana_atual))
print(type(semana_anterior))

# Query SQL para selecionar os dados da tabela específica
query_sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.Capitais`"

# Selecionar a aba "CAPITAIS" no arquivo base
df_capitais_base_comparativo = pandas_gbq.read_gbq(query_sql, project_id=PROJECT_ID, credentials=credentials)

# Converter a coluna 'DATA INICIAL' para o tipo datetime
df_capitais_base_comparativo['DATA_INICIAL'] = pd.to_datetime(df_capitais_base_comparativo['DATA_INICIAL'])

# Lista de capitais
capitais = [
    'ARACAJU', 'BELEM', 'BELO HORIZONTE', 'BOA VISTA', 'BRASILIA', 'CAMPO GRANDE',
    'CUIABA', 'CURITIBA', 'FLORIANOPOLIS', 'FORTALEZA', 'GOIANIA', 'JOAO PESSOA',
    'MACAPA', 'MACEIO', 'MANAUS', 'NATAL', 'PALMAS', 'PORTO ALEGRE', 'PORTO VELHO',
    'RECIFE', 'RIO BRANCO', 'RIO DE JANEIRO', 'SALVADOR', 'SAO LUIS', 'SAO PAULO', 'TERESINA', 'VITORIA'
]

# Filtrar os dados da aba 'CAPITAIS' para incluir apenas as capitais da lista e a data atual
df_capitais_filtrado_base = df_capitais_base_comparativo[df_capitais_base_comparativo['DATA_INICIAL'] == semana_atual_data].copy()
df_capitais_filtrado_base = df_capitais_filtrado_base[df_capitais_filtrado_base['MUNICIPIO'].isin(capitais)].copy()
# Pivotar a tabela para ter os produtos como colunas
df_pivot_base_atual = df_capitais_filtrado_base.pivot(index='MUNICIPIO', columns='PRODUTO', values='PRECO_MEDIO_REVENDA')

# Selecionar apenas as linhas de interesse (óleo diesel comum e óleo diesel S10)
df_resultado_base_atual = df_pivot_base_atual[['OLEO DIESEL', 'OLEO DIESEL S10']]
    
# Remover as linhas com valores NaN
df_resultado_limpo_atual = df_resultado_base_atual.dropna()

# Resetar o índice para transformar "MUNICÍPIO" em uma coluna
df_resultado_limpo_atual.reset_index(inplace=True)

# Renomear a coluna "MUNICÍPIO" para "CAPITAIS"
df_resultado_limpo_atual.rename(columns={'MUNICIPIO': 'CAPITAIS'}, inplace=True)

# Selecionar apenas as colunas desejadas
df_resultado_limpo_atual = df_resultado_limpo_atual[['CAPITAIS', 'OLEO DIESEL S10', 'OLEO DIESEL']]

# Exibir o resultado
print(f"Resultado da semana atual da data: {semana_atual}")
print(df_resultado_limpo_atual)

# Filtrar os dados da aba 'CAPITAIS' para incluir apenas as capitais da lista e a data atual
df_capitais_filtrado_base_anterior = df_capitais_base_comparativo[
    (df_capitais_base_comparativo['DATA_INICIAL'] == semana_anterior_data) &
    (df_capitais_base_comparativo['MUNICIPIO'].isin(capitais))
].copy()

# Pivotar a tabela para ter os produtos como colunas
df_pivot_base_anterior = df_capitais_filtrado_base_anterior.pivot(index='MUNICIPIO', columns='PRODUTO', values='PRECO_MEDIO_REVENDA')

# Selecionar apenas as linhas de interesse (óleo diesel comum e óleo diesel S10)
df_resultado_base_anterior = df_pivot_base_anterior[['OLEO DIESEL', 'OLEO DIESEL S10']]

# Remover as linhas com valores NaN
df_resultado_limpo_anterior = df_resultado_base_anterior.dropna()

# Resetar o índice para transformar "MUNICÍPIO" em uma coluna
df_resultado_limpo_anterior.reset_index(inplace=True)

# Renomear a coluna "MUNICÍPIO" para "CAPITAIS"
df_resultado_limpo_anterior.rename(columns={'MUNICIPIO': 'CAPITAIS'}, inplace=True)

# Selecionar apenas as colunas desejadas
df_resultado_limpo_anterior = df_resultado_limpo_anterior[['CAPITAIS', 'OLEO DIESEL S10', 'OLEO DIESEL']]

# Exibir o resultado
print(f"Resultado da semana anterior da data: {semana_anterior}")
print(df_resultado_limpo_anterior)

# Converter as colunas de preços para numérico
df_resultado_limpo_atual['OLEO DIESEL S10'] = pd.to_numeric(df_resultado_limpo_atual['OLEO DIESEL S10'], errors='coerce')
df_resultado_limpo_atual['OLEO DIESEL'] = pd.to_numeric(df_resultado_limpo_atual['OLEO DIESEL'], errors='coerce')

df_resultado_limpo_anterior['OLEO DIESEL S10'] = pd.to_numeric(df_resultado_limpo_anterior['OLEO DIESEL S10'], errors='coerce')
df_resultado_limpo_anterior['OLEO DIESEL'] = pd.to_numeric(df_resultado_limpo_anterior['OLEO DIESEL'], errors='coerce')

# Calcular a diferença entre os DataFrames
df_diferenca = df_resultado_limpo_atual.set_index('CAPITAIS') - df_resultado_limpo_anterior.set_index('CAPITAIS')

# Calcular a variação percentual
df_variacao_percentual = ((df_diferenca) / df_resultado_limpo_anterior.set_index('CAPITAIS')) * 100

# Exibir os resultados
print("diferença:\n \n",df_diferenca)
print("\n")
print("variação percentual:\n \n",df_variacao_percentual)

# Supondo que df_resultado_limpo_atual e df_resultado_limpo_anterior já estejam definidos

# Converter as colunas de preços para numérico
df_resultado_limpo_atual['OLEO DIESEL S10'] = pd.to_numeric(df_resultado_limpo_atual['OLEO DIESEL S10'], errors='coerce')
df_resultado_limpo_atual['OLEO DIESEL'] = pd.to_numeric(df_resultado_limpo_atual['OLEO DIESEL'], errors='coerce')

df_resultado_limpo_anterior['OLEO DIESEL S10'] = pd.to_numeric(df_resultado_limpo_anterior['OLEO DIESEL S10'], errors='coerce')
df_resultado_limpo_anterior['OLEO DIESEL'] = pd.to_numeric(df_resultado_limpo_anterior['OLEO DIESEL'], errors='coerce')

# Calcular a diferença entre os DataFrames
df_diferenca = df_resultado_limpo_atual.set_index('CAPITAIS') - df_resultado_limpo_anterior.set_index('CAPITAIS')

# Calcular a variação percentual
df_variacao_percentual = ((df_diferenca) / df_resultado_limpo_anterior.set_index('CAPITAIS')) * 100

# Criar um DataFrame comparativo com MultiIndex
df_comparativo = pd.concat(
    [
        df_diferenca.rename(columns=lambda x: ('Diferença', x)), 
        df_variacao_percentual.rename(columns=lambda x: ('Variação Percentual', x))
    ], 
    axis=1
)

# Ajustar para ter colunas em um MultiIndex sem o prefixo "PRODUTO"
df_comparativo.columns = pd.MultiIndex.from_tuples(df_comparativo.columns)

# Exibir o DataFrame comparativo
print("DataFrame Comparativo:\n", df_comparativo)

# Filtrar as capitais que tiveram uma variação absoluta maior que 2% (positiva ou negativa)
df_variacao_maior_que_2 = df_comparativo[
    (df_comparativo[('Variação Percentual', 'OLEO DIESEL S10')].abs() > 2) | 
    (df_comparativo[('Variação Percentual', 'OLEO DIESEL')].abs() > 2)
]

# Ordenar as capitais filtradas em ordem alfabética
df_variacao_maior_que_2_ordenado = df_variacao_maior_que_2.sort_index

# ... TODO SEU CÓDIGO ORIGINAL ATÉ AQUI ...

# Filtrar as capitais que tiveram uma variação absoluta maior que 2% (positiva ou negativa)
df_variacao_maior_que_2 = df_comparativo[
    (df_comparativo[('Variação Percentual', 'OLEO DIESEL S10')].abs() > 2) | 
    (df_comparativo[('Variação Percentual', 'OLEO DIESEL')].abs() > 2)
]

# Ordenar as capitais filtradas em ordem alfabética
df_variacao_maior_que_2_ordenado = df_variacao_maior_que_2.sort_index

# ============================================================================
# ADICIONE DAQUI PARA BAIXO (SUBSTITUA A LINHA INCOMPLETA ACIMA)
# ============================================================================

# Ordenar as capitais filtradas em ordem alfabética
df_variacao_maior_que_2_ordenado = df_variacao_maior_que_2.sort_index()

# Renomear para df_top_5_capitais para compatibilidade com o código novo
df_top_5_capitais = df_variacao_maior_que_2_ordenado

print("\n" + "="*100)
print("📝 GERAÇÃO DE RELATÓRIO AUTOMÁTICO")
print("="*100)

# Converter as datas para o formato desejado (%d-%m-%Y)
semana_anterior_formatada = semana_anterior_data.strftime("%d/%m/%Y")
semana_atual_formatada = semana_atual_data.strftime("%d/%m/%Y")

# Solicitar a ultima data da semana_atual
nova_data = input("Por favor, informe a data da ultima data da semana_atual no formato (dia/mês/ano): ")
nova_data_formatada = datetime.strptime(nova_data, "%d/%m/%Y")

# Calcular a data da semana anterior à próxima semana
semana_anterior_proxima = nova_data_formatada - timedelta(days=7)

# Construir o texto
texto = f"""
O Painel do Diesel foi atualizado com os dados da semana {semana_atual_formatada} a {nova_data_formatada.strftime("%d/%m/%Y")}.

- Conforme análise comparativa da semana {semana_anterior_formatada} a {semana_anterior_proxima.strftime("%d/%m/%Y")} com a semana {semana_atual_formatada} a {nova_data_formatada.strftime("%d/%m/%Y")}, foram identificados alguns indicadores relevantes, sendo eles:
"""

# Exibir o texto
print(texto)

# Obter os preços atuais de diesel comum e diesel S10 para cada capital
precos_atuais = df_resultado_limpo_atual.set_index('CAPITAIS')[['OLEO DIESEL', 'OLEO DIESEL S10']]

# Filtrar os valores de variação maiores que 2% ou menores que -2%
filtro_variacao_maior_que_2 = df_top_5_capitais[('Variação Percentual', 'OLEO DIESEL')].abs() > 2
filtro_variacao_menor_que_menos2 = df_top_5_capitais[('Variação Percentual', 'OLEO DIESEL')].abs() < -2
filtro_variacao_diesel_s10_maior_que_2 = df_top_5_capitais[('Variação Percentual', 'OLEO DIESEL S10')].abs() > 2
filtro_variacao_diesel_s10_menor_que_menos2 = df_top_5_capitais[('Variação Percentual', 'OLEO DIESEL S10')].abs() < -2

# Aplicar os filtros
df_top_5_capitais_filtrado = df_top_5_capitais[(filtro_variacao_maior_que_2 | filtro_variacao_menor_que_menos2 | filtro_variacao_diesel_s10_maior_que_2 | filtro_variacao_diesel_s10_menor_que_menos2)]

# Inicializar uma lista para armazenar as informações de cada capital
texto_capitais = []

# Percorrer as capitais do DataFrame filtrado
for capital, variacoes in df_top_5_capitais_filtrado.iterrows():
    # Inicializar uma lista para armazenar as informações de variação para a capital atual
    texto_variacoes = []
    
    # Verificar se há variação de preço do diesel comum maior que 2% ou menor que -2%
    if abs(variacoes[('Variação Percentual', 'OLEO DIESEL')]) > 2:
        # Extrair informações da variação do diesel comum
        var_diesel_comum_percentual = variacoes[('Variação Percentual', 'OLEO DIESEL')]
        preco_atual_diesel_comum = precos_atuais.loc[capital, 'OLEO DIESEL']
        # Gerar o texto correspondente
        if var_diesel_comum_percentual > 0:
            texto_diesel_comum = f"Aumentou o valor do diesel comum em {abs(var_diesel_comum_percentual):.2f}% (Valor atual: R$ {preco_atual_diesel_comum:.2f})."
        elif var_diesel_comum_percentual < 0:
            texto_diesel_comum = f"Diminuiu o valor do diesel comum em {abs(var_diesel_comum_percentual):.2f}% (Valor atual: R$ {preco_atual_diesel_comum:.2f})."
        texto_variacoes.append(texto_diesel_comum)
        
    # Verificar se há variação de preço do diesel S10 maior que 2% ou menor que -2%
    if abs(variacoes[('Variação Percentual', 'OLEO DIESEL S10')]) > 2:
        # Extrair informações da variação do diesel S10
        var_diesel_s10_percentual = variacoes[('Variação Percentual', 'OLEO DIESEL S10')]
        preco_atual_diesel_s10 = precos_atuais.loc[capital, 'OLEO DIESEL S10']
        # Gerar o texto correspondente
        if var_diesel_s10_percentual > 0:
            texto_diesel_s10 = f"Aumentou o valor do diesel S10 em {abs(var_diesel_s10_percentual):.2f}% (Valor atual: R$ {preco_atual_diesel_s10:.2f})."
        elif var_diesel_s10_percentual < 0:
            texto_diesel_s10 = f"Diminuiu o valor do diesel S10 em {abs(var_diesel_s10_percentual):.2f}% (Valor atual: R$ {preco_atual_diesel_s10:.2f})."
        texto_variacoes.append(texto_diesel_s10)
        
    # Adicionar o nome da capital ao texto
    texto_capital = capital + " – " + " E ".join(texto_variacoes)
    texto_capitais.append(texto_capital)

# Concatenar os textos das capitais
texto_parte2_corrigido = "\n".join(texto_capitais)

# Adicionar informações de São Paulo mesmo se não houver variação maior que 2% ou menor que -2%
var_diesel_comum_sp = df_resultado_limpo_atual.loc[df_resultado_limpo_atual['CAPITAIS'] == 'SAO PAULO', 'OLEO DIESEL'].values[0]
var_diesel_s10_sp = df_resultado_limpo_atual.loc[df_resultado_limpo_atual['CAPITAIS'] == 'SAO PAULO', 'OLEO DIESEL S10'].values[0]

texto_diesel_s10_sp = ""
texto_diesel_comum_sp = ""

# Inicializar uma lista para armazenar as informações de variação para São Paulo
texto_variacoes_sp = []

# Verificar se houve variação de preço do diesel comum em São Paulo
if 'SAO PAULO' in df_top_5_capitais.index:
    variacao_percentual_diesel_comum_sp = df_top_5_capitais.loc['SAO PAULO', ('Variação Percentual', 'OLEO DIESEL')]
    variacao_percentual_diesel_s10_sp = df_top_5_capitais.loc['SAO PAULO', ('Variação Percentual', 'OLEO DIESEL S10')]
    
    if abs(variacao_percentual_diesel_comum_sp) > 2:
        # Gerar o texto correspondente
        if variacao_percentual_diesel_comum_sp > 0:
            texto_diesel_comum_sp = f"Aumentou o valor do diesel comum em {abs(variacao_percentual_diesel_comum_sp):.2f}% (Valor atual: R$ {var_diesel_comum_sp:.2f})."
        elif variacao_percentual_diesel_comum_sp < 0:
            texto_diesel_comum_sp = f"Diminuiu o valor do diesel comum em {abs(variacao_percentual_diesel_comum_sp):.2f}% (Valor atual: R$ {var_diesel_comum_sp:.2f})."        
        texto_variacoes_sp.append(texto_diesel_comum_sp)

    # Verificar se houve variação de preço do diesel S10 em São Paulo
    if abs(variacao_percentual_diesel_s10_sp) > 2:
        # Gerar o texto correspondente
        if variacao_percentual_diesel_s10_sp > 0:
            texto_diesel_s10_sp = f"Aumentou o valor do diesel S10 em {abs(variacao_percentual_diesel_s10_sp):.2f}% (Valor atual: R$ {var_diesel_s10_sp:.2f})."
        elif variacao_percentual_diesel_s10_sp < 0:
            texto_diesel_s10_sp = f"Diminuiu o valor do diesel S10 em {abs(variacao_percentual_diesel_s10_sp):.2f}% (Valor atual: R$ {var_diesel_s10_sp:.2f})."        
        texto_variacoes_sp.append(texto_diesel_s10_sp)

    # Adicionar São Paulo ao texto das capitais se houver variações
    if texto_variacoes_sp:
        texto_sp = "SAO PAULO – " + " E ".join(texto_variacoes_sp)
        texto_capitais.append(texto_sp)

# Concatenar os textos das capitais, incluindo uma linha em branco após cada capital
texto_parte2_final = "\n\n".join(texto_capitais)

# Exibir o texto final
print("\n📊 ANÁLISE DAS VARIAÇÕES SIGNIFICATIVAS:")
print(texto_parte2_final)

# Calcular o preço médio para cada capital
df_resultado_limpo_atual['Preço Médio'] = (df_resultado_limpo_atual['OLEO DIESEL S10'] + df_resultado_limpo_atual['OLEO DIESEL']) / 2

# Converter a coluna 'Preço Médio' para tipo numérico
df_resultado_limpo_atual['Preço Médio'] = pd.to_numeric(df_resultado_limpo_atual['Preço Médio'])

# Encontrar o índice do preço médio mais alto e mais baixo
indice_diesel_mais_caro = df_resultado_limpo_atual['Preço Médio'].idxmax()
indice_diesel_mais_barato = df_resultado_limpo_atual['Preço Médio'].idxmin()

# Extrair as informações das capitais com os preços mais caro e mais barato
capital_diesel_mais_caro = df_resultado_limpo_atual.loc[indice_diesel_mais_caro, 'CAPITAIS']
capital_diesel_mais_barato = df_resultado_limpo_atual.loc[indice_diesel_mais_barato, 'CAPITAIS']

# Retornar os resultados formatados
print("\n" + "="*80)
print("🏆 RANKING DE PREÇOS")
print("="*80)
print("\n*A capital com diesel MAIS CARO é", capital_diesel_mais_caro, "com os seguintes valores:")
print("Diesel S10:", "R$", df_resultado_limpo_atual.loc[indice_diesel_mais_caro, 'OLEO DIESEL S10'])
print("Diesel comum:", "R$", df_resultado_limpo_atual.loc[indice_diesel_mais_caro, 'OLEO DIESEL'])
print("\n*A capital com diesel MAIS BARATO é", capital_diesel_mais_barato, "com os seguintes valores:")
print("Diesel S10:", "R$", df_resultado_limpo_atual.loc[indice_diesel_mais_barato, 'OLEO DIESEL S10'])
print("Diesel comum:", "R$", df_resultado_limpo_atual.loc[indice_diesel_mais_barato, 'OLEO DIESEL'])

print("\n" + "="*100)
print("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
print("="*100)
