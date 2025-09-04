# =============================================================================
# BLOCO 1: IMPORTAÇÃO DE BIBLIOTECAS
# =============================================================================
import pandas as pd
import numpy as np
import py7zr
import io
import os
import re
from pathlib import Path
from ftplib import FTP
from dotenv import load_dotenv

# Bibliotecas Google
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

print("[INFO] Bibliotecas importadas com sucesso!")

# =============================================================================
# BLOCO 2: FUNÇÕES 
# =============================================================================

# --- Funções de Autenticação e Conexão ---
def autenticar_google_drive(credentials_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        print("Google Drive autenticado com sucesso (somente leitura).")
        return drive_service
    except Exception as e:
        print(f"Erro ao autenticar Google Drive: {e}")
        return None

def criar_cliente_bigquery(credentials_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
            location="southamerica-east1"
        )
        print(f"Cliente BigQuery criado com sucesso para o projeto '{credentials.project_id}'.")
        return client
    except Exception as e:
        print(f"Erro ao criar cliente BigQuery: {e}")
        return None

def conectar_ftp(host, base_path):
    try:
        ftp = FTP(host)
        ftp.login()
        ftp.encoding = 'latin-1'
        ftp.cwd(base_path)
        return ftp
    except Exception as e:
        print(f"Erro ao conectar ao FTP: {str(e)}")
        return None

# --- Funções de Interação com o Usuário e FTP ---
def listar_itens(ftp):
    itens = []
    ftp.retrlines('NLST', itens.append)
    return itens

def listar_subdiretorios(ftp, itens):
    subdirs = []
    original_cwd = ftp.pwd()
    for item in itens:
        try:
            ftp.cwd(item)
            ftp.cwd(original_cwd)
            subdirs.append(item)
        except Exception:
            pass
    return subdirs

def listar_arquivos_7z(ftp, itens):
    return [item for item in itens if item.endswith(".7z")]

def escolher_item(itens_disponiveis, tipo="diretório"):
    itens_com_opcao_sair = itens_disponiveis + ["ENCERRAR CONSULTA"]
    print(f"\n{tipo.capitalize()}s disponíveis:")
    for i, item in enumerate(itens_com_opcao_sair):
        print(f"[{i}] {item}")

    while True:
        try:
            idx_item = int(input(f"\nDigite o número do {tipo} para acessar (ou escolha 'ENCERRAR CONSULTA'): "))
            if 0 <= idx_item < len(itens_com_opcao_sair):
                escolhido = itens_com_opcao_sair[idx_item]
                if escolhido == "ENCERRAR CONSULTA":
                    print("Consulta encerrada.")
                    return None
                return escolhido
            else:
                print("Número inválido. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Digite um número.")

# --- Funções de Manipulação de Arquivos e Dados ---
def acessar_arquivo_drive(drive_service, drive_id):
    try:
        nome_do_arquivo_drive = "DESCRIÇÃO - CAGED.xlsx"
        query = f"name = '{nome_do_arquivo_drive}' and trashed = false"
        response = drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name)',
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora='drive', driveId=drive_id
        ).execute()
        files = response.get('files', [])
        if files:
            return files[0].get('id')
        else:
            print(f"Arquivo '{nome_do_arquivo_drive}' não encontrado no Drive com ID '{drive_id}'.")
            return None
    except HttpError as error:
        print(f"Erro HTTP ao acessar arquivo no Drive: {error}")
        return None
    except Exception as e:
        print(f"Erro geral ao acessar arquivo no Drive: {e}")
        return None

def baixar_arquivo(ftp, nome_arquivo, local_download_dir):
    local_arquivo = os.path.join(local_download_dir, nome_arquivo)
    print(f"\nBaixando '{nome_arquivo}'...")
    with open(local_arquivo, 'wb') as f:
        ftp.retrbinary(f"RETR {nome_arquivo}", f.write)
    print(f"Arquivo salvo em: {local_arquivo}")
    return local_arquivo

def descompactar_arquivo(local_arquivo, local_download_dir):
    print("\nDescompactando...")
    nome_arquivo_txt = None
    with py7zr.SevenZipFile(local_arquivo, mode='r') as archive:
        archive.extractall(path=local_download_dir)
        for name in archive.getnames():
            if name.lower().endswith('.txt'):
                nome_arquivo_txt = os.path.join(local_download_dir, name)
                break
    
    if nome_arquivo_txt:
        print(f"Arquivo descompactado em: {nome_arquivo_txt}")
        return nome_arquivo_txt
    else:
        fallback_path = local_arquivo.replace('.7z', '.txt')
        print(f"Aviso: Não foi possível determinar o nome do arquivo .txt. Usando fallback: {os.path.basename(fallback_path)}")
        return fallback_path

def extrair_periodo_do_nome_arquivo(nome_arquivo):
    match = re.search(r'(\d{6})\.\w+$', nome_arquivo)
    if match:
        periodo = match.group(1)
        return f"{periodo[:4]}-{periodo[4:6]}"
    return None

def limpar_arquivos_brutos(local_arquivo, arquivo_txt):
    print("\nExcluindo arquivos brutos...")
    try:
        if os.path.exists(local_arquivo):
            os.remove(local_arquivo)
            print(f"Arquivo .7z removido: {local_arquivo}")
        if os.path.exists(arquivo_txt):
            os.remove(arquivo_txt)
            print(f"Arquivo .txt removido: {arquivo_txt}")
    except Exception as e:
        print(f"Erro ao limpar arquivos: {e}")

# --- Funções de Transformação de Dados (ETL) ---
def filtrar_dataframe(arquivo_txt):
    if not os.path.exists(arquivo_txt):
        print(f"Erro: O arquivo '{os.path.basename(arquivo_txt)}' não foi encontrado.")
        return None
    df = pd.read_csv(arquivo_txt, sep=";", low_memory=False, encoding='utf-8')
    df = df[df['subclasse'].astype(str).str.startswith('49302').fillna(False)]
    return df

def remover_colunas_desnecessarias(df):
    colunas_remover = [
        'seção', 'tipoempregador', 'tipoestabelecimento', 'tipomovimentação',
        'tipodedeficiência', 'indtrabintermitente', 'indtrabparcial',
        'origemdainformação', 'competênciadec', 'indicadordeforadoprazo',
        'unidadesaláriocódigo', 'valorsaláriofixo'
    ]
    colunas_existentes = [col for col in colunas_remover if col in df.columns]
    df = df.drop(columns=colunas_existentes)
    return df

def renomear_colunas(df):
    mapeamento_colunas = {
        'competênciamov': 'COD-RELATORIO', 'região': 'REGIAO', 'uf': 'UF',
        'município': 'MUNICIPIOCOD', 'subclasse': 'SUBCLASS',
        'saldomovimentação': 'SALDOMOVIMENTACAO', 'cbo2002ocupação': 'CBO2002OCUPACAO',
        'categoria': 'CATEGORIA', 'graudeinstrução': 'GRAUDEINSTRUCAO',
        'idade': 'IDADE', 'horascontratuais': 'HORASCONTRATUAIS',
        'raçacor': 'RACACOR', 'sexo': 'SEXO', 'salário': 'SALARIO',
        'tamestabjan': 'TAMESTABJAN', 'indicadoraprendiz': 'INDICADORAPRENDIZ'
    }
    colunas_existentes = {k: v for k, v in mapeamento_colunas.items() if k in df.columns}
    df = df.rename(columns=colunas_existentes)
    df.columns = df.columns.str.upper()
    return df

def carregar_arquivos_descricao(descricao_caged_path):
    try:
        workbook_desc = pd.ExcelFile(descricao_caged_path, engine='openpyxl')
        descricoes = {}
        abas = ['REGIAO', 'UF', 'MUNICIPIOS', 'CBO', 'categoria',
                'GRAU DE INSTRUCAO', 'RACA COR', 'SEXO', 'FAIXA ETARIA']
        
        for aba in abas:
            sheet_name_found = None
            for sheet in workbook_desc.sheet_names:
                if sheet.upper() == aba.upper():
                    sheet_name_found = sheet
                    break
            
            if sheet_name_found:
                df_temp = pd.read_excel(workbook_desc, sheet_name=sheet_name_found)
                descricoes[aba.upper()] = df_temp
        return descricoes
    except Exception as e:
        print(f"Erro ao carregar arquivo de descrição: {e}")
        return {}

def adicionar_colunas(df_tratado):
    colunas_para_adicionar = [
        ('COD-RELATORIO', 'ANO', '0'), ('ANO', 'MES', '0'), ('MES', 'MES_NUM', '0'),
        ('MUNICIPIOCOD', 'MUNICIPIO', '0'), ('MUNICIPIO', 'BASE', '0'),
        ('SALDOMOVIMENTACAO', 'SITUACAO', '0'), ('CBO2002OCUPACAO', 'DESCCBO', '0'),
        ('DESCCBO', 'DESCATIVIDADE', '0'), ('DESCATIVIDADE', 'AREA', '0'),
        ('GRAUDEINSTRUCAO', 'RESUMOGRAUDEINSTRUCAO', '0'), ('SALARIO', 'CARGOTRADICIONALTRC', '0'),
        ('CARGOTRADICIONALTRC', 'TETOSALARIO', '0'), ('TETOSALARIO', 'COMPARATIVOTETO', '0'),
        ('INDICADORAPRENDIZ', 'FAIXAETARIA', '0'), ('FAIXAETARIA', 'MODELOCONTRATACAO', '0'),
        ('MODELOCONTRATACAO', 'ADMISSOES', '0'), ('ADMISSOES', 'DEMISSOES', '0')
    ]
    for col_ref, nova_col, valor_default in colunas_para_adicionar:
        if col_ref in df_tratado.columns and nova_col not in df_tratado.columns:
            df_tratado.insert(df_tratado.columns.get_loc(col_ref) + 1, nova_col, valor_default)
    return df_tratado

def converter_colunas_float(df):
    colunas_float = ['HORASCONTRATUAIS', 'SALARIO', 'TETOSALARIO']
    for col in colunas_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    return df

def traduzir_colunas(df_tratado, descricoes):
    if not descricoes:
        print("Dicionário de descrições vazio. Nenhuma tradução será aplicada.")
        return df_tratado

    mapeamentos = [
        ('REGIAO', 'REGIAO', 'Códigos', 'Descrição', 'REGIAO'),
        ('UF', 'UF', 'Códigos', 'Descrição', 'UF'),
        ('MUNICIPIOCOD', 'MUNICIPIOS', 'Códigos', 'BASE', 'BASE'),
        ('MUNICIPIOCOD', 'MUNICIPIOS', 'Códigos', 'Descrição', 'MUNICIPIO'),
        ('CBO2002OCUPACAO', 'CBO', 'Códigos', 'Descrição', 'DESCCBO'),
        ('CBO2002OCUPACAO', 'CBO', 'Códigos', 'Atividade', 'DESCATIVIDADE'),
        ('CBO2002OCUPACAO', 'CBO', 'Códigos', 'Área', 'AREA'),
        ('CBO2002OCUPACAO', 'CBO', 'Códigos', 'CARGO TRADICIONAL DO TRC?', 'CARGOTRADICIONALTRC'),
        ('CBO2002OCUPACAO', 'CBO', 'Códigos', 'Teto salarial', 'TETOSALARIO'),
        ('CATEGORIA', 'CATEGORIA', 'Códigos', 'ModeloContratacao', 'MODELOCONTRATACAO'),
        ('CATEGORIA', 'CATEGORIA', 'Códigos', 'Descrição', 'CATEGORIA'),
        ('GRAUDEINSTRUCAO', 'GRAU DE INSTRUCAO', 'Códigos', 'Resumo', 'RESUMOGRAUDEINSTRUCAO'),
        ('GRAUDEINSTRUCAO', 'GRAU DE INSTRUCAO', 'Códigos', 'Descrição', 'GRAUDEINSTRUCAO'),
        ('RACACOR', 'RACA COR', 'Códigos', 'Descrição', 'RACACOR'),
        ('SEXO', 'SEXO', 'Códigos', 'Descrição', 'SEXO'),
        ('IDADE', 'FAIXA ETARIA', 'Códigos', 'Descrição', 'FAIXAETARIA')
    ]

    for col_origem, nome_desc, col_codigo, col_desc, col_destino in mapeamentos:
        try:
            nome_desc_upper = nome_desc.upper()
            if col_origem in df_tratado.columns and col_destino in df_tratado.columns and nome_desc_upper in descricoes:
                df_desc = descricoes[nome_desc_upper]
                df_desc[col_codigo] = pd.to_numeric(df_desc[col_codigo], errors='coerce')
                df_tratado[col_origem] = pd.to_numeric(df_tratado[col_origem], errors='coerce')
                df_desc = df_desc.dropna(subset=[col_codigo, col_desc])
                dicionario = dict(zip(df_desc[col_codigo], df_desc[col_desc]))
                df_tratado[col_destino] = df_tratado[col_origem].map(dicionario).fillna(df_tratado[col_destino])
        except Exception as e:
            print(f"Aviso: Erro ao aplicar mapeamento para '{col_destino}': {str(e)}")

    return df_tratado

def inferir_data_colunas(df_tratado):
    meses = {'01': 'JANEIRO', '02': 'FEVEREIRO', '03': 'MARÇO', '04': 'ABRIL', '05': 'MAIO',
             '06': 'JUNHO', '07': 'JULHO', '08': 'AGOSTO', '09': 'SETEMBRO', '10': 'OUTUBRO',
             '11': 'NOVEMBRO', '12': 'DEZEMBRO'}
    meses_num = {v: k for k, v in meses.items()}

    if 'COD-RELATORIO' in df_tratado.columns:
        # CORREÇÃO: Renomeando a variável para usar underscore
        cod_relatorio_str = df_tratado['COD-RELATORIO'].astype(str)
        if 'ANO' in df_tratado.columns:
            # CORREÇÃO: Usando o nome correto da variável
            df_tratado['ANO'] = cod_relatorio_str.str[:4]
        if 'MES' in df_tratado.columns:
            # CORREÇÃO: Usando o nome correto da variável
            df_tratado['MES'] = cod_relatorio_str.str[4:6].map(meses)
        if 'MES_NUM' in df_tratado.columns and 'MES' in df_tratado.columns:
            df_tratado['MES_NUM'] = df_tratado['MES'].map(meses_num)
    return df_tratado
    
def processar_salarios_situacao(df_tratado):
    df_tratado['SALARIO'] = pd.to_numeric(df_tratado['SALARIO'], errors='coerce').fillna(0)
    df_tratado['TETOSALARIO'] = pd.to_numeric(df_tratado['TETOSALARIO'], errors='coerce').fillna(0)

    if 'COMPARATIVOTETO' in df_tratado.columns:
        df_tratado['COMPARATIVOTETO'] = np.where(df_tratado['SALARIO'] > df_tratado['TETOSALARIO'], 'Maior', 'Menor')

    df_tratado['SALARIO'] = np.where(
        (df_tratado['SALARIO'] > df_tratado['TETOSALARIO']) & (df_tratado['TETOSALARIO'] > 0),
        df_tratado['TETOSALARIO'],
        df_tratado['SALARIO']
    )

    if 'SALDOMOVIMENTACAO' in df_tratado.columns:
        saldo_mov = pd.to_numeric(df_tratado['SALDOMOVIMENTACAO'], errors='coerce')
        if 'SITUACAO' in df_tratado.columns:
            df_tratado['SITUACAO'] = np.where(saldo_mov == 1, 'ADMITIDO', 'DEMITIDO')
        if 'ADMISSOES' in df_tratado.columns:
            df_tratado['ADMISSOES'] = (saldo_mov == 1).astype(int)
        if 'DEMISSOES' in df_tratado.columns:
            df_tratado['DEMISSOES'] = (saldo_mov == -1).astype(int)

    return df_tratado

# --- Função de Carga para o BigQuery ---

def enviar_para_bigquery(df_tratado, nome_arquivo, client):
    """
    Envia o DataFrame tratado para o BigQuery, substituindo a tabela se já existir.
    """
    periodo = extrair_periodo_do_nome_arquivo(nome_arquivo)
    if not periodo:
        print(f"Erro: Não foi possível extrair o período do nome do arquivo: {nome_arquivo}")
        return False

    dataset_id = "CAGED_TRATADO"
    
    table_id = periodo
    
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    print(f"\nEnviando dados para a tabela BigQuery: {table_ref}")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        print(f"Enviando {len(df_tratado)} linhas para o BigQuery...")
        job = client.load_table_from_dataframe(df_tratado, table_ref, job_config=job_config)
        job.result()
        print(f"Upload para a tabela {table_ref} concluído com sucesso!")
        return True
    except Exception as e:
        print(f"Erro ao enviar dados para o BigQuery: {str(e)}")
        # Adiciona uma mensagem de ajuda específica para o erro de nome de tabela
        if 'Invalid table ID' in str(e):
            print("\n[AVISO] O erro 'Invalid table ID' indica que o BigQuery não aceitou o nome da tabela com hífen.")
            print("Para corrigir, a substituição de '-' por '_' precisa ser reativada no código.")
        return False


# =============================================================================
# BLOCO 3: FUNÇÃO PRINCIPAL (MAIN)
# =============================================================================
def main():
    try:
        # --- Configurações (Carregadas do .env) ---
        ftp_host = "ftp.mtps.gov.br"
        base_path = "/pdet/microdados/NOVO CAGED"
        
        # Carrega variáveis sensíveis do ambiente
        credentials_path = os.getenv("GCP_CREDENTIALS_PATH")
        drive_compartilhado_id = os.getenv("GDRIVE_SHARED_DRIVE_ID")
        local_download_dir = os.getenv("LOCAL_DOWNLOAD_DIR", os.path.join(str(Path.home()), "Downloads"))

        if not credentials_path or not os.path.exists(credentials_path):
            print(f"ERRO CRÍTICO: A variável de ambiente 'GCP_CREDENTIALS_PATH' não está definida ou o caminho é inválido.")
            return

        os.makedirs(local_download_dir, exist_ok=True)
        print(f"Diretório temporário para download: {local_download_dir}")

        # --- Autenticação ---
        drive_service = autenticar_google_drive(credentials_path)
        client = criar_cliente_bigquery(credentials_path)
        if not drive_service or not client:
            print("Falha na autenticação. Encerrando.")
            return

        # --- Download do Arquivo de Descrição ---
        descricao_caged_path = os.path.join(local_download_dir, "DESCRIÇÃO - CAGED.xlsx")
        print("\nVerificando arquivo de descrição do Google Drive...")
        arquivo_id = acessar_arquivo_drive(drive_service, drive_compartilhado_id)
        if not arquivo_id:
            print("ERRO: Arquivo de descrição CAGED não encontrado.")
            return

        print("Baixando arquivo de descrição do Drive...")
        request = drive_service.files().get_media(fileId=arquivo_id)
        with io.FileIO(descricao_caged_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Download do arquivo de descrição: {int(status.progress() * 100)}%")
        print(f"Arquivo de descrição baixado para: {descricao_caged_path}")

        # --- Conexão e Navegação FTP ---
        print("\nConectando ao servidor FTP...")
        ftp = conectar_ftp(ftp_host, base_path)
        if not ftp: return

        caminho_atual = base_path
        nome_arquivo_7z = None

        while nome_arquivo_7z is None:
            print(f"\nNavegando em: {caminho_atual}")
            itens = listar_itens(ftp)
            subdirs = listar_subdiretorios(ftp, itens)
            arquivos_7z = listar_arquivos_7z(ftp, itens)

            if arquivos_7z:
                print("Arquivos .7z encontrados no diretório atual.")
                nome_arquivo_7z = escolher_item(arquivos_7z, tipo="arquivo")
                if nome_arquivo_7z is None:
                    ftp.quit()
                    return
            elif subdirs:
                print("Nenhum arquivo .7z. Navegando para subdiretórios...")
                escolha = escolher_item(subdirs, tipo="diretório")
                if escolha is None:
                    ftp.quit()
                    return
                ftp.cwd(escolha)
                caminho_atual = os.path.join(caminho_atual, escolha).replace("\\", "/")
            else:
                print("Nenhum arquivo .7z ou subdiretório encontrado.")
                ftp.quit()
                return

        # --- Download e Descompactação ---
        local_arquivo_7z = baixar_arquivo(ftp, nome_arquivo_7z, local_download_dir)
        ftp.quit()
        arquivo_txt = descompactar_arquivo(local_arquivo_7z, local_download_dir)
        
        # --- Processamento dos Dados (ETL) ---
        print("\nIniciando processamento dos dados...")
        df = filtrar_dataframe(arquivo_txt)
        if df is None or df.empty:
             print("Nenhum registro encontrado para a subclasse 49302. Encerrando.")
             limpar_arquivos_brutos(local_arquivo_7z, arquivo_txt)
             return
        print(f"Registros filtrados: {len(df)}")

        df_tratado = remover_colunas_desnecessarias(df)
        df_tratado = renomear_colunas(df_tratado)

        print("\nCarregando dicionários de dados para enriquecimento...")
        descricoes = carregar_arquivos_descricao(descricao_caged_path)

        print("\nEnriquecendo os dados...")
        df_tratado = adicionar_colunas(df_tratado)
        df_tratado = traduzir_colunas(df_tratado, descricoes)
        df_tratado = converter_colunas_float(df_tratado)
        df_tratado = inferir_data_colunas(df_tratado)
        df_tratado = processar_salarios_situacao(df_tratado)

        print("\nConvertendo todas as colunas para STRING antes do envio ao BigQuery...")
        for col in df_tratado.columns:
            df_tratado[col] = df_tratado[col].astype(str)

        sucesso = enviar_para_bigquery(df_tratado, nome_arquivo_7z, client)
        if sucesso:
            print("\nDados enviados com sucesso para o BigQuery!")
        else:
            print("\nHouve um problema ao enviar os dados para o BigQuery.")

        # --- Limpeza Final ---
        limpar_arquivos_brutos(local_arquivo_7z, arquivo_txt)
        if os.path.exists(descricao_caged_path):
            os.remove(descricao_caged_path)
            print(f"Arquivo de descrição removido: {descricao_caged_path}")
            
        print(f"\nProcessamento do arquivo {nome_arquivo_7z} concluído!")

    except Exception as e:
        print(f"\n--- ERRO INESPERADO NO FLUXO PRINCIPAL ---")
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()

# =============================================================================
# BLOCO 4: PONTO DE ENTRADA DO SCRIPT
# =============================================================================
if __name__ == "__main__":
    main()
