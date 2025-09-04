import ftplib
import os
import sys
import pandas as pd
import py7zr
import time
import gc
import re
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import sys
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# ==============================================================================
# CLASSE PRINCIPAL - RAIS LOADER MELHORADO
# ==============================================================================

class ImprovedRAISLoader:
    def __init__(self, config: Dict[str, Any]):
        """Inicializa o loader RAIS com estilo melhorado"""
        
        self.config = config
        
        # Configurações de resilência
        self.max_retries = 3
        self.retry_delay = 10
        self.backoff_multiplier = 2
        
        # Arquivos de controle
        self.progress_file = "rais_progress.json"
        self.progress = self._load_progress()
        self.dicionarios = {}
        self.client_bq = None
        
        # Criar diretórios
        self._create_directories()

    def _create_directories(self):
        """Cria diretórios necessários"""
        os.makedirs(self.config['DIRETORIO_TEMPORARIO'], exist_ok=True)
        os.makedirs(self.config['DIRETORIO_TRATADO'], exist_ok=True)
        print("DIRETORIOS: Estrutura criada/verificada com sucesso")

    def _load_progress(self) -> Dict:
        """Carrega estado do progresso"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        
        return {
            'session_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'current_year': None,
            'files_status': {},
            'last_update': None
        }

    def _save_progress(self):
        """Salva progresso atual"""
        self.progress['last_update'] = datetime.now().isoformat()
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            print(f"ERRO PROGRESS: Falha ao salvar progresso: {e}")

    def _print_separator(self, char="=", length=70):
        """Imprime separador visual"""
        print(char * length)

    def _print_header(self, text: str):
        """Imprime cabeçalho formatado"""
        self._print_separator()
        print(f"  {text}")
        self._print_separator()

    def _print_step(self, step: str, description: str):
        """Imprime etapa do processo"""
        print(f"\n{step}: {description}")

    def _execute_with_retry(self, operation_name: str, operation_func, *args, **kwargs):
        """Executa operação com retry automático e logs melhorados"""
        for attempt in range(self.max_retries + 1):
            try:
                return operation_func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                is_connection_error = any(keyword in error_str for keyword in 
                                        ['connection', 'timeout', 'network', 'ssl', 'socket', 'ftp'])
                
                if attempt < self.max_retries and is_connection_error:
                    wait_time = self.retry_delay * (self.backoff_multiplier ** attempt)
                    print(f"RETRY: Tentativa {attempt + 1} falhou para {operation_name}")
                    print(f"AGUARDANDO: {wait_time}s antes da próxima tentativa...")
                    time.sleep(wait_time)
                else:
                    print(f"ERRO {operation_name.upper()}: {e}")
                    break
        raise Exception(f"Operação {operation_name} falhou após {self.max_retries + 1} tentativas")

    def criar_cliente_bigquery(self) -> bool:
        """Cria e testa cliente BigQuery"""
        try:
            print("BIGQUERY: Inicializando conexão...")
            
            credentials = service_account.Credentials.from_service_account_file(
                self.config['CAMINHO_CREDENCIAL_BQ'],
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            
            self.client_bq = bigquery.Client(
                credentials=credentials,
                project=self.config['PROJECT_ID_BQ'],
                location=self.config['LOCATION_BQ']
            )
            
            # Testa conectividade
            self.client_bq.get_dataset(self.config['DATASET_ID_BQ'])
            print("BIGQUERY: ✅ Conexão estabelecida com sucesso")
            return True
            
        except Exception as e:
            print(f"ERRO BIGQUERY: Falha na conexão - {e}")
            return False

    def obter_anos_disponiveis(self) -> List[str]:
        """Obtém anos disponíveis no FTP, incluindo nomes como '2024 parcial'."""
        print("FTP: Consultando anos disponíveis...")
        
        def get_years():
            with ftplib.FTP(self.config['FTP_HOST'], timeout=60) as ftp:
                ftp.login()
                ftp.cwd(self.config['FTP_BASE_PATH'])
                
                # --- LÓGICA CORRIGIDA AQUI ---
                # Usa regex para encontrar todos os itens que começam com 4 dígitos (o ano)
                anos_encontrados = [item for item in ftp.nlst() if re.match(r'^\d{4}', item)]
                
                # Ordena de forma decrescente para ter o mais recente primeiro
                return sorted(anos_encontrados, reverse=True)
        
        try:
            anos = self._execute_with_retry("consulta anos FTP", get_years)
            print(f"FTP: ✅ Encontrados {len(anos)} anos/diretórios disponíveis")
            return anos
        except Exception as e:
            print(f"ERRO FTP: Falha ao obter anos - {e}")
            return []

    def obter_arquivos_ano(self, ano: str) -> List[str]:
        """Obtém arquivos para um ano específico"""
        print(f"FTP: Consultando arquivos do ano {ano}...")
        
        def get_files():
            with ftplib.FTP(self.config['FTP_HOST'], timeout=60) as ftp:
                ftp.login()
                ftp.cwd(f"{self.config['FTP_BASE_PATH']}{ano}/")
                arquivos = [f for f in ftp.nlst() 
                          if f.endswith('.7z') and f not in self.config['ARQUIVOS_A_EXCLUIR']]
                return arquivos
        
        try:
            arquivos = self._execute_with_retry(f"consulta arquivos {ano}", get_files)
            print(f"FTP: ✅ Encontrados {len(arquivos)} arquivos para processar")
            return arquivos
        except Exception as e:
            print(f"ERRO FTP: Falha ao obter arquivos do ano {ano} - {e}")
            return []

    def verificar_status_arquivo(self, ano: str, nome_arquivo: str) -> str:
        """Verifica status de um arquivo específico"""
        file_key = f"{ano}:{nome_arquivo}"
        status = self.progress['files_status'].get(file_key, {})
        return status.get('stage', 'NOT_STARTED')

    def atualizar_status_arquivo(self, ano: str, nome_arquivo: str, stage: str, 
                               success: bool = True, info: Dict = None):
        """Atualiza status de um arquivo"""
        file_key = f"{ano}:{nome_arquivo}"
        
        if file_key not in self.progress['files_status']:
            self.progress['files_status'][file_key] = {}
        
        self.progress['files_status'][file_key].update({
            'stage': stage,
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'info': info or {}
        })
        
        self._save_progress()

    def baixar_arquivo(self, ano: str, nome_arquivo: str, caminho_local: str) -> bool:
        """Baixa arquivo do FTP"""
        
        # Verifica se já foi baixado
        current_status = self.verificar_status_arquivo(ano, nome_arquivo)
        if current_status == 'DOWNLOADED' and os.path.exists(caminho_local):
            print(f"SKIP: {nome_arquivo} já foi baixado anteriormente")
            return True
        
        print(f"DOWNLOAD: Iniciando {nome_arquivo}...")
        
        def download_operation():
            with ftplib.FTP(self.config['FTP_HOST'], timeout=60) as ftp:
                ftp.login()
                ftp.encoding = 'latin-1'
                caminho_remoto = f"{self.config['FTP_BASE_PATH']}{ano}/"
                ftp.cwd(caminho_remoto)
                
                with open(caminho_local, 'wb') as f:
                    ftp.retrbinary('RETR ' + nome_arquivo, f.write)
        
        try:
            self._execute_with_retry(f"download {nome_arquivo}", download_operation)
            
            # Verifica se o arquivo foi baixado corretamente
            if os.path.exists(caminho_local) and os.path.getsize(caminho_local) > 0:
                file_size = os.path.getsize(caminho_local) / (1024*1024)  # MB
                print(f"DOWNLOAD: ✅ {nome_arquivo} concluído ({file_size:.1f} MB)")
                
                self.atualizar_status_arquivo(ano, nome_arquivo, 'DOWNLOADED', True, 
                                            {'file_size_mb': file_size})
                return True
            else:
                raise Exception("Arquivo baixado está vazio ou corrompido")
            
        except Exception as e:
            print(f"ERRO DOWNLOAD: {nome_arquivo} - {e}")
            self.atualizar_status_arquivo(ano, nome_arquivo, 'DOWNLOAD_FAILED', False, 
                                        {'error': str(e)})
            return False

    def extrair_arquivo(self, caminho_7z: str, destino: str, ano: str, nome_arquivo: str) -> Optional[str]:
        """Extrai arquivo 7z"""
        
        current_status = self.verificar_status_arquivo(ano, nome_arquivo)
        expected_txt = caminho_7z.replace('.7z', '.txt')
        
        if current_status == 'EXTRACTED' and os.path.exists(expected_txt):
            print(f"SKIP: {nome_arquivo} já foi extraído anteriormente")
            return expected_txt
        
        print(f"EXTRACAO: Processando {nome_arquivo}...")
        
        try:
            with py7zr.SevenZipFile(caminho_7z, mode='r') as z:
                z.extractall(path=destino)
            
            nome_txt = nome_arquivo.replace('.7z', '.txt')
            caminho_txt = os.path.join(destino, nome_txt)
            
            if not os.path.exists(caminho_txt):
                raise Exception(f"Arquivo extraído não encontrado: {caminho_txt}")
            
            file_size = os.path.getsize(caminho_txt) / (1024*1024)  # MB
            print(f"EXTRACAO: ✅ {nome_arquivo} concluído ({file_size:.1f} MB)")
            
            self.atualizar_status_arquivo(ano, nome_arquivo, 'EXTRACTED', True,
                                        {'extracted_file_size_mb': file_size})
            return caminho_txt
            
        except Exception as e:
            print(f"ERRO EXTRACAO: {nome_arquivo} - {e}")
            self.atualizar_status_arquivo(ano, nome_arquivo, 'EXTRACTION_FAILED', False,
                                        {'error': str(e)})
            return None

    def carregar_dicionarios(self) -> bool:
        """Carrega dicionários da RAIS"""
        if self.dicionarios:  # Já carregados
            return True
        
        print("DICIONARIOS: Carregando traduções...")
        
        try:
            dicionarios = {}
            colunas_para_traduzir = [
                'Mun Trab', 'Natureza Jurídica', 'Tamanho Estabelecimento', 'CBO Ocupação 2002',
                'Faixa Hora Contrat', 'Faixa Tempo Emprego', 'Tipo Vínculo', 'Escolaridade após 2005',
                'Nacionalidade', 'Raça Cor', 'Sexo Trabalhador', 'Tipo Defic', 'CNAE 2.0 Subclasse'
            ]
            
            xls = pd.ExcelFile(self.config['CAMINHO_DICIONARIO_EXCEL'])
            
            for coluna in colunas_para_traduzir:
                try:
                    df_dict = pd.read_excel(xls, sheet_name=coluna, dtype={0: str})
                    cod_col_name = df_dict.columns[0]
                    df_dict[cod_col_name] = df_dict[cod_col_name].str.strip()
                    
                    if coluna == 'Mun Trab':
                        mapa_municipio = pd.Series(df_dict['DESC MUNICIPIO'].values, index=df_dict['COD']).to_dict()
                        mapa_uf = pd.Series(df_dict['DESC UF'].values, index=df_dict['COD']).to_dict()
                        dicionarios[coluna] = {'municipio': mapa_municipio, 'uf': mapa_uf}
                    else:
                        desc_col_name = df_dict.columns[1]
                        mapa = pd.Series(df_dict[desc_col_name].values, index=df_dict[cod_col_name]).to_dict()
                        dicionarios[coluna] = mapa
                        
                except Exception as e:
                    print(f"AVISO DICIONARIO: Falha ao carregar '{coluna}' - {e}")
                    dicionarios[coluna] = {}
            
            self.dicionarios = dicionarios
            print(f"DICIONARIOS: ✅ {len(dicionarios)} dicionários carregados")
            return True
            
        except Exception as e:
            print(f"ERRO DICIONARIOS: Falha crítica - {e}")
            return False

    def processar_arquivo_rais(self, caminho_txt: str, caminho_csv_saida: str, 
                              ano: str, nome_arquivo_original: str) -> bool:
        """Processa arquivo RAIS em chunks"""
        
        current_status = self.verificar_status_arquivo(ano, nome_arquivo_original)
        if current_status == 'PROCESSED' and os.path.exists(caminho_csv_saida):
            print(f"SKIP: {nome_arquivo_original} já foi processado anteriormente")
            return True
        
        print(f"PROCESSAMENTO: Iniciando {nome_arquivo_original}...")
        
        try:
            colunas_iniciais = [
                'CNAE 2.0 Subclasse', 'Mun Trab', 'Natureza Jurídica', 'Tamanho Estabelecimento',
                'CBO Ocupação 2002', 'Faixa Hora Contrat', 'Faixa Tempo Emprego', 'Tipo Vínculo',
                'Escolaridade após 2005', 'Idade', 'Nacionalidade', 'Raça Cor', 'Sexo Trabalhador',
                'Tipo Defic', 'Vl Remun Média Nom', 'Vínculo Ativo 31/12'
            ]
            
            primeira_escrita = True
            total_processados = 0
            chunk_count = 0
            
            print(f"CHUNKS: Processando arquivo em chunks de {self.config['CHUNK_SIZE_PROCESSAMENTO']:,}")
            
            with pd.read_csv(
                caminho_txt, sep=';', encoding='latin-1', dtype=str,
                chunksize=self.config['CHUNK_SIZE_PROCESSAMENTO'], 
                usecols=lambda col: col in colunas_iniciais, low_memory=False
            ) as chunk_reader:
                
                for chunk_num, df_chunk in enumerate(chunk_reader):
                    chunk_count += 1
                    
                    # Filtra apenas vínculos ativos
                    df_base = df_chunk[df_chunk['Vínculo Ativo 31/12'] == "1"].copy()
                    if len(df_base) == 0:
                        continue
                    
                    df_base = df_base.drop('Vínculo Ativo 31/12', axis=1)
                    
                    # Aplica traduções
                    df_tratado_chunk = self._aplicar_traducoes(df_base)
                    
                    # Sanitiza colunas
                    df_tratado_chunk = self._sanitizar_nomes_colunas(df_tratado_chunk)
                    
                    # Salva chunk
                    modo_escrita = 'w' if primeira_escrita else 'a'
                    header = primeira_escrita
                    
                    df_tratado_chunk.to_csv(
                        caminho_csv_saida, mode=modo_escrita, header=header,
                        index=False, sep=';', encoding='utf-8-sig'
                    )
                    
                    total_processados += len(df_tratado_chunk)
                    primeira_escrita = False
                    
                    # Limpeza de memória
                    del df_chunk, df_base, df_tratado_chunk
                    gc.collect()
                    
                    # Log periódico
                    if chunk_num % 50 == 0:
                        print(f"PROGRESSO: {chunk_num + 1} chunks, {total_processados:,} registros processados")
            
            file_size = os.path.getsize(caminho_csv_saida) / (1024*1024)  # MB
            print(f"PROCESSAMENTO: ✅ {nome_arquivo_original} concluído")
            print(f"RESULTADO: {total_processados:,} registros, {chunk_count} chunks, {file_size:.1f} MB")
            
            self.atualizar_status_arquivo(ano, nome_arquivo_original, 'PROCESSED', True, {
                'total_records': total_processados,
                'chunks_processed': chunk_count,
                'output_file_size_mb': file_size
            })
            
            return True
            
        except Exception as e:
            print(f"ERRO PROCESSAMENTO: {nome_arquivo_original} - {e}")
            self.atualizar_status_arquivo(ano, nome_arquivo_original, 'PROCESSING_FAILED', False,
                                        {'error': str(e)})
            return False

    def _aplicar_traducoes(self, df_chunk: pd.DataFrame) -> pd.DataFrame:
        """Aplica traduções dos dicionários"""
        df_tratado = df_chunk.copy()
        
        for coluna, mapa in self.dicionarios.items():
            if coluna in df_tratado.columns:
                try:
                    df_tratado[coluna] = df_tratado[coluna].astype(str).str.strip()
                    
                    if coluna == 'Mun Trab':
                        df_tratado['UF'] = df_tratado['Mun Trab'].map(mapa['uf'])
                        df_tratado['Mun Trab (Traduzido)'] = df_tratado['Mun Trab'].map(mapa['municipio'])
                    elif coluna in ['CNAE 2.0 Subclasse', 'CBO Ocupação 2002']:
                        df_tratado[f'{coluna} (Traduzido)'] = df_tratado[coluna].map(mapa)
                    else:
                        df_tratado[coluna] = df_tratado[coluna].map(mapa)
                except Exception:
                    pass
        
        df_tratado.fillna('N/I', inplace=True)
        return df_tratado

    def _sanitizar_nomes_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitiza nomes das colunas para BigQuery"""
        novos_nomes = {}
        for col in df.columns:
            novo_col = col.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
            novo_col = novo_col.replace('â', 'a').replace('ê', 'e').replace('ô', 'o')
            novo_col = novo_col.replace('ã', 'a').replace('õ', 'o').replace('ç', 'c')
            novo_col = re.sub(r'[^0-9a-zA-Z_]', '_', novo_col)
            novo_col = '_'.join(filter(None, novo_col.split('_')))
            novos_nomes[col] = novo_col
        df.rename(columns=novos_nomes, inplace=True)
        return df

    def carregar_csv_para_bigquery(self, caminho_csv: str, table_ref: str, 
                                  write_disposition: str, ano: str, nome_arquivo_original: str) -> bool:
        """Carrega CSV para BigQuery"""
        
        current_status = self.verificar_status_arquivo(ano, nome_arquivo_original)
        if current_status == 'UPLOADED':
            print(f"SKIP: {nome_arquivo_original} já foi carregado no BigQuery")
            return True
        
        print(f"BIGQUERY: Carregando {nome_arquivo_original}...")
        
        def upload_operation():
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=write_disposition,
                autodetect=True,
                field_delimiter=';'
            )
            
            with open(caminho_csv, "rb") as source_file:
                job = self.client_bq.load_table_from_file(source_file, table_ref, job_config=job_config)
            
            return job.result()  # Aguarda conclusão

        try:
            job_result = self._execute_with_retry(f"upload BigQuery {nome_arquivo_original}", upload_operation)
            
            table = self.client_bq.get_table(table_ref)
            total_rows = table.num_rows
            
            print(f"BIGQUERY: ✅ {nome_arquivo_original} carregado ({total_rows:,} linhas na tabela)")
            
            self.atualizar_status_arquivo(ano, nome_arquivo_original, 'UPLOADED', True, {
                'table_total_rows': total_rows,
                'write_disposition': write_disposition
            })
            
            return True
            
        except Exception as e:
            print(f"ERRO BIGQUERY: {nome_arquivo_original} - {e}")
            self.atualizar_status_arquivo(ano, nome_arquivo_original, 'UPLOAD_FAILED', False,
                                        {'error': str(e)})
            return False

    def limpar_arquivos_temporarios(self, arquivos_para_limpar: List[str]):
        """Remove arquivos temporários"""
        if not arquivos_para_limpar:
            return
        
        print(f"LIMPEZA: Removendo {len(arquivos_para_limpar)} arquivos temporários...")
        
        removidos = 0
        for arquivo in arquivos_para_limpar:
            try:
                if os.path.exists(arquivo):
                    os.remove(arquivo)
                    removidos += 1
            except Exception as e:
                print(f"AVISO LIMPEZA: Não foi possível remover {os.path.basename(arquivo)} - {e}")
        
        print(f"LIMPEZA: ✅ {removidos} arquivos removidos")

    def gerar_relatorio_progresso(self) -> Dict:
        """Gera relatório de progresso atual"""
        status_count = {'NOT_STARTED': 0, 'DOWNLOADED': 0, 'EXTRACTED': 0, 
                       'PROCESSED': 0, 'UPLOADED': 0, 'FAILED': 0}
        
        for file_status in self.progress['files_status'].values():
            stage = file_status.get('stage', 'NOT_STARTED')
            if 'FAILED' in stage:
                status_count['FAILED'] += 1
            else:
                status_count[stage] = status_count.get(stage, 0) + 1
        
        return {
            'session_id': self.progress['session_id'],
            'current_year': self.progress['current_year'],
            'status_summary': status_count,
            'total_files': len(self.progress['files_status']),
            'last_update': self.progress['last_update']
        }

    def executar_processo_completo(self, ano: str) -> bool:
        """Executa processo completo para um ano com logs melhorados"""
        
        self._print_header(f"PROCESSAMENTO RAIS - ANO {ano}")
        
        # Inicializa estado
        self.progress['current_year'] = ano
        start_time = datetime.now()
        
        print(f"SESSAO: {self.progress['session_id']}")
        print(f"INICIO: {start_time.strftime('%H:%M:%S')}")
        
        try:
            # ETAPA 1: Inicialização
            self._print_step("ETAPA 1/6", "Inicialização dos sistemas")
            
            if not self.criar_cliente_bigquery():
                return False
            
            if not self.carregar_dicionarios():
                return False
            
            # ETAPA 2: Obtenção dos arquivos
            self._print_step("ETAPA 2/6", "Consulta de arquivos disponíveis")
            
            arquivos_ano = self.obter_arquivos_ano(ano)
            if not arquivos_ano:
                print("ERRO CONSULTA: Nenhum arquivo encontrado")
                return False
            
            # ETAPA 3: Configuração da tabela
            self._print_step("ETAPA 3/6", "Configuração da tabela BigQuery")
            
            table_id = f"{ano}-12"
            table_ref = f"{self.config['PROJECT_ID_BQ']}.{self.config['DATASET_ID_BQ']}.{table_id}"
            print(f"TABELA DESTINO: {table_ref}")
            
            # ETAPA 4: Processamento dos arquivos
            self._print_step("ETAPA 4/6", f"Processamento de {len(arquivos_ano)} arquivos")
            
            arquivos_processados = []
            arquivos_com_erro = 0
            
            for i, nome_arquivo_7z in enumerate(arquivos_ano):
                print(f"\n--- ARQUIVO {i+1}/{len(arquivos_ano)}: {nome_arquivo_7z} ---")
                
                # Caminhos
                caminho_7z_local = os.path.join(self.config['DIRETORIO_TEMPORARIO'], nome_arquivo_7z)
                nome_csv_tratado = nome_arquivo_7z.replace('.7z', '_tratado.csv')
                caminho_csv_tratado = os.path.join(self.config['DIRETORIO_TRATADO'], nome_csv_tratado)
                
                arquivos_para_limpar = []
                
                try:
                    # Download
                    if not self.baixar_arquivo(ano, nome_arquivo_7z, caminho_7z_local):
                        arquivos_com_erro += 1
                        continue
                    arquivos_para_limpar.append(caminho_7z_local)
                    
                    # Extração
                    caminho_txt = self.extrair_arquivo(caminho_7z_local, self.config['DIRETORIO_TEMPORARIO'], 
                                                     ano, nome_arquivo_7z)
                    if not caminho_txt:
                        arquivos_com_erro += 1
                        self.limpar_arquivos_temporarios(arquivos_para_limpar)
                        continue
                    arquivos_para_limpar.append(caminho_txt)
                    
                    # Processamento
                    if not self.processar_arquivo_rais(caminho_txt, caminho_csv_tratado, ano, nome_arquivo_7z):
                        arquivos_com_erro += 1
                        self.limpar_arquivos_temporarios(arquivos_para_limpar)
                        continue
                    
                    arquivos_processados.append((caminho_csv_tratado, nome_arquivo_7z))
                    
                    # Limpeza de arquivos temporários intermediários
                    self.limpar_arquivos_temporarios(arquivos_para_limpar)
                    
                    print(f"ARQUIVO: ✅ {nome_arquivo_7z} processado com sucesso")
                    
                except Exception as e:
                    print(f"ERRO ARQUIVO: {nome_arquivo_7z} - {e}")
                    arquivos_com_erro += 1
                    self.limpar_arquivos_temporarios(arquivos_para_limpar)
            
            # ETAPA 5: Upload para BigQuery
            self._print_step("ETAPA 5/6", f"Upload de {len(arquivos_processados)} arquivos para BigQuery")
            
            if not arquivos_processados:
                print("ERRO UPLOAD: Nenhum arquivo foi processado com sucesso")
                return False
            
            uploads_com_sucesso = 0
            
            for i, (caminho_csv, nome_arquivo_original) in enumerate(sorted(arquivos_processados)):
                write_disposition = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
                
                print(f"UPLOAD {i+1}/{len(arquivos_processados)}: {nome_arquivo_original} (Modo: {write_disposition})")
                
                if self.carregar_csv_para_bigquery(caminho_csv, table_ref, write_disposition, 
                                                  ano, nome_arquivo_original):
                    uploads_com_sucesso += 1
                else:
                    print(f"ERRO UPLOAD: Falha no arquivo {nome_arquivo_original}")
            
            # ETAPA 6: Limpeza final e relatório
            self._print_step("ETAPA 6/6", "Limpeza final e geração de relatório")
            
            # Remove CSVs tratados
            csvs_para_limpar = [csv for csv, _ in arquivos_processados]
            self.limpar_arquivos_temporarios(csvs_para_limpar)
            
            # Relatório final
            end_time = datetime.now()
            duration = end_time - start_time
            
            relatorio = self.gerar_relatorio_progresso()
            
            self._print_header("PROCESSO CONCLUÍDO")
            print(f"SESSAO: {self.progress['session_id']}")
            print(f"ANO PROCESSADO: {ano}")
            print(f"INICIO: {start_time.strftime('%H:%M:%S')}")
            print(f"TERMINO: {end_time.strftime('%H:%M:%S')}")
            print(f"DURACAO: {str(duration).split('.')[0]}")
            print()
            print("RESUMO FINAL:")
            print(f"  • Total de arquivos: {len(arquivos_ano)}")
            print(f"  • Processados com sucesso: {len(arquivos_processados)}")
            print(f"  • Carregados no BigQuery: {uploads_com_sucesso}")
            print(f"  • Com erro: {arquivos_com_erro}")
            print(f"  • Taxa de sucesso: {(uploads_com_sucesso/len(arquivos_ano)*100):.1f}%")
            
            # Validação final da tabela
            if uploads_com_sucesso > 0:
                try:
                    table = self.client_bq.get_table(table_ref)
                    print(f"  • Registros finais na tabela: {table.num_rows:,}")
                except Exception as e:
                    print(f"  • Erro ao consultar tabela final: {e}")
            
            if relatorio['status_summary'].get('FAILED', 0) > 0:
                print("\nARQUIVOS COM ERRO:")
                for file_key, file_status in self.progress['files_status'].items():
                    if 'FAILED' in file_status.get('stage', ''):
                        arquivo = file_key.split(':', 1)[1]
                        erro = file_status.get('info', {}).get('error', 'Erro não especificado')
                        print(f"  • {arquivo}: {file_status['stage']} - {erro[:80]}...")
            
            success = uploads_com_sucesso > 0
            status_icon = "✅" if success else "❌"
            print(f"\n{status_icon} RESULTADO FINAL: {'SUCESSO' if success else 'FALHA'}")
            
            return success
            
        except Exception as e:
            print(f"ERRO CRITICO: {e}")
            import traceback
            print(traceback.format_exc())
            return False

# ==============================================================================
# FUNÇÃO PRINCIPAL E UTILITÁRIOS
# ==============================================================================

def mostrar_progresso():
    """Mostra progresso atual salvado"""
    print("=" * 70)
    print("STATUS ATUAL DO PROGRESSO RAIS")
    print("=" * 70)
    
    try:
        with open('rais_progress.json', 'r') as f:
            progress = json.load(f)
        
        print(f"SESSAO: {progress.get('session_id', 'N/A')}")
        print(f"ANO ATUAL: {progress.get('current_year', 'N/A')}")
        print(f"ULTIMA ATUALIZACAO: {progress.get('last_update', 'N/A')}")
        
        # Conta status
        status_count = {}
        for file_status in progress.get('files_status', {}).values():
            stage = file_status.get('stage', 'NOT_STARTED')
            status_count[stage] = status_count.get(stage, 0) + 1
        
        print("\nRESUMO POR STATUS:")
        for status, count in sorted(status_count.items()):
            print(f"  • {status}: {count} arquivos")
        
        # Mostra últimos erros
        failed_files = [(k, v) for k, v in progress.get('files_status', {}).items() 
                       if 'FAILED' in v.get('stage', '')]
        
        if failed_files:
            print("\nÚLTIMOS ERROS:")
            for file_key, file_status in failed_files[-3:]:
                arquivo = file_key.split(':', 1)[1]
                erro = file_status.get('info', {}).get('error', 'N/A')
                print(f"  • {arquivo}: {erro[:60]}...")
        
    except FileNotFoundError:
        print("NENHUM ARQUIVO DE PROGRESSO ENCONTRADO")
    except Exception as e:
        print(f"ERRO AO LER PROGRESSO: {e}")

def limpar_progresso():
    """Limpa arquivo de progresso para começar do zero"""
    try:
        if os.path.exists('rais_progress.json'):
            os.remove('rais_progress.json')
            print("✅ PROGRESSO LIMPO: Arquivo removido com sucesso")
        else:
            print("INFO: Nenhum arquivo de progresso encontrado")
    except Exception as e:
        print(f"ERRO: Não foi possível limpar progresso - {e}")

def main():
    """Função principal melhorada"""
    
    # Configurações (lidas do .env)
    config = {
        'FTP_HOST': "ftp.mtps.gov.br",
        'FTP_BASE_PATH': "/pdet/microdados/RAIS/",
        'ARQUIVOS_A_EXCLUIR': ["RAIS_ESTAB_PUB.7z", "RAIS_VINC_PUB_NI.7z"],
        'CHUNK_SIZE_PROCESSAMENTO': 1000000,
        'LOCATION_BQ': "southamerica-east1",

        # Variáveis carregadas de forma segura do ambiente
        'CAMINHO_CREDENCIAL_BQ': os.getenv("GCP_CREDENTIALS_PATH"),
        'PROJECT_ID_BQ': os.getenv("GCP_PROJECT_ID"),
        'DATASET_ID_BQ': os.getenv("BIGQUERY_DATASET_ID"),
        'CAMINHO_DICIONARIO_EXCEL': os.getenv("RAIS_DICIONARIO_PATH"),
        'DIRETORIO_TEMPORARIO': os.getenv("RAIS_TEMP_DIR"),
        'DIRETORIO_TRATADO': os.getenv("RAIS_TRATADO_DIR"),
    }

    # Validação das variáveis de ambiente
    for key, value in config.items():
        if value is None:
            print(f"ERRO CRÍTICO: A variável de ambiente para '{key}' não foi definida no arquivo .env")
            return False
    
    # Processa argumentos de linha de comando
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "--status":
            mostrar_progresso()
            return True
        elif arg == "--clear":
            limpar_progresso()
            return True
        elif arg == "--help":
            print("="*70)
            print("SISTEMA RAIS - OPCÕES DISPONÍVEIS")
            print("="*70)
            print("python script_rais.py           # Execução normal")
            print("python script_rais.py --status  # Mostra progresso atual") 
            print("python script_rais.py --clear   # Limpa progresso salvo")
            print("python script_rais.py --help    # Mostra esta ajuda")
            return True
    
    try:
        # Inicializa loader
        loader = ImprovedRAISLoader(config)
        
        print("=" * 70)
        print("🚀 SISTEMA RAIS - PROCESSAMENTO PARA BIGQUERY")
        print("=" * 70)
        
        # Testa conectividade FTP
        print("CONECTIVIDADE: Testando conexão FTP...")
        try:
            with ftplib.FTP(config['FTP_HOST'], timeout=30) as ftp:
                ftp.login()
            print("CONECTIVIDADE: ✅ FTP acessível")
        except Exception as e:
            print(f"ERRO CONECTIVIDADE: FTP inacessível - {e}")
            return False
        
        # Obtém anos disponíveis
        try:
            anos_disponiveis = loader.obter_anos_disponiveis()
            if not anos_disponiveis:
                print("ERRO CONSULTA: Nenhum ano disponível")
                return False
            
            print(f"ANOS DISPONÍVEIS: {', '.join(anos_disponiveis)}")
        except Exception as e:
            print(f"ERRO CONSULTA: Falha ao obter anos - {e}")
            return False
        
        # Verifica se há progresso anterior
        if loader.progress.get('current_year'):
            current_year = loader.progress['current_year']
            print(f"\nPROGRESSO ANTERIOR: Encontrado para o ano {current_year}")
            
            relatorio = loader.gerar_relatorio_progresso()
            print(f"STATUS ATUAL: {relatorio['status_summary']}")
            
            continuar = input(f"CONTINUAR: Retomar processamento do ano {current_year}? (s/n): ").strip().lower()
            if continuar.startswith('n'):
                limpar = input("LIMPAR: Apagar progresso e começar do zero? (s/n): ").strip().lower()
                if limpar.startswith('s'):
                    limpar_progresso()
                    loader = ImprovedRAISLoader(config)  # Reinicializa
                else:
                    print("CANCELADO: Processo interrompido pelo usuário")
                    return True
            else:
                ano_escolhido = current_year
        
        # Solicita ano se não há progresso anterior ou foi limpo
        if not loader.progress.get('current_year'):
            while True:
                ano_escolhido = input("\nANO: Digite o ano para processar: ").strip()
                if ano_escolhido in anos_disponiveis:
                    break
                elif ano_escolhido.lower() in ['quit', 'exit', 'sair']:
                    print("CANCELADO: Processo interrompido pelo usuário")
                    return True
                else:
                    print(f"ERRO ANO: '{ano_escolhido}' não disponível. Anos: {', '.join(anos_disponiveis)}")
        
        # Executa processo principal
        print(f"\nPREPARANDO: Início do processamento para o ano {ano_escolhido}")
        print("INFO: Processo pode ser interrompido e retomado a qualquer momento")
        print("INFO: Progresso salvo automaticamente em 'rais_progress.json'")
        
        input("\nPressione ENTER para continuar ou Ctrl+C para cancelar...")
        
        sucesso = loader.executar_processo_completo(ano_escolhido)
        
        if sucesso:
            print("\n🎉 PROCESSO FINALIZADO COM SUCESSO!")
        else:
            print("\n⚠️  PROCESSO FINALIZADO COM PROBLEMAS")
            print("INFO: Execute novamente para tentar reprocessar arquivos com falha")
        
        return sucesso
        
    except KeyboardInterrupt:
        print("\n\n⏹️  PROCESSO INTERROMPIDO PELO USUÁRIO")
        print("INFO: Progresso salvo. Execute novamente para continuar de onde parou.")
        return True
        
    except Exception as e:
        print(f"\nERRO CRITICO INESPERADO: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    main()

# ==============================================================================
# DOCUMENTAÇÃO DE USO
# ==============================================================================
"""
🚀 SISTEMA RAIS MELHORADO - INSPIRADO NO ESTILO RF

PRINCIPAIS MELHORIAS:
✅ Logs limpos e visuais como o código da RF
✅ Separadores visuais para facilitar acompanhamento  
✅ Status claro de cada etapa (SKIP, OK, ERRO, etc.)
✅ Progresso simplificado mas robusto
✅ Relatórios mais claros e organizados
✅ Sistema de retry mantido mas com logs melhores

COMANDOS DISPONÍVEIS:
- python script_rais.py           # Execução normal
- python script_rais.py --status  # Mostra progresso atual
- python script_rais.py --clear   # Limpa progresso salvo  
- python script_rais.py --help    # Mostra ajuda

EXEMPLO DE LOGS:
======================================================================
ETAPA 1/6: Inicialização dos sistemas
======================================================================
BIGQUERY: Inicializando conexão...
BIGQUERY: ✅ Conexão estabelecida com sucesso
DICIONARIOS: Carregando traduções...
DICIONARIOS: ✅ 13 dicionários carregados

--- ARQUIVO 1/25: RAIS_VINC_PUB_CENTRO_OESTE.7z ---
DOWNLOAD: Iniciando RAIS_VINC_PUB_CENTRO_OESTE.7z...
DOWNLOAD: ✅ RAIS_VINC_PUB_CENTRO_OESTE.7z concluído (245.3 MB)
EXTRACAO: Processando RAIS_VINC_PUB_CENTRO_OESTE.7z...
EXTRACAO: ✅ RAIS_VINC_PUB_CENTRO_OESTE.7z concluído (1,023.4 MB)

RECURSOS MANTIDOS:
🔄 Sistema de retry automático
💾 Checkpoint/progresso para retomar
🔍 Verificação de integridade  
🧹 Limpeza automática de arquivos
📊 Relatórios detalhados de validação
"""
