from flask import Flask, render_template, request, redirect, url_for, session
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime
import os
import json
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env (para desenvolvimento local)
load_dotenv()

app = Flask(__name__)

# --- CONFIGURAÇÃO SEGURA ---
# Carrega a chave secreta do Flask do ambiente para gerenciar sessões de login
app.secret_key = os.getenv("FLASK_SECRET_KEY")

# Carrega informações do BigQuery do ambiente
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
TABLE_ID = os.getenv('BIGQUERY_TABLE_ID')

# Validação para garantir que as variáveis de ambiente foram definidas
if not all([GOOGLE_CREDENTIALS_JSON, TABLE_ID, app.secret_key]):
    raise ValueError("ERRO: As variáveis de ambiente GOOGLE_APPLICATION_CREDENTIALS_JSON, BIGQUERY_TABLE_ID e FLASK_SECRET_KEY devem ser definidas.")

# Converte o JSON string em um dicionário e cria as credenciais
try:
    credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    print("Cliente BigQuery inicializado com sucesso.")
except Exception as e:
    print(f"ERRO CRÍTICO ao inicializar cliente BigQuery: {e}")
    # Em um app real, você poderia ter um tratamento de erro mais robusto aqui
    client = None

# --- ROTAS DA APLICAÇÃO ---

@app.route('/')
def home():
    # Se o jurado já está logado, vai direto para a avaliação
    if 'jurado_id' in session:
        return redirect(url_for('avaliacao'))
    # Senão, vai para a tela de login
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Lógica de login simplificada. Em um projeto real, isso consultaria
        # um banco de dados de jurados com senhas criptografadas.
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Exemplo: Verificação simples (substituir por um sistema real de autenticação)
        if password == "premio2024":
            session['jurado_id'] = username
            return redirect(url_for('avaliacao'))
        else:
            return render_template('login.html', error="Usuário ou senha inválidos")
            
    return render_template('login.html')

@app.route('/avaliacao', methods=['GET', 'POST'])
def avaliacao():
    # Protege a rota, só permite acesso se estiver logado
    if 'jurado_id' not in session:
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        # Captura todos os dados do formulário de avaliação
        dados_formulario = request.form.to_dict()
        # Adiciona metadados importantes à avaliação
        dados_formulario['jurado_id'] = session['jurado_id']
        dados_formulario['timestamp_avaliacao'] = datetime.utcnow().isoformat()

        print(f"Recebendo avaliação de {session['jurado_id']}: {dados_formulario}")

        # Insere os dados no BigQuery
        if client:
            try:
                errors = client.insert_rows_json(TABLE_ID, [dados_formulario])
                if not errors:
                    print(f"Avaliação de {session['jurado_id']} inserida com sucesso no BigQuery.")
                else:
                    print(f"Erro ao inserir no BigQuery: {errors}")
            except Exception as e:
                print(f"Erro de conexão com BigQuery: {e}")
        
        return redirect(url_for('obrigado'))

    return render_template('avaliacao.html', jurado=session.get('jurado_id'))

@app.route('/obrigado')
def obrigado():
    return "<h1>Obrigado!</h1><p>Sua avaliação foi registrada com sucesso.</p><a href='/logout'>Sair</a>"

@app.route('/logout')
def logout():
    session.pop('jurado_id', None)
    return redirect(url_for('login'))


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    # O modo debug deve ser False em produção
    app.run(host='0.0.0.0', port=port, debug=True)
