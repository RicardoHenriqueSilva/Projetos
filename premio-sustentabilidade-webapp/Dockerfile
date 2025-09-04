# Usa uma imagem base oficial do Python
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependências para o container
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto do código da aplicação para o container
COPY . .

# Expõe a porta que o gunicorn irá rodar
EXPOSE 8080

# Comando para iniciar a aplicação em produção usando gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
