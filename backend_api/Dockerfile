FROM python:3.10-slim

WORKDIR /app

# Instala dependências do sistema (necessário para psycopg2)
RUN apt-get update && apt-get install -y libpq-dev gcc

# Copia e instala libs
COPY config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY . .

# Roda o servidor
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]