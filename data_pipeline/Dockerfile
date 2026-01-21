FROM python:3.10-slim

# Evita arquivos .pyc e logs em buffer
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/dagster/app

COPY config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

COPY . .

RUN cp config/dagster.yaml $DAGSTER_HOME/dagster.yaml

EXPOSE 3000

# CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m" "dags"]