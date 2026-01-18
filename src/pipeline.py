import requests

from dagster import asset, AssetExecutionContext, Definitions
from blizzard_auth import BlizzardAuth
from minio_client import MinIOClient
from datetime import datetime
from transform import transform_auctions
from postgres_client import PostgresClient

@asset
def get_token(context: AssetExecutionContext):
    auth = BlizzardAuth()

    token = auth.get_token()

    context.log.info(f"TOKEN OBTIDO COM SUCESSO: {token[:10]}...")
    return token

@asset
def get_realm_id(context: AssetExecutionContext):
    auth = BlizzardAuth()
    token = auth.get_token()

    realm_slug = "azralon"
    url = f"https://us.api.blizzard.com/data/wow/realm/{realm_slug}"

    query_params = {
        'namespace': 'dynamic-us',
        'locale': 'pt_BR'
    }
    
    headers = {
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, params=query_params, headers=headers)

    response.raise_for_status()

    data = response.json()

    connected_real_url = data ['connected_realm']['href']
    context.log.info(f"URL de Conexão encontrada: {connected_real_url}")

    return data

@asset
def extract_auction_data(context: AssetExecutionContext):
    auth = BlizzardAuth()
    token = auth.get_token()
    
    connected_realm_id = 3209

    url = f"https://us.api.blizzard.com/data/wow/connected-realm/{connected_realm_id}/auctions"
    query_params = {
        'namespace': 'dynamic-us',
        'locale': 'pt_BR'
    }
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    context.log.info(f"Baixando dados do leilão para Realm ID: {connected_realm_id}...")

    response = requests.get(url, params=query_params, headers=headers)
    response.raise_for_status()
    data = response.json()

    qtd_itens = len(data.get('auctions', []))
    context.log.info(f"Download concluído. Total de lotes no leilão: {qtd_itens}")

    minio = MinIOClient()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"auctions_{connected_realm_id}_{timestamp}.json"
    
    minio.save_json(data, filename)

    context.log.info(f"Arquivo salvo no MinIO (bucket = bronze): {filename}")

    return data

@asset
def process_silver_data(context: AssetExecutionContext, extract_auction_data):
    raw_data = extract_auction_data

    context.log.info("Iniciando transformação dos dados do leilão...")
    transformed_data = transform_auctions(raw_data)

    if not transformed_data:
        context.log.warn("Nenhum dado para processar.")
        return []
    
    minio = MinIOClient()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"silver_auctions_{timestamp}.json"

    minio.save_json(transformed_data, filename, bucket_name="silver")
    context.log.info(f"Arquivo transformado salvo no MinIO (bucket = silver): {filename}")

    pg_client = PostgresClient()
    context.log.info("Iniciando inserção dos dados transformados no Postgres...")
    
    qtd_inserida = pg_client.insert_auctions(transformed_data)
    context.log.info(f"Inserção concluída. Total de registros inseridos: {qtd_inserida}")
    return transformed_data

defs = Definitions(
    assets = [get_token, get_realm_id, extract_auction_data, process_silver_data]
)