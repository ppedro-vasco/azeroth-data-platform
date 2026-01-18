import requests

from blizzard_auth import BlizzardAuth
from minio_client import MinIOClient
from datetime import datetime
from transform import transform_auctions
from postgres_client import PostgresClient

from dagster import (
    asset, 
    AssetExecutionContext, 
    Definitions,
    ScheduleDefinition,
    define_asset_job, 
    asset_check, 
    AssetCheckResult
)

# ------------------------------------------------
# DEFINIÇÃO DE JOBS E SCHEDULES
# ------------------------------------------------
auction_job = define_asset_job(name = "auction_update_job", selection = "*")
maintenance_job = define_asset_job(name = "maintenance_job", selection = "compliance_enforcer")

hourly_schedule = ScheduleDefinition(
    job = auction_job,
    cron_schedule = "0 * * * *",  # Executa a cada hora no minuto 0
    name = "hourly_update_schedule"
)

daily_cleanup_schedule = ScheduleDefinition(
    job = maintenance_job,
    cron_schedule = "0 0 * * *",
    name = "daily_compliance_check_schedule"
)

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

@asset
def compliance_enforcer(context: AssetExecutionContext):
    RETENTION_DAYS = 30

    minio = MinIOClient()
    deleted_bronze = minio.prune_old_files("bronze", RETENTION_DAYS)
    deleted_silver = minio.prune_old_files("silver", RETENTION_DAYS)
    context.log.info(f"Arquivos antigos deletados: bronze={deleted_bronze}, silver={deleted_silver}")

    pg = PostgresClient()
    deleted_rows = pg.delete_old_data(RETENTION_DAYS)
    context.log.info(f"Postgres Cleanup: {deleted_rows} linhas deletadas.")

    return {
        "deleted_bronze_files": deleted_bronze,
        "deleted_silver_files": deleted_silver,
        "deleted_db_rows": deleted_rows
    }

@asset_check(asset = process_silver_data, description = "Garante que não existem preços negativos nos dados processados.")
def check_prices_non_negative(context, process_silver_data):
    data = process_silver_data
    issues = []

    for row in data:
        if row.get('buyout') is not None and row ['buyout'] < 0:
            issues.append(f"Item {row['item_id']} com BUYOUT negativo: {row['buyout']}")

        if row.get('unit_price') is not None and row ['unit_price'] < 0:
            issues.append(f"Item {row['item_id']} com UNIT_PRICE negativo: {row['unit_price']}")

    return AssetCheckResult(
        passed =len(issues) == 0,
        metadata = {
            "total_rows_checked": len(data),
            "negative_price_count": len(issues),
            "sample_issues": issues[:5]
        }
    )

@asset_check(asset = process_silver_data, description = "Garante que a quantidade de itens processados é sempre positiva.")
def check_quantity_positive(context, process_silver_data):
    data = process_silver_data
    invalid_rows = [row for row in data if row.get('quantity', 0) <= 0]

    return AssetCheckResult(
        passed = len(invalid_rows) == 0,
        metadata = {
            "invalid_quantity_count": len(invalid_rows)
        }
    )

defs = Definitions(
    assets = [
        get_token, 
        get_realm_id, 
        extract_auction_data, 
        process_silver_data, 
        compliance_enforcer
        ],
    asset_checks = [check_prices_non_negative, check_quantity_positive],
    schedules = [hourly_schedule, daily_cleanup_schedule]
)