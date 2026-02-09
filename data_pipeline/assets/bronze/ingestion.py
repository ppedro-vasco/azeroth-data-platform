# JSON ou CSV bruto

from dagster import asset, Output, AssetExecutionContext
import requests
import datetime
from data_pipeline.utils.minio_client import MinIOClient
from data_pipeline.utils.blizzard_auth import get_blizzard_token

@asset(
    group_name="bronze_ingestion",
    description="Baixa o dump bruto da Blizzard e salva no MinIO (Bronze Layer)"
)
def raw_auctions_data(context: AssetExecutionContext):
    token = get_blizzard_token()

    connected_real_id = 3209 # Azarlaon
    search_url = f"https://us.api.blizzard.com/data/wow/connected-realm/{connected_real_id}/auctions"
    headers = {"Authorization": f"Bearer {token}"}

    context.log.info(f"Iniciando download de: {search_url}")

    try:
        response = requests.get(
            search_url, 
            headers=headers,
            params={"namespace": "dynamic-us", "locale": "en_US"},
            timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Falha ao conectar na Blizzard API: {e}")
    
    minio = MinIOClient()

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    file_name = f"raw_auctions_{today}.json"

    path = f"{today}/{file_name}"

    saved_path = minio.save_json(data, path, bucket_name="bronze")

    context.log.info(f"Arquivo salvo com sucesso: {saved_path}")

    return Output(
        saved_path,
        metadata={
            "path": saved_path,
            "records": len(data.get("auctions", [])),
            "date": today
        }
    )