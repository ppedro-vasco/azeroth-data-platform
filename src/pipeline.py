import requests

from dagster import asset, AssetExecutionContext, Definitions
from blizzard_auth import BlizzardAuth

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
    
    return data

defs = Definitions(
    assets = [get_token, get_realm_id, extract_auction_data]
)