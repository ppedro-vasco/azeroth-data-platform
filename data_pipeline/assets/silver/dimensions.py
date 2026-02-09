from dagster import asset, Output, AssetExecutionContext
import requests
from datetime import datetime
import time

from data_pipeline.utils.postgres_client import PostgresClient
from data_pipeline.utils.blizzard_auth import get_blizzard_token

@asset(
    group_name="silver_dimensions",
    deps=["silver_auctions"],
    description="Consulta API da Blizzard para pegar nomes e classes de novos itens (Incremental)",
)
def dim_items(context: AssetExecutionContext):
    pg = PostgresClient()

    missing_ids = pg.get_missing_item_ids(limit=100)

    if not missing_ids:
        context.log.info("Nenhum item novo ou pendente de atualização.")
        return Output(None)
    
    context.log.info(f"Encontrados {len(missing_ids)} itens para enriquecer. Iniciando fetch...")

    token = get_blizzard_token()
    headers = {"Authorization": f"Bearer {token}"}
    items_to_save = []

    for item_id in missing_ids:
        url = f"https://us.api.blizzard.com/data/wow/item/{item_id}"

        try:
            response = requests.get(
                url,
                headers=headers,
                params={"namespace": "static-us", "locale": "pt_BR"},
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()

                item_payload = {
                    "item_id": item_id,
                    "name": data.get("name"),
                    "quality": data.get("quality", {}).get("name", "Unknown"),
                    "item_class": data.get("item_class", {}).get("name", "Misc"),
                    "item_subclass": data.get("item_subclass", {}).get("name", "Junk"),
                    # URL do ícone é complexa na API nova, vamos deixar placeholder ou null por enquanto
                    "icon_url": None, 
                    "last_updated": datetime.utcnow()
                }
                items_to_save.append(item_payload)
                context.log.debug(f"Item {item_id} ({data.get('name')}) processado.")

            elif response.status_code == 404:
                context.log.warning(f"Item {item_id} não existe na API da Blizzard. Ignorando.")
                # Opcional: Inserir com nome "DEPRECATED" para não tentar buscar de novo?
                # Por enquanto, não salvamos nada. Na próxima execução, ele cai no "missing" de novo.
        
            else:
                context.log.error(f"Erro {response.status_code} para item {item_id}")
                time.sleep(1)  # Pequena pausa para evitar rate limits
        
        except Exception as e:
            context.log.error(f"Erro de conexão ao buscar item {item_id}: {e}")
    
    if items_to_save:
        rows = pg.insert_item_dimensions(items_to_save)
        context.log.info(f"Enriquecimento concluído. {rows} itens salvos/atualizados na dim_items.")
        return Output(rows, metadata={"updated_count": rows})
    else:
        context.log.warning("Nenhum item válido retornado pela API nesta leva.")
        return Output(0)

def insert_item_dimensions(self, items_dict_list):
    session = self.Session()
    from data_pipeline.utils.database import ItemDimension
    
    try:
        if not items_dict_list:
            return 0
        
        stmt = insert_item_dimensions(ItemDimension).values(items_dict_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=['item_id'],
            set_={
                'name': stmt.excluded.name,
                'quality': stmt.excluded.quality,
                'item_class': stmt.excluded.item_class,
                'item_subclass': stmt.excluded.item_subclass,
                'icon_url': stmt.excluded.icon_url,
                'last_updated': stmt.excluded.last_updated
            }
        )

        result = session.execute(stmt)
        session.commit()

        return result.rowcount
    
    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir dimensões: {e}")
        raise e
    finally:
        session.close()

