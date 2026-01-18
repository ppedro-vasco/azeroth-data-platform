from typing import Any, Dict, List

def transform_auctions(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not raw_data or 'auctions' not in raw_data:
        print("Aviso: JSON vazio ou chave 'auctions' não encontrada.")
        return []
    
    transformed_list = []

    for row in raw_data['auctions']:
        item_data = row.get('item', {})

        action_object = {
            "id": row.get('id'),
            "item_id": row.get('item_id'),
            "quantity":row.get('quantity'),
            "unit_price":row.get('unit_price'),
            "buyout":row.get('buyout'),
            "time_left":row.get('time_left'),
            "modifiers":row.get('modifiers')
        }

        transformed_list.append(action_object)

    print(f"Transformação concluída: {len(transformed_list)} registros processados.")
    return transformed_list



    

    
