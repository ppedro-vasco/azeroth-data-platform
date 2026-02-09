from dagster import asset, Output, AssetExecutionContext, AssetIn
import pandas as pd
import datetime
import json
import numpy as np

from data_pipeline.utils.minio_client import MinIOClient
from data_pipeline.utils.postgres_client import PostgresClient

@asset(
    group_name="silver_transformation",
    description="Processa JSON bruto, normaliza preços e salva no Postgres",
    ins={
        "raw_auctions_path": AssetIn(key="raw_auctions_data")
    }
)
def silver_auctions(context: AssetExecutionContext, raw_auctions_path: str):
    minio = MinIOClient()
    context.log.info(f"Carregando dados brutos de: {raw_auctions_path}")
    
    json_data = minio.read_json(raw_auctions_path)    
    
    if "auctions" not in json_data:
        raise ValueError("JSON inválido: chave 'auctions' não encontrada")
    
    df = pd.DataFrame(json_data["auctions"])

    if df.empty:
        context.log.warning("Arquivo de leitura está vazio.")
        return Output(None)
    
    if 'item' in df.columns:
        df['item_id'] = df['item'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else None
        )

        df['modifiers'] = df['item'].apply(
            lambda x: x.get('modifiers') if isinstance(x, dict) else None
        )
    else:
        df['item_id'] = None
        df['modifiers'] = None
    
    if 'unit_price' not in df.columns:
        df['unit_price'] = np.nan
    if 'buyout' not in df.columns:
        df['buyout'] = np.nan
    
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['buyout'] = pd.to_numeric(df['buyout'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1)

    df['unit_price'] = df['unit_price'].fillna(df['buyout'] / df['quantity'])

    df['snapshot_date'] = datetime.date.today()

    final_df = df[[
        'id',
        'item_id',
        'quantity', 
        'unit_price', 
        'buyout', 
        'time_left', 
        'modifiers',
        'snapshot_date'
    ]].copy()

    final_df = final_df.replace({np.nan: None})

    context.log.info(f"Transformação concluída. {len(final_df)} registros prontos.")

    pg = PostgresClient()
    rows_inserted = pg.insert_dataframe(final_df)
    
    context.log.info(f"{rows_inserted} registros inseridos na tabela silver_auctions.")

    return Output(
        "silver_auctions", 
        metadata={
            "table": "silver_auctions",
            "records": len(final_df),
            "inserted": rows_inserted
        }
    )