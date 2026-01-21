from dagster import asset, AssetExecutionContext
from postgres_client import PostgresClient

@asset(deps=["process_silver_data", "build_item_dimension"])
def gold_daily_market_summary(context: AssetExecutionContext):
    sql = """
    DROP TABLE IF EXISTS gold_daily_market_summary;
    
    CREATE TABLE gold_daily_market_summary AS
    SELECT 
        s.item_id,
        d.name as item_name,
        d.icon_url,
        DATE(s.created_at) as snapshot_date,
        
        MIN(s.buyout) as min_buyout,
        MAX(s.buyout) as max_buyout,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY s.buyout) as median_buyout,
        
        SUM(s.quantity) as quantity_available,
        (SUM(s.quantity) * MIN(s.buyout)) as market_cap, 
        
        COALESCE(STDDEV(s.buyout), 0) as price_volatility,
        COUNT(*) as auction_count

    FROM silver_auctions s
    LEFT JOIN dim_items d ON s.item_id = d.item_id
    GROUP BY 1, 2, 3, 4;
    
    CREATE INDEX idx_gold_daily_item ON gold_daily_market_summary(item_id, snapshot_date);
    """

    pg = PostgresClient()
    pg.execute_sql_command(sql)
    
    context.log.info("Tabela gold_daily_market_summary recriada com sucesso.")

@asset(deps=["process_silver_data", "build_item_dimension"])
def gold_price_history(context: AssetExecutionContext):
    sql = """
    DROP TABLE IF EXISTS gold_price_history;
    
    CREATE TABLE gold_price_history AS
    SELECT
        s.item_id,
        d.name as item_name,
        DATE_TRUNC('hour', s.created_at) as snapshot_hour,

        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.buyout) as open_price,        -- Preço de Mercado (Mediana)
        MAX(s.buyout) as high_price,                                                -- Preço Máximo (Topo da Sombra)
        MIN(s.buyout) as low_price,                                                 -- Preço Mínimo (Base da Sombra)
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.buyout) as close_price,       -- Preço de Mercado (Igual ao Open no snapshot único)
        
        AVG(s.buyout) as average_price,
        SUM(s.quantity) as volume

    FROM silver_auctions s
    LEFT JOIN dim_items d ON s.item_id = d.item_id
    GROUP BY 1, 2, 3;

    CREATE INDEX idx_gold_price_history_item ON gold_price_history(item_id, snapshot_hour);
    """

    pg = PostgresClient()
    pg.execute_sql_command(sql)

    context.log.info("Tabela gold_price_history recriada com sucesso.")