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

@asset(deps=["gold_daily_market_summary", "build_item_dimension"])
def gold_market_opportunities(context: AssetExecutionContext):
    sql = """
    DROP TABLE IF EXISTS gold_market_opportunities;
    
    CREATE TABLE gold_market_opportunities AS
    WITH rolling_stats AS (
        SELECT
            item_id,
            snapshot_date,
            median_buyout,
            min_buyout,
            
            AVG(median_buyout) OVER (
                PARTITION BY item_id
                ORDER BY snapshot_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS avg_price_7d,

            STDDEV(median_buyout) OVER (
                PARTITION BY item_id
                ORDER BY snapshot_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS std_dev_7d
        FROM gold_daily_market_summary
    )
    SELECT
        r.item_id,
        d.name as item_name,
        d.icon_url,
        r.snapshot_date,
        
        r.min_buyout as current_price,
        r.avg_price_7d,
        r.std_dev_7d,
        
        (r.min_buyout - r.avg_price_7d) / NULLIF(r.std_dev_7d, 0) AS z_score,

        CASE
            WHEN (r.min_buyout - r.avg_price_7d) / NULLIF(r.std_dev_7d, 0) <= -1.5 THEN 'BUY'
            WHEN (r.min_buyout - r.avg_price_7d) / NULLIF(r.std_dev_7d, 0) >= -1.5 THEN 'SELL'
            ELSE 'HOLD'
        END AS recommendation

    FROM rolling_stats r
    LEFT JOIN dim_items d ON r.item_id = d.item_id
    WHERE r.snapshot_date >= CURRENT_DATE - INTERVAL '30 days';

    CREATE INDEX indx_opps_item ON gold_market_opportunities(item_id, snapshot_date);
    """

    pg = PostgresClient()
    pg.execute_sql_command(sql)

    context.log.info("Tabela gold_market_opportunities recriada com sucesso.")

@asset(deps=["process_silver_data", "build_item_dimension"])
def gold_item_demand(context: AssetExecutionContext):
    sql = """
    DROP TABLE IF EXISTS gold_item_demand;
    
    CREATE TABLE gold_item_demand AS
    WITH hourly_snapshots AS (
        SELECT
            item_id,
            DATE_TRUNC('hour', created_at) as snapshot_hour,
            SUM(quantity) as total_quantity
        FROM silver_auctions
        GROUP BY 1, 2
    ),
    quantity_deltas AS (
        SELECT
            item_id,
            snapshot_hour,
            total_quantity,
            LAG(total_quantity) OVER (PARTITION BY item_id ORDER BY snapshot_hour) as prev_quantity
        FROM hourly_snapshots
    ),
    daily_churn AS (
        SELECT 
            item_id,
            DATE(snapshot_hour) as snapshot_date,
            SUM(GREATEST(prev_quantity - total_quantity, 0)) as estimated_daily_sales,
            AVG(total_quantity) as avg_daily_stock
        FROM quantity_deltas
        GROUP BY 1, 2
    )
    SELECT 
        c.item_id,
        d.name as item_name,
        d.icon_url,
        c.snapshot_date,
        c.estimated_daily_sales,
        c.avg_daily_stock,

        (c.estimated_daily_sales / NULLIF(c.avg_daily_stock, 0)) * 100 as turnover_percentage
    FROM daily_churn c
    LEFT JOIN dim_items d ON c.item_id = d.item_id;

    CREATE INDEX idx_gold_demand ON gold_item_demand(item_id, snapshot_date);
    """

    pg = PostgresClient()
    pg.execute_sql_command(sql)

    context.log.info("Tabela gold_item_demand recriada com sucesso.")

@asset(deps=["process_silver_data", "build_item_dimension"])
def gold_market_concentration(context: AssetExecutionContext):
    sql = """
    DROP TABLE IF EXISTS gold_market_concentration;
    
    CREATE TABLE gold_market_concentration AS
    WITH daily_stats AS (
        SELECT 
            item_id,
            DATE(created_at) as snapshot_date,
            MIN(buyout) as min_price,
            SUM(quantity) as total_market_quantity
        FROM silver_auctions
        GROUP BY 1, 2
    ),
    floor_stats AS (
        SELECT 
            s.item_id,
            DATE(s.created_at) as snapshot_date,
            SUM(s.quantity) as quantity_at_floor
        FROM silver_auctions s
        JOIN daily_stats d ON s.item_id = d.item_id AND DATE(s.created_at) = d.snapshot_date
        WHERE s.buyout = d.min_price
        GROUP BY 1, 2
    )
    SELECT 
        d.item_id,
        i.name as item_name,
        i.icon_url,
        d.snapshot_date,
        d.total_market_quantity,
        COALESCE(f.quantity_at_floor, 0) as quantity_at_floor,
        
        (COALESCE(f.quantity_at_floor, 0) / NULLIF(d.total_market_quantity, 0)) * 100 as floor_concentration_pct,
        
        CASE 
            WHEN (COALESCE(f.quantity_at_floor, 0) / NULLIF(d.total_market_quantity, 0)) > 0.8 THEN 'HIGH_CONCENTRATION'
            WHEN d.total_market_quantity < 50 THEN 'SCARCE_SUPPLY'
            ELSE 'HEALTHY'
        END as market_status
        
    FROM daily_stats d
    LEFT JOIN floor_stats f ON d.item_id = f.item_id AND d.snapshot_date = f.snapshot_date
    LEFT JOIN dim_items i ON d.item_id = i.item_id;
    
    CREATE INDEX idx_gold_concentration ON gold_market_concentration(item_id, snapshot_date);
    """
    
    pg = PostgresClient()
    pg.execute_sql_command(sql)
    context.log.info("Tabela gold_market_concentration criada.")