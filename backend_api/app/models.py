from sqlalchemy import Column, Integer, String, BigInteger, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Float, Date
from .database import Base

class ItemDimension(Base):
    __tablename__ = 'dim_items'

    item_id = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    quality = Column(String, nullable=False)
    item_class = Column(String, nullable=False)
    item_subclass = Column(String, nullable=False)
    icon_url = Column(String)
    last_updated = Column(DateTime, default=func.now())

class Auction(Base):
    __tablename__ = 'silver_auctions'

    id = Column(BigInteger, primary_key=True)
    
    item_id = Column(BigInteger, nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(BigInteger, nullable=True)
    buyout = Column(BigInteger, nullable=True)
    time_left = Column(String, nullable=False)
    modifiers = Column(JSONB, nullable=True) 
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class GoldDailyMarketSummary(Base):
    __tablename__ = 'gold_daily_market_summary'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_date = Column(Date, primary_key=True)

    item_name = Column(String)
    icon_url = Column(String)
    min_buyout = Column(BigInteger)
    max_buyout = Column(BigInteger)
    median_buyout = Column(BigInteger)
    quantity_available = Column(BigInteger)
    market_cap = Column(BigInteger)
    price_volatility = Column(Float)
    auction_count = Column(Integer)

class GoldPriceHistory(Base):
    __tablename__ = 'gold_price_history'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_hour = Column(DateTime, primary_key=True)

    item_name = Column(String)
    open_price = Column(Float)
    high_price = Column(BigInteger)
    low_price = Column(BigInteger)
    close_price = Column(Float)
    avarage_price = Column(Float)
    volume = Column(BigInteger)

class GoldMarketOpportunities(Base):
    __tablename__ = 'gold_market_opportunities'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_date = Column(Date, primary_key=True)

    item_name = Column(String)
    icon_url = Column(String)
    current_price = Column(BigInteger)
    avg_price_7d = Column(Float)
    std_dev_7d = Column(Float)
    z_score = Column(Float)
    recommendation = Column(String)

class GoldItemDemand(Base):
    __tablename__ = 'gold_item_demand'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_date = Column(Date, primary_key=True)

    item_name = Column(String)
    icon_url = Column(String)
    estimated_daily_sales = Column(BigInteger)
    avg_daily_stock = Column(Float)
    turnover_percentage = Column(Float)

class GoldMarketConcentration(Base):
    __tablename__ = 'gold_market_concentration'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_date = Column(Date, primary_key=True)

    item_name = Column(String)
    icon_url = Column(String)
    total_market_quantity = Column(BigInteger)
    quantity_at_floor = Column(BigInteger)
    floor_concentration_pct = Column(Float)
    market_status = Column(String)

class GoldMarketIndex(Base):
    __tablename__ = 'gold_market_index'

    snapshot_date = Column(Date, primary_key=True)
    items_in_index = Column(Integer)
    index_value = Column(Float)
    inflation_pct_daily = Column(Float)