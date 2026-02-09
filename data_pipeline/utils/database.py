from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, BigInteger, DateTime, Date
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

Base = declarative_base()

class Auction(Base):
    __tablename__ = 'silver_auctions'

    id = Column(BigInteger, primary_key=True)
    
    item_id = Column(BigInteger, nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(BigInteger, nullable=True)
    buyout = Column(BigInteger, nullable=True)
    time_left = Column(String, nullable=False)
    modifiers = Column(JSONB, nullable=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

def create_tables(connection_string):
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    print("Schema verificado/criado com sucesso!")

class ItemDimension(Base):
    __tablename__ = 'dim_items'

    item_id = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    quality = Column(String, nullable=False)
    item_class = Column(String, nullable=False)
    item_subclass = Column(String, nullable=False)
    icon_url = Column(String)
    last_updated = Column(DateTime, default=datetime.utcnow)

class MarketHistory(Base):
    __tablename__ = 'gold_market_history'

    item_id = Column(BigInteger, primary_key=True)
    snapshot_date = Column(Date, primary_key=True)

    min_buyout = Column(float)
    avg_buyout = Column(float)
    market_cap = Column(BigInteger)

    quantity = Column(Integer)
    num_auctions = Column(Integer)
    last_updated = Column(DateTime, default=datetime.utcnow)