from sqlalchemy import Column, Integer, String, BigInteger, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
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