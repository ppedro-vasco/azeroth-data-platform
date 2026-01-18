from sqlalchemy import create_engine, Column, Integer, String, BigInteger, DateTime
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
    created_at = Column(DateTime(timezone=True), server_default=func.now())

def create_tables(connection_string):
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    print("Schema verificado/criado com sucesso!")