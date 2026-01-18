import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert
from database import Auction, create_tables

class PostgresClient:
    def __init__(self):
        user = os.getenv("DAGSTER_POSTGRES_USER")
        password = os.getenv("DAGSTER_POSTGRES_PASSWORD")
        db = os.getenv("DAGSTER_POSTGRES_DB")

        host = os.getenv("POSTGRES_HOST", "postgres") 
        port = os.getenv("POSTGRES_PORT", "5432")

        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind = self.engine)
        self.Auction = Auction

        create_tables(self.connection_string)

    def insert_auctions(self, auction_dict_list):
        session = self.Session()
        try:
            if not auction_dict_list:
                return 0
            
            stmt = insert(self.Auction).values(auction_dict_list)
            stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
            result = session.execute(stmt)
            session.commit()
            
            return result.rowcount
           
        except Exception as e:
            session.rollback()
            print(f"Erro ao inserir no postgres: {e}")
            raise e
        finally:
            session.close()
    
    def delete_old_data(self, days_retention=30):
        session = self.Session()
        try:
            sql = f"DELETE FROM silver_auctions WHERE created_at < NOW() - INTERVAL '{days_retention} days'"

            result = session.execute(text(sql))
            session.commit()

            return result.rowcount
        except Exception as e:
            session.rollback()
            print(f"Erro ao deletar dados antigos no postgres: {e}")
            raise e
        finally:
            session.close()