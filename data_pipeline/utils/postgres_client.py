import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert
from utils.database import Auction, create_tables

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

    def get_missing_item_ids(self, limit=100):
        # Retorna IDs que estão na silver_auctions mas não na dim_items
        # Limite em 100 para não ultrapassar o Rate Limit da API
        session = self.Session()
        try:
            sql = """
            SELECT DISTINCT s.item_id
            FROM silver_auctions s
            LEFT JOIN dim_items d ON s.item_id = d.item_id
            WHERE d.item_id IS NULL
            LIMIT :limit
            """
            result = session.execute(text(sql), {'limit': limit})
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"Erro ao buscar item_ids faltantes: {e}")
            return []
        finally:
            session.close()
    
    def insert_item_dimensions(self, items_dict_list):
        session = self.Session()
        from utils.database import ItemDimension

        try:
            if not items_dict_list:
                return 0
            stmt = insert(ItemDimension).values(items_dict_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=['item_id'],
                set_={
                    "name": stmt.excluded.name,
                    "icon_url": stmt.excluded.icon_url,
                    "last_updated": stmt.excluded.last_updated
                }
            )

            result = session.execute(stmt)
            session.commit()
            return result.rowcount
        except Exception as e:
            session.rollback()
            print(f"Erro ao inserir itens: {e}")
            raise e
        finally:
            session.close()

    def execute_sql_command(self, sql_query):
        session = self.Session()
        try:
            session.execute(text(sql_query))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Erro ao executar comando SQL: {e}")
            raise e
        finally:
            session.close()
            
    