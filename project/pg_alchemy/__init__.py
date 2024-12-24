import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import select
from models import table_registry, User

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, DateTime, func
from sqlalchemy.future import select


class PG_ALCHEMY:
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'SU_PASSWORD'), 'r') as file:
            password = file.read().strip()
        port = 5433
        dbname = "db_chronos"

        self.db_url = f"postgresql+psycopg://alex:{password}@localhost:{port}/{dbname}"
        self.engine = create_engine(
            self.db_url,
            pool_size=5,
            max_overflow=2,
            pool_timeout=10,
            pool_recycle=3600
        )


    def create_tables(self):
        table_registry.metadata.create_all(self.engine)


    def get_session(self):
        """Retorna uma sessão de banco de dados."""
        session = Session(self.engine)
        return session


if __name__ == "__main__":
    db = PG_ALCHEMY()
    session = db.get_session()
    
    novo_usuario = User(
        username="alex",
        password="1234",
        email="alexsandro77flores@gmail.com"
    )
    
    try:
        db.create_tables()
        results = session.execute(select(User))
        for user in results.scalars():
            print(user.username, user.email)

    except Exception as e:
        print(f"Erro ao adicionar o usuário: {e}")
        session.rollback()
    finally:
        session.close()
