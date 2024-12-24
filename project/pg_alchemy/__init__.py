import os
import sys 
sys.path.append(r'C:\Project\SQLAlchemy\project')
import polars as pl

from models import table_registry, User
from decorators.timer import timeit
from functools import wraps

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.orm import MappedAsDataclass


class PG_ALCHEMY:
    def __init__(self, local=False):
        if local:
            self.db_url = "postgresql+psycopg://postgres:1234@localhost:5432/postgres"
        else:
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

        self.__create_tables()


    @staticmethod # (método estático) -> Isso significa que ele não depende de uma instância específica da classe para ser chamado.
    def read_operation(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            session = self.get_session()
            try:
                session.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
                result = func(self, *args, session=session, **kwargs)
                return result
            except Exception as e:
                print(f"Erro durante a leitura: {e}")
                session.rollback()
                return None
            finally:
                session.close()
        return wrapper


    @staticmethod
    def commit_operation(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            session = self.get_session()
            try:
                result = func(self, session, *args, **kwargs)
                session.commit()
                return result
            except Exception as e:
                print(f"Erro durante a transação: {e}")
                session.rollback()
                return False
            finally:
                session.close()
        return wrapper


    def __create_tables(self):
        table_registry.metadata.create_all(self.engine)


    def get_session(self):
        session = Session(self.engine)
        return session


    @timeit
    @read_operation
    def read_database(self, tb_name: str, query: str = None, session=None) -> pl.DataFrame:
        if query is None: 
            query = f"SELECT * FROM {tb_name}"
        results = session.execute(text(query))
        rows = results.fetchall()
        df = pl.DataFrame(rows)
        return df


    @commit_operation 
    def add_user(self, session: Session, username: str, password: str, email: str): 
        new_user = User(username=username, password=password, email=email)
        session.add(new_user)

if __name__ == "__main__":
    db = PG_ALCHEMY(local=True)

    df1 = db.read_database(tb_name='users')

    print(df1)
