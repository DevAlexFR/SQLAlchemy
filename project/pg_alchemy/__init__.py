import os
import sys
sys.path.append(r'C:\Project\SQLAlchemy\project')
import polars as pl

from models import table_registry, User
from decorators.timer import timeit # Importação do decorador de tempo
from functools import wraps # Utilizado para que os decoradores possam acessar o nome real da função e seus argumentos

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


class PG_ALCHEMY:
    """
        Classe de controle do postgres com sqlalchemy e polars
    """

    def __init__(self, local=False):
        """
        Inicializa a conexão com o banco de dados, localmente ou em produção.

        Parameters:
        local (bool): Flag para indicar se a conexão é local.
        """
        if local:
            self.db_url = "postgresql+psycopg://postgres:1234@localhost:5432/postgres"
        else:
            # Lê a senha de um arquivo seguro.
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

        # Cria as tabelas no banco de dados se elas não existirem.
        self.__create_tables()


    ### / DECORADORES \  ###
    @staticmethod # (método estático) -> Isso significa que ele não depende de uma instância específica da classe para ser chamado.
    def read_operation(func):
        """
        Decorador para operações de leitura no banco de dados, gerencia a sessão e executa rollback em caso de erro.

        Parameters:
        func (function): Função a ser decorada.

        Returns:
        function: Função decorada.
        """
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


    @staticmethod # (método estático) -> Isso significa que ele não depende de uma instância específica da classe para ser chamado.
    def commit_operation(func):
        """
        Decorador para operações que requerem commit no banco de dados, gerencia a sessão e executa rollback em caso de erro.

        Parameters:
        func (function): Função a ser decorada.

        Returns:
        function: Função decorada.
        """
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


    ### / FUNÇÕES INTERNAS \  ###
    def __create_tables(self):
        """
        Cria as tabelas do core, caso não existam no banco de dados.
        """
        table_registry.metadata.create_all(self.engine)


    ### / FUNÇÕES DE CONTROLE \  ###
    def get_session(self) -> Session:
        """
        Inicia uma sessão com a engine do banco de dados.

        Returns:
        Session: Sessão do SQLAlchemy.
        """
        session = Session(self.engine)
        return session


    ### / FUNÇÕES DE LEITURA \  ###
    @timeit
    @read_operation
    def read_database(self, tb_name: str, query: str = None, session: Session = None) -> pl.DataFrame:
        """
        Leitura da tabela do banco de dados.

        Parameters:
        tb_name (str): Nome da tabela.
        query (str, optional): Query personalizada para consulta, por padrão None.
        session (Session, optional): Sessão iniciada pelo decorador, por padrão None.

        Returns:
        pl.DataFrame: Retorna um DataFrame do Polars com os dados da tabela.
        """
        if query is None:
            query = f"SELECT * FROM {tb_name};"
        data = session.execute(text(query)).fetchall()
        df = pl.DataFrame(data)
        return df


    ### / FUNÇÕES DE MANIPULAÇÃO \  ###
    @commit_operation
    def add_user(self, session: Session, username: str, password: str, email: str):
        new_user = User(username=username, password=password, email=email)
        session.add(new_user)



if __name__ == "__main__":
    db = PG_ALCHEMY(local=True)

    df = db.read_database(tb_name='users')
    print(df)
