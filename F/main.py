import polars as pl
import numpy as np

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

class AlchemyDataSQL:
    """Banco de dados em SQL server baseado em alchemy"""
    def __init__(self, server, database, uid, password):
        self.server = server
        self.database = database
        self.uid = uid
        self.password = password
        conn_string = (
            f"Driver={{SQL SERVER}};"
            f"Server={server};"
            f"Database={database};"
            f"UID={uid};PWD={password};"
        )
        self.url_conn = URL.create(
            'mssql+pyodbc', query={'odbc_connect': conn_string}
        )
        self.engine = create_engine(self.url_conn, use_setinputsizes=False)
        self.Session = sessionmaker(bind=self.engine)
        self.conn_string = conn_string

    ### / DECORADORES \  ###
    @staticmethod # (método estático) -> Isso significa que ele não depende de uma instância específica da classe para ser chamado.
    def clear_none(func):
        def wrapper(self, *args, **kwargs):
            df = args[0]
            df = df.with_columns([
                pl.col(col).fill_null('').fill_nan('') for col in df.columns
            ])
            return func(self, df, *args[1:], **kwargs)
        return wrapper

    ### / FUNÇÕES INTERNAS \  ###
    def __where_compare(self, df: pl.DataFrame, on_values: list[str]) -> str:
        conditions = []
        for row in df.iter_rows(named=True):
            str_columns = '+'.join([f'CAST([{col}] AS NVARCHAR)' for col in on_values])
            str_values = '+'.join([f"'{row[col]}'" for col in on_values])
            conditions.append(f"({str_columns} = {str_values})")
        
        where_sql = "WHERE " + " OR ".join(conditions)
        return where_sql

    def run_query(self, query: str):
        with self.engine.connect() as connection:
            query = text(query)
            connection.execute(query)
            connection.commit()
        return True

    def table_exists(self, tb_name: str):
        """Verifica se uma determinada tabela existe em um banco"""
        with self.Session() as session:
            result = session.execute(
                text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tb_name}'")
            ).fetchone()
            return result is not None

    def read_database(self, query: str) -> pl.DataFrame:
        """ Leitura de uma tabela do banco de dados

        Parameters
        ----------
        query : str
            Query SQL.

        Returns
        -------
        pl.DataFrame
            Dataframe polars.
        """
        with self.engine.connect() as connection:
            df = pl.read_sql(query, connection)
            return df

    def get_columns(self, tb_name: str) -> list[str]:
        """Retorna todas as colunas da tabela"""
        with self.Session() as session:
            result = session.execute(
                text(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{tb_name}';
                """)
            ).fetchall()
            columns = [row[0] for row in result]
        return columns

    @clear_none
    def update_old_case(self, df: pl.DataFrame, tb_name: str, on_values: list[str], ignore_columns: list[str] = list()) -> None:
        where = self.__where_compare(df, on_values)
        df_sql = self.read_database(query=f'SELECT * FROM [dbo].[{tb_name}] WITH (NOLOCK) {where}')

        df_sql = df_sql.with_columns([
            pl.col(col).fill_null('').fill_nan('') for col in df_sql.columns
        ])
        df_sql = df_sql.drop(ignore_columns)

        df = df.drop(ignore_columns)

        df_join = df.join(df_sql, on=on_values, suffix="_sql")

        df_columns = df_join.select([col for col in df_join.columns if not col.endswith('_sql')])
        df_sql_columns = df_join.select([col for col in df_join.columns if col.endswith('_sql')])

        df_columns = df_columns.with_columns([
            pl.col(col).alias(col.replace('_sql', '')) for col in df_sql_columns.columns
        ])

        differences = df_columns != df_sql_columns

        df_dif = df_join.filter(differences.any(axis=1))
        df_dif = df_dif.select(df.columns)

        if not df_dif.is_empty():
            for row in df_dif.iter_rows(named=True):
                set_clause = ', '.join([f'[{col}] = \'{row[col]}\'' for col in df.columns if col not in on_values and col not in ignore_columns])
                where_clause = ' AND '.join([f'[{col}] = \'{row[col]}\'' for col in on_values])
                sql = f"""UPDATE [dbo].[{tb_name}] SET {set_clause} WHERE {where_clause}"""
                sql = sql.replace("= ''", '= null')
                self.run_query(query=sql)

    @clear_none
    def write_database(self, df: pl.DataFrame, tb_name: str, if_exists: str = 'append') -> bool:
        """Insere um DataFrame do Polars no banco de dados
        Parameters
        ----------
        df (DataFrame): Data frame que deseja inserir no banco
        tb_name (string): Nome da tabela em que o data frame será incluso
        if_exists (string): 
                           'fail': Gera um ValueError.
                           'replace': Elimina a tabela antes de inserir novos valores.
                           'append': Insere novos valores na tabela existente.
                           
       Returns
       -------
       bool
       
       """
        
        if '' in list(df.columns):
            df = df.drop([''])

        df = df.with_columns([
            pl.col(col).fill_null(np.nan).fill_nan(np.nan) for col in df.columns
        ])
        df = df.unique()

        df.write_sql(
            table_name=tb_name,
            schema='dbo',
            con=self.engine,
            if_exists=if_exists,
            index=False,
        )

        return True

# Exemplo de classe ORM
class EnriquecimentoCobrancaPJ(Base):
    __tablename__ = 'ENRIQUECIMENTO_COBRANÇA_PJ'
    id = Column(Integer, primary_key=True)
    # Adicione aqui as outras colunas da tabela

if __name__ == '__main__':
    db = AlchemyDataSQL(server='your_server', database='your_database', uid='your_uid', password='your_password')

    # Criar as tabelas no banco de dados
    Base.metadata.create_all(db.engine)

    # Criar uma sessão
    session = db.Session()

    # Ler dados do banco de dados usando ORM
    results = session.query(EnriquecimentoCobrancaPJ).all()
    for row in results:
        print(row)

    # Fechar a sessão
    session.close()