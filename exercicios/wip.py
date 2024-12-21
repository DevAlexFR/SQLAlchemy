from sqlalchemy import create_engine

engine = create_engine(
    'sqlite://'
    # ,echo=True # Com isso gera log do que esta acontecendo dentro do banco "dentro da conexao" o que e como o banco esta fazendo.
)

con = engine.connect()

print(con.connection.dbapi_connection)

con.close()
