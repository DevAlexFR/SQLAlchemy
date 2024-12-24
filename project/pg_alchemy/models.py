import uuid
from datetime import datetime
from sqlalchemy.orm import registry, Mapped, mapped_column
from sqlalchemy.sql import func


table_registry = registry()

@table_registry.mapped_as_dataclass
class User:
    __tablename__ = "users"  # Nome da tabela no banco de dados

    id: Mapped[str] = mapped_column(
        init=False,
        primary_key=True,
        default=lambda: str(uuid.uuid4())  # Gera um UUID aleatório
    )
    username: Mapped[str] = mapped_column(unique=True, nullable=False)
    password: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str] = mapped_column(unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        init=False,
        server_default=func.now()
    )
