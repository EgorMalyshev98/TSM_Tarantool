from typing import AsyncGenerator, Generator

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, declarative_base, registry, sessionmaker

from src.config import db_config

DATABASE_URL = db_config.get_db_url()
ASYNC_DATABASE_URL = db_config.get_async_db_url()


Base = declarative_base()

convention = {
    "ix": "ix_%(column_0_label)s",  # INDEX
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",  # UNIQUE
    "ck": "ck_%(table_name)s_%(constraint_name)s",  # CHECK
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",  # FOREIGN KEY
    "pk": "pk_%(table_name)s",  # PRIMARY KEY
}

mapper_registry = registry(metadata=MetaData(naming_convention=convention))


class BaseModel(DeclarativeBase):
    registry = mapper_registry
    metadata = mapper_registry.metadata


engine = create_engine(DATABASE_URL, pool_size=20, max_overflow=10, pool_timeout=30, pool_pre_ping=True)
engine.echo = False


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False,
)


def get_session() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session


async_engine = create_async_engine(ASYNC_DATABASE_URL, pool_size=20, max_overflow=10, pool_timeout=30, pool_recycle=180)
async_engine.echo = False
async_session_maker = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session
