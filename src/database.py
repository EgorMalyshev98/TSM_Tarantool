import threading
import time
from datetime import datetime
from threading import Lock
from typing import AsyncGenerator, Dict, Generator
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg_pool import ConnectionPool
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, declarative_base, registry, sessionmaker

from src.config import db_config
from src.log import logger

DATABASE_URL = db_config.get_db_url()
ASYNC_DATABASE_URL = db_config.get_async_db_url()
SESSION_TIMEOUT = 30


pool = ConnectionPool(DATABASE_URL, min_size=5, max_size=50, max_wait=30, max_lifetime=600, max_idle=300)


def get_session() -> Generator[Connection, None, None]:
    with pool.connection() as session:
        yield session


session_lock = Lock()


class Session:
    def __init__(self, tid: UUID, connection: Connection, created_at: datetime):
        self.tid = tid
        self.connection = connection
        self.created_at = created_at


class SessionManger:
    def __init__(self):
        self.sessions: Dict[str, Session] = {}

    def create(self):
        connection = pool.getconn()
        tid = str(uuid4())
        now = time.time()

        with session_lock:
            t = Session(tid=tid, connection=connection, created_at=now)
            self.sessions[t.tid] = t

        logger.debug(f"connection {tid} created")
        return tid

    def complete(self, tid: str):
        with session_lock:
            session = self.sessions.pop(tid, None)
            if session:
                session.connection.commit()
                pool.putconn(session.connection)
            logger.debug(f"connection {tid} completed")

    def session_cleanup(self):
        while True:
            time.sleep(10)
            with session_lock:
                now = time.time()
                expired_sessions = [tid for tid, t in self.sessions.items() if now - t.created_at > SESSION_TIMEOUT]
                for tid in expired_sessions:
                    session = self.sessions.pop(tid, None)
                    if session:
                        session.connection.rollback()
                        pool.putconn(session.connection)
                        logger.debug(f"Session {tid} rolled back due to timeout")


session_manager = SessionManger()

t = threading.Thread(target=session_manager.session_cleanup, daemon=True)
t.start()


#########################################################

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


async_engine = create_async_engine(ASYNC_DATABASE_URL, pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=180)
async_engine.echo = False
async_session_maker = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session
