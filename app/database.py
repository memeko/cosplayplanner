import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

def _default_sqlite_url() -> str:
    # In Amvera, persistent storage is mounted to /data.
    data_dir = Path("/data")
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        return "sqlite:////data/cosplay.db"
    return "sqlite:///./cosplay.db"


DATABASE_URL = os.getenv("DATABASE_URL", _default_sqlite_url())

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, future=True)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
