import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

def _persistent_sqlite_url() -> str | None:
    # In Amvera, persistent storage is mounted to /data.
    data_dir = Path("/data")
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        return "sqlite:////data/cosplay.db"
    return None


def _resolve_database_url() -> str:
    configured = os.getenv("DATABASE_URL", "").strip()
    persistent_sqlite = _persistent_sqlite_url()

    # Safety: in environments with /data, force SQLite to persistent storage.
    # This prevents accidental deployments with ephemeral sqlite path.
    if configured:
        if configured.startswith("sqlite") and persistent_sqlite:
            if configured != persistent_sqlite:
                print(f"[db] DATABASE_URL overridden to persistent path: {persistent_sqlite}")
            return persistent_sqlite
        return configured

    return persistent_sqlite or "sqlite:///./cosplay.db"


DATABASE_URL = _resolve_database_url()

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
