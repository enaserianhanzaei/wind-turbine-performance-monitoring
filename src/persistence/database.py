from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager

from config.constants import DATABASE_URL

# Create the engine
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}  # Needed only for SQLite
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Shared Base class for all models
Base = declarative_base()


def create_database():
    """Create the database tables if they do not exist."""
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully (if they didn't already exist).")


@contextmanager
def get_db_session():
    """Provide a transactional scope around a series of operations."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()