from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings


def make_database(settings: Settings):
    engine = create_async_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"command_timeout": 10},
    )
    return engine, async_sessionmaker(engine, expire_on_commit=False)
