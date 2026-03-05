"""Veritabani connector factory."""

from src.base_connector import BaseConnector
from src.config_loader import DatabaseConfig


def create_connector(config: DatabaseConfig) -> BaseConnector:
    """Config'e gore uygun connector olustur."""
    if config.db_type == "mssql":
        from src.mssql_connector import MssqlConnector
        return MssqlConnector(config)
    else:
        from src.db_connector import PostgresConnector
        return PostgresConnector(config)
