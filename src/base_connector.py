"""Veritabani baglanti arayuzu."""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Veritabani baglanti arayuzu. Tum dialect'ler bunu uygular."""

    def __init__(self, config: DatabaseConfig):
        self.config = config

    @abstractmethod
    @contextmanager
    def connection(self) -> Generator:
        """Read-only baglanti context manager'i."""
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Baglanti testi."""
        ...

    @abstractmethod
    def discover_schemas(self) -> List[str]:
        """Non-system schema isimlerini dondur."""
        ...

    @abstractmethod
    def discover_tables(self, schema: str) -> List[Dict[str, Any]]:
        """Bir sema icindeki tablo bilgilerini dondur."""
        ...

    @abstractmethod
    def get_query_timeout_error(self) -> type:
        """Dialect-specific query timeout/cancel exception sinifi."""
        ...

    @abstractmethod
    def get_estimated_row_count(
        self, conn, schema: str, table: str
    ) -> Dict[str, Any]:
        """Tahmini satir sayisi."""
        ...

    @abstractmethod
    def validate_db_type(self, conn) -> bool:
        """Konfigurasyon db_type ile gercek veritabani tipini dogrula."""
        ...

    def get_table_size(self, conn, schema: str, table: str) -> Optional[int]:
        """Tablo boyutunu byte cinsinden dondur. Alt siniflar override edebilir."""
        return None
