"""PostgreSQL baglanti yonetimi."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras

from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """psycopg2 tabanli read-only PostgreSQL baglanti yoneticisi."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Read-only baglanti context manager'i."""
        conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.dbname,
            user=self.config.user,
            password=self.config.password,
            connect_timeout=self.config.connect_timeout,
            options=f"-c statement_timeout={self.config.statement_timeout}",
        )
        conn.set_session(readonly=True, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """Parametrik SQL calistir, sonuclari dict listesi olarak don."""
        with self.connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                if fetch:
                    return [dict(row) for row in cur.fetchall()]
        return None

    def execute_query_with_conn(
        self,
        conn: psycopg2.extensions.connection,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """Mevcut baglanti ile SQL calistir."""
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch:
                return [dict(row) for row in cur.fetchall()]
        return None

    def test_connection(self) -> bool:
        """Baglanti testi. Basarili ise True doner."""
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info(
                "[%s] Baglanti basarili: %s:%s/%s",
                self.config.alias,
                self.config.host,
                self.config.port,
                self.config.dbname,
            )
            return True
        except Exception as e:
            logger.error("[%s] Baglanti hatasi: %s", self.config.alias, e)
            return False

    def discover_schemas(self) -> List[str]:
        """Non-system schema isimlerini dondur."""
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name;
        """
        rows = self.execute_query(sql)
        all_schemas = [r["schema_name"] for r in rows]

        # Config filtresi uygula
        sf = self.config.schema_filter
        if sf == "*":
            return all_schemas
        if isinstance(sf, list):
            return [s for s in all_schemas if s in sf]
        return all_schemas

    def discover_tables(self, schema: str) -> List[Dict[str, Any]]:
        """Bir sema icindeki tablo bilgilerini dondur."""
        sql = """
            SELECT
                t.table_name,
                t.table_type,
                COALESCE(s.n_live_tup, 0) AS estimated_rows
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                ON t.table_schema = s.schemaname
                AND t.table_name = s.relname
            WHERE t.table_schema = %(schema)s
              AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name;
        """
        return self.execute_query(sql, {"schema": schema})
