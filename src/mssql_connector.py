"""MSSQL baglanti yonetimi."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import pyodbc

from src.base_connector import BaseConnector
from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


class MssqlConnector(BaseConnector):
    """pyodbc tabanli read-only MSSQL baglanti yoneticisi."""

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)

    def _build_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE={self.config.dbname};"
            f"UID={self.config.user};"
            f"PWD={self.config.password};"
            f"LOGIN_TIMEOUT={self.config.connect_timeout};"
            f"TrustServerCertificate=yes;"
        )

    @contextmanager
    def connection(self) -> Generator:
        """Read-only baglanti context manager'i."""
        conn = pyodbc.connect(self._build_connection_string(), autocommit=True)
        conn.timeout = self.config.statement_timeout // 1000
        try:
            # Read uncommitted for lock-free profiling
            cursor = conn.cursor()
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            cursor.close()
            yield conn
        finally:
            conn.close()

    def execute_query(
        self,
        sql: str,
        params: Optional[Any] = None,
        fetch: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """SQL calistir, sonuclari dict listesi olarak don."""
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                if isinstance(params, dict):
                    cursor.execute(sql, list(params.values()))
                else:
                    cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            if fetch and cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return None

    def test_connection(self) -> bool:
        """Baglanti testi."""
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
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
            SELECT s.name AS schema_name
            FROM sys.schemas s
            INNER JOIN sys.sysusers u ON s.principal_id = u.uid
            WHERE s.name NOT IN (
                'sys', 'INFORMATION_SCHEMA', 'guest',
                'db_owner', 'db_accessadmin', 'db_securityadmin',
                'db_ddladmin', 'db_backupoperator', 'db_datareader',
                'db_datawriter', 'db_denydatareader', 'db_denydatawriter'
            )
            ORDER BY s.name;
        """
        rows = self.execute_query(sql)
        all_schemas = [r["schema_name"] for r in rows]

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
                t.name AS table_name,
                'BASE TABLE' AS table_type,
                ISNULL(SUM(p.row_count), 0) AS estimated_rows
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.dm_db_partition_stats p
                ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            WHERE s.name = ?
            GROUP BY t.name
            UNION ALL
            SELECT
                v.name AS table_name,
                'VIEW' AS table_type,
                0 AS estimated_rows
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = ?
            ORDER BY table_name;
        """
        try:
            return self.execute_query(sql, [schema, schema])
        except Exception:
            # Fallback: dm_db_partition_stats erisim yoksa sysindexes kullan
            sql_fallback = """
                SELECT
                    t.name AS table_name,
                    'BASE TABLE' AS table_type,
                    ISNULL(MAX(i.rowcnt), 0) AS estimated_rows
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.sysindexes i
                    ON t.object_id = i.id AND i.indid IN (0, 1)
                WHERE s.name = ?
                GROUP BY t.name
                UNION ALL
                SELECT
                    v.name AS table_name,
                    'VIEW' AS table_type,
                    0 AS estimated_rows
                FROM sys.views v
                INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                WHERE s.name = ?
                ORDER BY table_name;
            """
            return self.execute_query(sql_fallback, [schema, schema])

    def get_query_timeout_error(self) -> type:
        return pyodbc.Error

    def get_estimated_row_count(
        self, conn, schema: str, table: str
    ) -> Dict[str, Any]:
        """Tahmini satir sayisi (dm_db_partition_stats veya sysindexes)."""
        queries = [
            """SELECT ISNULL(SUM(p.row_count), 0)
               FROM sys.tables t
               INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
               INNER JOIN sys.dm_db_partition_stats p
                   ON t.object_id = p.object_id AND p.index_id IN (0, 1)
               WHERE s.name = ? AND t.name = ?""",
            """SELECT ISNULL(MAX(i.rowcnt), 0)
               FROM sys.tables t
               INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
               INNER JOIN sys.sysindexes i
                   ON t.object_id = i.id AND i.indid IN (0, 1)
               WHERE s.name = ? AND t.name = ?""",
        ]
        for sql in queries:
            try:
                cursor = conn.cursor()
                cursor.execute(sql, [schema, table])
                result = cursor.fetchone()
                count = result[0] if result else 0
                cursor.close()
                return {"row_count": int(count), "estimated": True}
            except Exception:
                continue
        return {"row_count": 0, "estimated": True}

    def validate_db_type(self, conn) -> bool:
        """MSSQL sunucu dogrulamasi."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            if "Microsoft SQL Server" not in version:
                logger.error(
                    "[%s] db_type=mssql ama sunucu MSSQL degil: %s",
                    self.config.alias, version,
                )
                return False
            return True
        except Exception as e:
            logger.warning("[%s] db_type dogrulama hatasi: %s", self.config.alias, e)
            return True
