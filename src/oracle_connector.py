"""Oracle DB baglanti yonetimi."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import oracledb

from src.base_connector import BaseConnector
from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)

# Oracle sistem semalari (filtreleme icin)
_SYSTEM_SCHEMAS = {
    "SYS", "SYSTEM", "DBSNMP", "OUTLN", "XDB", "CTXSYS", "MDSYS",
    "OLAPSYS", "WMSYS", "ORDDATA", "ORDSYS", "ANONYMOUS", "APPQOSSYS",
    "AUDSYS", "DBSFWUSER", "DIP", "GGSYS", "GSMADMIN_INTERNAL",
    "GSMCATUSER", "GSMUSER", "LBACSYS", "MDDATA", "OJVMSYS",
    "ORACLE_OCM", "REMOTE_SCHEDULER_AGENT", "SI_INFORMTN_SCHEMA",
    "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "XS$NULL",
}


class OracleConnector(BaseConnector):
    """oracledb tabanli read-only Oracle baglanti yoneticisi."""

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)

    def _build_dsn(self) -> str:
        """Oracle DSN olustur (service_name veya SID)."""
        service = self.config.service_name or self.config.dbname
        return oracledb.makedsn(
            self.config.host,
            self.config.port,
            service_name=service,
        )

    @contextmanager
    def connection(self) -> Generator:
        """Read-only baglanti context manager'i."""
        dsn = self._build_dsn()
        conn = oracledb.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=dsn,
        )
        # Statement timeout (ms)
        conn.call_timeout = self.config.statement_timeout
        try:
            # READ ONLY transaction — hicbir DML calistirilamaz, lock alinmaz
            cursor = conn.cursor()
            cursor.execute("SET TRANSACTION READ ONLY")
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
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            if fetch and cursor.description:
                columns = [desc[0].lower() for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return None

    def test_connection(self) -> bool:
        """Baglanti testi."""
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.close()
            logger.info(
                "[%s] Baglanti basarili: %s:%s/%s",
                self.config.alias,
                self.config.host,
                self.config.port,
                self.config.service_name or self.config.dbname,
            )
            return True
        except Exception as e:
            logger.error("[%s] Baglanti hatasi: %s", self.config.alias, e)
            return False

    def discover_schemas(self) -> List[str]:
        """Non-system schema (owner) isimlerini dondur."""
        # Tablolari olan owner'lari bul (bos semalari atla)
        sql = """
            SELECT DISTINCT owner AS schema_name
            FROM all_tables
            WHERE owner NOT IN ({placeholders})
            ORDER BY owner
        """.format(
            placeholders=", ".join(f"'{s}'" for s in _SYSTEM_SCHEMAS)
        )
        rows = self.execute_query(sql)
        all_schemas = [r["schema_name"] for r in (rows or [])]

        # APEX_ ve FLOWS_ prefix'li semalari filtrele
        all_schemas = [
            s for s in all_schemas
            if not s.startswith("APEX_") and not s.startswith("FLOWS_")
        ]

        sf = self.config.schema_filter
        if sf == "*":
            return all_schemas
        if isinstance(sf, list):
            # Case-insensitive eslestirme (Oracle uppercase default)
            sf_upper = {s.upper() for s in sf}
            return [s for s in all_schemas if s.upper() in sf_upper]
        return all_schemas

    def discover_tables(self, schema: str) -> List[Dict[str, Any]]:
        """Bir sema icindeki tablo bilgilerini dondur."""
        sql = """
            SELECT
                table_name,
                'BASE TABLE' AS table_type,
                NVL(num_rows, 0) AS estimated_rows
            FROM all_tables
            WHERE owner = :schema
            UNION ALL
            SELECT
                view_name AS table_name,
                'VIEW' AS table_type,
                0 AS estimated_rows
            FROM all_views
            WHERE owner = :schema
            ORDER BY table_name
        """
        return self.execute_query(sql, {"schema": schema}) or []

    def get_query_timeout_error(self) -> type:
        """Oracle timeout/cancel exception (ORA-01013)."""
        return oracledb.DatabaseError

    def get_estimated_row_count(
        self, conn, schema: str, table: str
    ) -> Dict[str, Any]:
        """Tahmini satir sayisi (ALL_TABLES.NUM_ROWS)."""
        sql = """
            SELECT NVL(num_rows, 0)
            FROM all_tables
            WHERE owner = :schema AND table_name = :table_name
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql, {"schema": schema, "table_name": table})
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            return {"row_count": int(count), "estimated": True}
        except Exception:
            return {"row_count": 0, "estimated": True}

    def get_table_size(self, conn, schema: str, table: str) -> Optional[int]:
        """Tablo + index boyutu (byte). DBA_SEGMENTS veya USER_SEGMENTS."""
        queries = [
            # DBA erisimiyle
            """SELECT NVL(SUM(bytes), 0)
               FROM dba_segments
               WHERE owner = :schema AND segment_name = :table_name""",
            # DBA erisimi yoksa user_segments fallback
            """SELECT NVL(SUM(bytes), 0)
               FROM user_segments
               WHERE segment_name = :table_name""",
        ]
        for sql in queries:
            try:
                cursor = conn.cursor()
                if "dba_segments" in sql:
                    cursor.execute(sql, {"schema": schema, "table_name": table})
                else:
                    cursor.execute(sql, {"table_name": table})
                result = cursor.fetchone()
                cursor.close()
                size = int(result[0]) if result and result[0] else None
                if size is not None:
                    return size
            except Exception:
                continue
        return None

    def validate_db_type(self, conn) -> bool:
        """Oracle sunucu dogrulamasi."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
            version = cursor.fetchone()[0]
            cursor.close()
            if "Oracle" not in version:
                logger.error(
                    "[%s] db_type=oracle ama sunucu Oracle degil: %s",
                    self.config.alias, version,
                )
                return False
            return True
        except Exception as e:
            logger.warning("[%s] db_type dogrulama hatasi: %s", self.config.alias, e)
            return True
