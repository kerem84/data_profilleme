"""SAP HANA BW baglanti yonetimi."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from hdbcli import dbapi

from src.base_connector import BaseConnector
from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)

# SAP HANA sistem semalari (filtreleme icin)
_SYSTEM_SCHEMAS = {
    "SYS", "SYSTEM", "_SYS_AFL", "_SYS_BI", "_SYS_BIC", "_SYS_EPM",
    "_SYS_PLAN_STABILITY", "_SYS_REPO", "_SYS_RT", "_SYS_SECURITY",
    "_SYS_SQL_ANALYZER", "_SYS_STATISTICS", "_SYS_TASK", "_SYS_XS",
    "_SYS_DATA_ANONYMIZATION",
}

# SAP dil kodu mapping
_LANG_MAP = {"TR": "T", "EN": "E", "DE": "D"}


class HanaBwConnector(BaseConnector):
    """hdbcli tabanli read-only SAP HANA BW baglanti yoneticisi."""

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self._bw_table_filter = config.bw_table_filter
        self._sap_lang = _LANG_MAP.get(
            config.bw_description_lang.upper(), "T"
        )

    @contextmanager
    def connection(self) -> Generator:
        """Read-only baglanti context manager'i."""
        conn = dbapi.connect(
            address=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                f"SET 'statement_timeout' = '{self.config.statement_timeout}'"
            )
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
                cursor.execute("SELECT 1 FROM DUMMY")
                cursor.close()
            logger.info(
                "[%s] Baglanti basarili: %s:%s",
                self.config.alias,
                self.config.host,
                self.config.port,
            )
            return True
        except Exception as e:
            logger.error("[%s] Baglanti hatasi: %s", self.config.alias, e)
            return False

    def discover_schemas(self) -> List[str]:
        """Non-system schema isimlerini dondur."""
        sql = """
            SELECT SCHEMA_NAME
            FROM SYS.SCHEMAS
            WHERE HAS_PRIVILEGES = 'TRUE'
            ORDER BY SCHEMA_NAME
        """
        rows = self.execute_query(sql)
        all_schemas = [
            r["schema_name"] for r in (rows or [])
            if r["schema_name"] not in _SYSTEM_SCHEMAS
            and not r["schema_name"].startswith("_SYS_")
        ]

        sf = self.config.schema_filter
        if sf == "*":
            return all_schemas
        if isinstance(sf, list):
            sf_upper = {s.upper() for s in sf}
            return [s for s in all_schemas if s.upper() in sf_upper]
        if isinstance(sf, str):
            return [s for s in all_schemas if s.upper() == sf.upper()]
        return all_schemas

    def discover_tables(self, schema: str) -> List[Dict[str, Any]]:
        """BW tablo filtresine gore tablo listesi dondur, RSDCUBET aciklamalari ile."""
        sql = """
            SELECT
                t.TABLE_NAME AS table_name,
                'BASE TABLE' AS table_type,
                COALESCE(t.RECORD_COUNT, 0) AS estimated_rows,
                ct.TXTLG AS table_description
            FROM TABLES t
            LEFT JOIN RSDCUBET ct
                ON t.TABLE_NAME LIKE '%' || ct.INFOCUBE || '%'
                AND ct.OBJVERS = 'A'
                AND ct.LANGU = ?
            WHERE t.SCHEMA_NAME = ?
            ORDER BY t.TABLE_NAME
        """
        rows = self.execute_query(sql, [self._sap_lang, schema]) or []

        if self._bw_table_filter:
            rows = [
                r for r in rows
                if any(
                    r["table_name"].startswith(prefix)
                    for prefix in self._bw_table_filter
                )
            ]

        return rows

    def get_query_timeout_error(self) -> type:
        """HANA timeout exception."""
        return dbapi.Error

    def get_estimated_row_count(
        self, conn, schema: str, table: str
    ) -> Dict[str, Any]:
        """Tahmini satir sayisi (TABLES.RECORD_COUNT)."""
        sql = """
            SELECT COALESCE(RECORD_COUNT, 0)
            FROM TABLES
            WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql, [schema, table])
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            return {"row_count": int(count), "estimated": True}
        except Exception:
            return {"row_count": 0, "estimated": True}

    def get_table_size(self, conn, schema: str, table: str) -> Optional[int]:
        """Tablo boyutu (byte). M_TABLE_PERSISTENCE_STATISTICS kullanir."""
        sql = """
            SELECT COALESCE(SUM(DISK_SIZE), 0)
            FROM M_TABLE_PERSISTENCE_STATISTICS
            WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql, [schema, table])
            result = cursor.fetchone()
            cursor.close()
            size = int(result[0]) if result and result[0] else None
            return size if size and size > 0 else None
        except Exception:
            return None

    def validate_db_type(self, conn) -> bool:
        """HANA sunucu dogrulamasi."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION FROM M_DATABASE")
            version = cursor.fetchone()[0]
            cursor.close()
            logger.info("[%s] HANA version: %s", self.config.alias, version)
            return True
        except Exception as e:
            logger.warning("[%s] db_type dogrulama hatasi: %s", self.config.alias, e)
            return True

    def get_sap_lang_code(self) -> str:
        """SAP dil kodunu dondur (metadata sorgulari icin)."""
        return self._sap_lang
