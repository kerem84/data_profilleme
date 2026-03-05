"""Temel profilleme metrikleri: satir sayisi, NULL orani, distinct, min/max."""

import logging
from typing import Any, Dict, Optional

import psycopg2

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)


class BasicMetrics:
    """Temel kolon metrikleri hesaplayici."""

    def __init__(self, sql_loader: SqlLoader):
        self.sql = sql_loader

    def get_row_count(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        """
        Tablo satir sayisi. Timeout durumunda pg_stat tahmini kullanir.
        Returns: {"row_count": int, "estimated": bool}
        """
        sql = self.sql.load("row_count", schema_name=schema, table_name=table)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()
                return {"row_count": result[0], "estimated": False}
        except psycopg2.errors.QueryCanceled:
            logger.warning(
                "[%s.%s] row_count timeout, pg_stat tahmini kullaniliyor", schema, table
            )
            return self._get_estimated_row_count(conn, schema, table)
        except Exception as e:
            logger.warning("[%s.%s] row_count hatasi: %s", schema, table, e)
            return self._get_estimated_row_count(conn, schema, table)

    def _get_estimated_row_count(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        """pg_stat_user_tables'dan tahmini satir sayisi."""
        sql = """
            SELECT COALESCE(n_live_tup, 0) AS estimated_rows
            FROM pg_stat_user_tables
            WHERE schemaname = %(schema)s AND relname = %(table)s;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(sql, {"schema": schema, "table": table})
                result = cur.fetchone()
                count = result[0] if result else 0
                return {"row_count": int(count), "estimated": True}
        except Exception:
            return {"row_count": 0, "estimated": True}

    def get_column_basics(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
        row_count: int,
    ) -> Dict[str, Any]:
        """
        Kolon icin NULL orani, distinct sayisi, min/max.
        Returns dict with: total_count, non_null_count, null_count, null_ratio,
                          distinct_count, distinct_ratio, min_value, max_value
        """
        result: Dict[str, Any] = {
            "total_count": row_count,
            "non_null_count": 0,
            "null_count": row_count,
            "null_ratio": 1.0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
            "min_value": None,
            "max_value": None,
        }

        if row_count == 0:
            return result

        # NULL ratio + distinct
        try:
            sql = self.sql.load(
                "null_ratio",
                schema_name=schema,
                table_name=table,
                column_name=column,
            )
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row:
                    result["total_count"] = row[0]
                    result["non_null_count"] = row[1]
                    result["null_count"] = row[2]
                    result["null_ratio"] = float(row[3])
                    result["distinct_count"] = row[4]
                    result["distinct_ratio"] = float(row[5])
        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] null_ratio timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] null_ratio hatasi: %s", schema, table, column, e)

        # Min/max
        try:
            sql = self.sql.load(
                "min_max",
                schema_name=schema,
                table_name=table,
                column_name=column,
            )
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row:
                    result["min_value"] = row[0]
                    result["max_value"] = row[1]
        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] min_max timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] min_max hatasi: %s", schema, table, column, e)

        return result
