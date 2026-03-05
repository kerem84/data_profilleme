"""Temel profilleme metrikleri: satir sayisi, NULL orani, distinct, min/max."""

import logging
from typing import Any, Dict

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)


class BasicMetrics:
    """Temel kolon metrikleri hesaplayici."""

    def __init__(self, sql_loader: SqlLoader, connector):
        self.sql = sql_loader
        self.connector = connector
        self._timeout_error = connector.get_query_timeout_error()

    def get_row_count(self, conn, schema: str, table: str) -> Dict[str, Any]:
        """
        Tablo satir sayisi. Timeout durumunda tahmini kullanir.
        Returns: {"row_count": int, "estimated": bool}
        """
        sql = self.sql.load("row_count", schema_name=schema, table_name=table)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()
                return {"row_count": result[0], "estimated": False}
        except self._timeout_error:
            logger.warning(
                "[%s.%s] row_count timeout, tahmini kullaniliyor", schema, table
            )
            return self.connector.get_estimated_row_count(conn, schema, table)
        except Exception as e:
            logger.warning("[%s.%s] row_count hatasi: %s", schema, table, e)
            return self.connector.get_estimated_row_count(conn, schema, table)

    def get_column_basics(
        self, conn, schema: str, table: str, column: str, row_count: int
    ) -> Dict[str, Any]:
        """
        Kolon icin NULL orani, distinct sayisi, min/max.
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
        except self._timeout_error:
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
        except self._timeout_error:
            logger.warning("[%s.%s.%s] min_max timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] min_max hatasi: %s", schema, table, column, e)

        return result
