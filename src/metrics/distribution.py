"""Dagilim metrikleri: Top N, numerik istatistik, histogram."""

import logging
from typing import Any, Dict, List, Optional

import psycopg2

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)

# Numerik veri tipleri
NUMERIC_TYPES = {
    "smallint", "integer", "bigint", "decimal", "numeric",
    "real", "double precision", "serial", "bigserial",
    "int2", "int4", "int8", "float4", "float8", "money",
}


def is_numeric_type(data_type: str) -> bool:
    """Veri tipinin numerik olup olmadigini kontrol et."""
    return data_type.lower() in NUMERIC_TYPES


class DistributionMetrics:
    """Deger dagilimi ve numerik istatistik hesaplayici."""

    def __init__(self, sql_loader: SqlLoader):
        self.sql = sql_loader

    def get_top_n(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
        top_n: int,
        row_count: int,
    ) -> List[Dict[str, Any]]:
        """En sik N degeri dondur."""
        if row_count == 0:
            return []

        try:
            sql = self.sql.load(
                "top_n_values",
                schema_name=schema,
                table_name=table,
                column_name=column,
            )
            with conn.cursor() as cur:
                cur.execute(sql, {"total_count": row_count, "top_n": top_n})
                rows = cur.fetchall()
                return [
                    {"value": str(r[0]), "frequency": r[1], "pct": float(r[2])}
                    for r in rows
                ]
        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] top_n timeout", schema, table, column)
            return []
        except Exception as e:
            logger.warning("[%s.%s.%s] top_n hatasi: %s", schema, table, column, e)
            return []

    def get_numeric_stats(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
    ) -> Optional[Dict[str, Any]]:
        """Numerik kolon istatistikleri: mean, stddev, percentiles."""
        try:
            sql = self.sql.load(
                "numeric_stats",
                schema_name=schema,
                table_name=table,
                column_name=column,
            )
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row and row[0] is not None:
                    return {
                        "mean": float(row[0]) if row[0] else None,
                        "stddev": float(row[1]) if row[1] else None,
                        "p01": float(row[2]) if row[2] else None,
                        "p05": float(row[3]) if row[3] else None,
                        "p25": float(row[4]) if row[4] else None,
                        "p50": float(row[5]) if row[5] else None,
                        "p75": float(row[6]) if row[6] else None,
                        "p95": float(row[7]) if row[7] else None,
                        "p99": float(row[8]) if row[8] else None,
                    }
        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] numeric_stats timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] numeric_stats hatasi: %s", schema, table, column, e)
        return None

    def get_histogram(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
        buckets: int = 20,
    ) -> Optional[List[Dict[str, Any]]]:
        """Numerik kolon icin esit genislikte histogram."""
        try:
            sql = f"""
                WITH stats AS (
                    SELECT
                        MIN("{column}"::numeric) AS min_val,
                        MAX("{column}"::numeric) AS max_val
                    FROM "{schema}"."{table}"
                    WHERE "{column}" IS NOT NULL
                ),
                histogram AS (
                    SELECT
                        WIDTH_BUCKET("{column}"::numeric, s.min_val, s.max_val + 0.0001, {buckets}) AS bucket,
                        COUNT(*) AS freq
                    FROM "{schema}"."{table}" t, stats s
                    WHERE "{column}" IS NOT NULL
                    GROUP BY bucket
                    ORDER BY bucket
                )
                SELECT
                    h.bucket,
                    s.min_val + (h.bucket - 1) * (s.max_val - s.min_val) / {buckets} AS lower_bound,
                    s.min_val + h.bucket * (s.max_val - s.min_val) / {buckets} AS upper_bound,
                    h.freq
                FROM histogram h, stats s
                ORDER BY h.bucket;
            """
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                return [
                    {
                        "bucket": r[0],
                        "lower_bound": float(r[1]) if r[1] else 0,
                        "upper_bound": float(r[2]) if r[2] else 0,
                        "frequency": r[3],
                    }
                    for r in rows
                ]
        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] histogram timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] histogram hatasi: %s", schema, table, column, e)
        return None
