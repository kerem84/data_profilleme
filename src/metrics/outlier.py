"""IQR tabanli outlier tespiti."""

import logging
from typing import Any, Dict, Optional

import psycopg2

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)


class OutlierDetector:
    """Numerik kolonlar icin IQR tabanli outlier tespiti."""

    def __init__(self, sql_loader: SqlLoader):
        self.sql = sql_loader

    def detect(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
        iqr_multiplier: float = 1.5,
    ) -> Optional[Dict[str, Any]]:
        """
        IQR yontemiyle outlier tespit et.
        Returns: {"q1", "q3", "iqr", "lower_bound", "upper_bound",
                  "outlier_count", "outlier_ratio"}
        """
        try:
            sql = self.sql.load(
                "outlier_detection",
                schema_name=schema,
                table_name=table,
                column_name=column,
            )

            with conn.cursor() as cur:
                cur.execute(sql, {"iqr_multiplier": iqr_multiplier})
                row = cur.fetchone()
                if not row or row[0] is None:
                    return None

                q1 = float(row[0])
                q3 = float(row[1])
                iqr = float(row[2])
                lower_bound = float(row[3])
                upper_bound = float(row[4])
                outlier_count = int(row[5])
                total_non_null = int(row[6])

                outlier_ratio = (
                    round(outlier_count / total_non_null, 6)
                    if total_non_null > 0
                    else 0.0
                )

                return {
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "outlier_count": outlier_count,
                    "total_non_null": total_non_null,
                    "outlier_ratio": outlier_ratio,
                }

        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] outlier detection timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] outlier detection hatasi: %s", schema, table, column, e)

        return None
