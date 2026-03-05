"""String kolon pattern analizi."""

import logging
from typing import Any, Dict, List, Optional

import psycopg2

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)

# String veri tipleri
STRING_TYPES = {
    "character varying", "varchar", "character", "char", "text",
    "name", "citext", "bpchar",
}


def is_string_type(data_type: str) -> bool:
    """Veri tipinin string olup olmadigini kontrol et."""
    return data_type.lower() in STRING_TYPES


class PatternAnalyzer:
    """Regex tabanli string pattern tespiti."""

    def __init__(self, sql_loader: SqlLoader, patterns: Dict[str, str], max_sample: int):
        self.sql = sql_loader
        self.patterns = patterns
        self.max_sample = max_sample

    def analyze(
        self,
        conn: psycopg2.extensions.connection,
        schema: str,
        table: str,
        column: str,
        row_count: int,
    ) -> Optional[Dict[str, Any]]:
        """
        String kolonda pattern analizi yap.
        Returns: {"patterns": {"email": 0.85, ...}, "dominant_pattern": "email", ...}
        """
        if row_count == 0 or not self.patterns:
            return None

        # Pattern cases SQL parcasi olustur
        pattern_cases = self._build_pattern_cases()
        if not pattern_cases:
            return None

        # SQL'i dogrudan olustur (pattern_cases bir SQL fragment, identifier degil)
        quoted_schema = SqlLoader.validate_identifier(schema)
        quoted_table = SqlLoader.validate_identifier(table)
        quoted_column = SqlLoader.validate_identifier(column)

        sql = f"""
            SELECT
                COUNT(*) AS sample_size,
                {pattern_cases}
            FROM (
                SELECT {quoted_column}::text AS val
                FROM {quoted_schema}.{quoted_table}
                WHERE {quoted_column} IS NOT NULL
                LIMIT %(max_sample)s
            ) sub;
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, {"max_sample": self.max_sample})
                row = cur.fetchone()
                if not row:
                    return None

                sample_size = row[0]
                if sample_size == 0:
                    return None

                # Sonuclari isle
                patterns_result: Dict[str, float] = {}
                col_idx = 1
                for pattern_name in self.patterns:
                    match_count = row[col_idx] or 0
                    ratio = round(match_count / sample_size, 6)
                    if ratio > 0:
                        patterns_result[pattern_name] = ratio
                    col_idx += 1

                # Dominant pattern
                dominant = None
                if patterns_result:
                    dominant = max(patterns_result, key=patterns_result.get)

                # Unclassified ratio
                total_classified = sum(min(v, 1.0) for v in patterns_result.values())
                unclassified = max(0, 1.0 - total_classified)

                return {
                    "patterns": patterns_result,
                    "dominant_pattern": dominant,
                    "unclassified_ratio": round(unclassified, 6),
                    "sample_size": sample_size,
                }

        except psycopg2.errors.QueryCanceled:
            logger.warning("[%s.%s.%s] pattern analysis timeout", schema, table, column)
        except Exception as e:
            logger.warning("[%s.%s.%s] pattern analysis hatasi: %s", schema, table, column, e)

        return None

    def _build_pattern_cases(self) -> str:
        """SQL pattern CASE ifadelerini olustur."""
        cases = []
        for pattern_name, regex in self.patterns.items():
            # Regex'i SQL string olarak escape et
            escaped_regex = regex.replace("'", "''")
            # psycopg2 icin % karakterlerini %% olarak escape et
            escaped_regex = escaped_regex.replace("%", "%%")
            cases.append(
                f"SUM(CASE WHEN val ~ '{escaped_regex}' THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)
