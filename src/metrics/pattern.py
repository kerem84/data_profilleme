"""String kolon pattern analizi."""

import logging
from typing import Any, Dict, Optional

from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)

# String veri tipleri (PostgreSQL + MSSQL + Oracle + HANA)
STRING_TYPES = {
    # PostgreSQL
    "character varying", "varchar", "character", "char", "text",
    "name", "citext", "bpchar",
    # MSSQL
    "nvarchar", "nchar", "ntext",
    # Oracle
    "varchar2", "nvarchar2", "clob", "nclob", "long",
    # HANA
    "shorttext", "alphanum",
}

# MSSQL icin bilinen pattern'lerin LIKE/PATINDEX karsiliklari
_MSSQL_PATTERN_MAP = {
    "email": "PATINDEX('_%@_%._%', val) > 0",
    "phone_tr": (
        "(val LIKE '+90[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
        " OR val LIKE '0[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
        " OR (LEN(val) = 10 AND PATINDEX('%[^0-9]%', val) = 0))"
    ),
    "tc_kimlik": "(LEN(val) = 11 AND LEFT(val, 1) <> '0' AND PATINDEX('%[^0-9]%', val) = 0)",
    "uuid": (
        "(LEN(val) = 36 AND SUBSTRING(val,9,1) = '-' AND SUBSTRING(val,14,1) = '-'"
        " AND SUBSTRING(val,19,1) = '-' AND SUBSTRING(val,24,1) = '-')"
    ),
    "iso_date": "(LEN(val) >= 10 AND PATINDEX('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%', val) = 1)",
    "iso_datetime": "(LEN(val) >= 16 AND PATINDEX('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][T ][0-9][0-9]:[0-9][0-9]%', val) = 1)",
    "url": "(val LIKE 'http://%' OR val LIKE 'https://%')",
    "json_object": "(LEFT(val, 1) = '{' AND RIGHT(val, 1) = '}')",
    "numeric_string": "(PATINDEX('%[^0-9.+-]%', val) = 0 AND LEN(val) > 0)",
}

# Oracle icin REGEXP_LIKE tabanli pattern karsiliklari
_ORACLE_PATTERN_MAP = {
    "email": "REGEXP_LIKE(val, '.+@.+\\..+')",
    "phone_tr": (
        "(REGEXP_LIKE(val, '^\\+90[0-9]{10}$')"
        " OR REGEXP_LIKE(val, '^0[0-9]{10}$')"
        " OR (LENGTH(val) = 10 AND REGEXP_LIKE(val, '^[0-9]+$')))"
    ),
    "tc_kimlik": "(LENGTH(val) = 11 AND SUBSTR(val,1,1) != '0' AND REGEXP_LIKE(val, '^[0-9]+$'))",
    "uuid": "REGEXP_LIKE(val, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')",
    "iso_date": "REGEXP_LIKE(val, '^[0-9]{4}-[0-9]{2}-[0-9]{2}')",
    "iso_datetime": "REGEXP_LIKE(val, '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}')",
    "url": "(val LIKE 'http://%' OR val LIKE 'https://%')",
    "json_object": "(SUBSTR(val,1,1) = '{' AND SUBSTR(val,-1) = '}')",
    "numeric_string": "(REGEXP_LIKE(val, '^[0-9.+-]+$') AND LENGTH(val) > 0)",
}

# HANA icin LIKE_REGEXPR tabanli pattern karsiliklari
# LIKE_REGEXPR bir operator: val LIKE_REGEXPR 'pattern' (fonksiyon degil!)
_HANA_PATTERN_MAP = {
    "email": "val LIKE_REGEXPR '.+@.+\\..+'",
    "phone_tr": (
        "(val LIKE_REGEXPR '^\\+90[0-9]{10}$'"
        " OR val LIKE_REGEXPR '^0[0-9]{10}$'"
        " OR (LENGTH(val) = 10 AND val LIKE_REGEXPR '^[0-9]+$'))"
    ),
    "tc_kimlik": "(LENGTH(val) = 11 AND SUBSTR(val,1,1) != '0' AND val LIKE_REGEXPR '^[0-9]+$')",
    "uuid": "val LIKE_REGEXPR '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'",
    "iso_date": "val LIKE_REGEXPR '^[0-9]{4}-[0-9]{2}-[0-9]{2}'",
    "iso_datetime": "val LIKE_REGEXPR '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}'",
    "url": "(val LIKE 'http://%' OR val LIKE 'https://%')",
    "json_object": "(SUBSTR(val,1,1) = '{' AND SUBSTR(val,LENGTH(val),1) = '}')",
    "numeric_string": "(val LIKE_REGEXPR '^[0-9.+-]+$' AND LENGTH(val) > 0)",
}


def is_string_type(data_type: str) -> bool:
    """Veri tipinin string olup olmadigini kontrol et."""
    return data_type.lower() in STRING_TYPES


class PatternAnalyzer:
    """Regex/PATINDEX tabanli string pattern tespiti."""

    def __init__(
        self, sql_loader: SqlLoader, patterns: Dict[str, str],
        max_sample: int, db_type: str = "postgresql",
    ):
        self.sql = sql_loader
        self.patterns = patterns
        self.max_sample = max_sample
        self.db_type = db_type

    def analyze(
        self, conn, schema: str, table: str, column: str, row_count: int,
    ) -> Optional[Dict[str, Any]]:
        """
        String kolonda pattern analizi yap.
        Returns: {"patterns": {"email": 0.85, ...}, "dominant_pattern": "email", ...}
        """
        if row_count == 0 or not self.patterns:
            return None

        pattern_cases = self._build_pattern_cases()
        if not pattern_cases:
            return None

        quoted_schema = self.sql.validate_identifier(schema)
        quoted_table = self.sql.validate_identifier(table)
        quoted_column = self.sql.validate_identifier(column)

        if self.db_type == "oracle":
            sql = f"""
                SELECT
                    COUNT(*) AS sample_size,
                    {pattern_cases}
                FROM (
                    SELECT CAST({quoted_column} AS VARCHAR2(4000)) AS val
                    FROM {quoted_schema}.{quoted_table}
                    WHERE {quoted_column} IS NOT NULL
                    FETCH FIRST {self.max_sample} ROWS ONLY
                ) sub
            """
            params = None
        elif self.db_type == "mssql":
            sql = f"""
                SELECT
                    COUNT(*) AS sample_size,
                    {pattern_cases}
                FROM (
                    SELECT TOP ({self.max_sample})
                        CAST({quoted_column} AS NVARCHAR(MAX)) AS val
                    FROM {quoted_schema}.{quoted_table}
                    WHERE {quoted_column} IS NOT NULL
                ) sub;
            """
            params = None
        elif self.db_type == "hanabw":
            sql = f"""
                SELECT
                    COUNT(*) AS sample_size,
                    {pattern_cases}
                FROM (
                    SELECT CAST({quoted_column} AS NVARCHAR(5000)) AS val
                    FROM {quoted_schema}.{quoted_table}
                    WHERE {quoted_column} IS NOT NULL
                    LIMIT {self.max_sample}
                ) sub
            """
            params = None
        else:
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
            params = {"max_sample": self.max_sample}

        try:
            cur = conn.cursor()
            try:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                row = cur.fetchone()
            finally:
                cur.close()

            if not row:
                return None

            sample_size = row[0]
            if sample_size == 0:
                return None

            patterns_result: Dict[str, float] = {}
            col_idx = 1
            for pattern_name in self.patterns:
                match_count = row[col_idx] or 0
                ratio = round(match_count / sample_size, 6)
                if ratio > 0:
                    patterns_result[pattern_name] = ratio
                col_idx += 1

            dominant = None
            if patterns_result:
                dominant = max(patterns_result, key=patterns_result.get)

            total_classified = sum(min(v, 1.0) for v in patterns_result.values())
            unclassified = max(0, 1.0 - total_classified)

            return {
                "patterns": patterns_result,
                "dominant_pattern": dominant,
                "unclassified_ratio": round(unclassified, 6),
                "sample_size": sample_size,
            }

        except Exception as e:
            logger.warning(
                "[%s.%s.%s] pattern analysis hatasi: %s", schema, table, column, e
            )

        return None

    def _build_pattern_cases(self) -> str:
        """SQL pattern CASE ifadelerini olustur."""
        if self.db_type == "mssql":
            return self._build_mssql_pattern_cases()
        if self.db_type == "oracle":
            return self._build_oracle_pattern_cases()
        if self.db_type == "hanabw":
            return self._build_hana_pattern_cases()
        return self._build_pg_pattern_cases()

    def _build_pg_pattern_cases(self) -> str:
        """PostgreSQL regex (~) tabanli pattern ifadeleri."""
        cases = []
        for pattern_name, regex in self.patterns.items():
            escaped_regex = regex.replace("'", "''")
            escaped_regex = escaped_regex.replace("%", "%%")
            cases.append(
                f"SUM(CASE WHEN val ~ '{escaped_regex}' THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)

    def _build_mssql_pattern_cases(self) -> str:
        """MSSQL LIKE/PATINDEX tabanli pattern ifadeleri."""
        cases = []
        for pattern_name in self.patterns:
            expr = _MSSQL_PATTERN_MAP.get(pattern_name, "1=0")
            cases.append(
                f"SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)

    def _build_oracle_pattern_cases(self) -> str:
        """Oracle REGEXP_LIKE tabanli pattern ifadeleri."""
        cases = []
        for pattern_name in self.patterns:
            expr = _ORACLE_PATTERN_MAP.get(pattern_name, "1=0")
            cases.append(
                f"SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)

    def _build_hana_pattern_cases(self) -> str:
        """HANA LIKE_REGEXPR tabanli pattern ifadeleri."""
        cases = []
        for pattern_name in self.patterns:
            expr = _HANA_PATTERN_MAP.get(pattern_name, "1=0")
            cases.append(
                f"SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)
