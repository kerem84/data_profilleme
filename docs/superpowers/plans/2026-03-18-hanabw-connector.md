# SAP HANA BW Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SAP HANA BW as the fourth database backend, enabling profiling of DSO active and InfoCube fact tables with BW description enrichment.

**Architecture:** New `HanaBwConnector` class implementing `BaseConnector`, with 9 HANA-dialect SQL templates under `sql/hanabw/`, BW-specific table filtering and description enrichment from SAP dictionary tables (`RSDIOBJT`, `RSDCUBET`). Uses `hdbcli` native driver with `?` positional parameter binding.

**Tech Stack:** Python 3.10+, hdbcli (SAP HANA DB Client), existing profiling pipeline

**Spec:** `docs/superpowers/specs/2026-03-18-hanabw-connector-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/hana_bw_connector.py` | HANA BW connector: connection, schema/table discovery, BW descriptions |
| Create | `sql/hanabw/row_count.sql` | Row count query |
| Create | `sql/hanabw/metadata.sql` | Column metadata with RSDIOBJT description join |
| Create | `sql/hanabw/null_ratio.sql` | NULL/distinct ratio |
| Create | `sql/hanabw/min_max.sql` | Min/max values |
| Create | `sql/hanabw/top_n_values.sql` | Top N frequent values |
| Create | `sql/hanabw/histogram.sql` | Numeric histogram (manual bucketing) |
| Create | `sql/hanabw/numeric_stats.sql` | Numeric statistics with percentiles |
| Create | `sql/hanabw/pattern_analysis.sql` | LIKE_REGEXPR pattern matching |
| Create | `sql/hanabw/outlier_detection.sql` | IQR outlier detection |
| Modify | `src/config_loader.py:10,14-26,104-125` | VALID_DB_TYPES, DatabaseConfig fields, dbname validation |
| Modify | `src/connector_factory.py:7-17` | Add hanabw branch |
| Modify | `src/sql_loader.py:7,47-59` | Identifier regex for `/BIC/` names |
| Modify | `src/profiler.py:28-68,316-322` | ColumnProfile.column_description, metadata execution branch |
| Modify | `src/metrics/distribution.py:11-20,52-59` | HANA numeric types, execution branch |
| Modify | `src/metrics/pattern.py:11-19,41-56,94-106,182-219` | HANA string types, LIKE_REGEXPR patterns |
| Modify | `src/metrics/outlier.py:37-44` | HANA execution branch |
| Modify | `src/report/excel_report.py:178-238` | Description column in Kolon Profil sheet |
| Modify | `src/cli.py:38-39` | CLI description update |
| Modify | `config/config.example.yaml` | HANA BW config example |
| Modify | `requirements.txt` | Add hdbcli |

---

## Task 1: Configuration Foundation

**Files:**
- Modify: `src/config_loader.py`
- Modify: `config/config.example.yaml`
- Modify: `requirements.txt`

- [ ] **Step 1: Add hdbcli to requirements.txt**

Append `hdbcli>=2.18.0` to `requirements.txt`.

- [ ] **Step 2: Update VALID_DB_TYPES and DatabaseConfig**

In `src/config_loader.py`:

Line 10 — add `"hanabw"` to tuple:
```python
VALID_DB_TYPES = ("postgresql", "mssql", "oracle", "hanabw")
```

Lines 14-26 — add two new fields to `DatabaseConfig`:
```python
@dataclass
class DatabaseConfig:
    alias: str
    host: str
    port: int
    dbname: str
    user: str
    password: str
    db_type: str = "postgresql"
    connect_timeout: int = 15
    statement_timeout: int = 300000
    schema_filter: Union[str, List[str]] = "*"
    driver: str = "ODBC Driver 17 for SQL Server"
    service_name: str = ""
    bw_table_filter: List[str] = field(default_factory=lambda: ["/BIC/A", "/BIC/F"])
    bw_description_lang: str = "TR"
```

Note: Need to add `field` to the `dataclasses` import at line 4. Currently the file imports `dataclass` — add `field` next to it.

- [ ] **Step 3: Update config parsing for new fields and dbname relaxation**

In `src/config_loader.py`, `load_config()` function:

Line 105 — relax `dbname` requirement for hanabw:
```python
    for alias, db_data in db_raw.items():
        db_type = db_data.get("db_type", "postgresql")
        if db_type == "hanabw":
            _require_keys(db_data, ["host", "port", "user", "password"], f"databases.{alias}")
            db_data.setdefault("dbname", "")
        else:
            _require_keys(db_data, ["host", "port", "dbname", "user", "password"], f"databases.{alias}")
```

Lines 112-125 — add parsing of new fields:
```python
        databases[alias] = DatabaseConfig(
            alias=alias,
            host=db_data["host"],
            port=int(db_data["port"]),
            dbname=db_data.get("dbname", ""),
            user=db_data["user"],
            password=db_data["password"],
            db_type=db_type,
            connect_timeout=int(db_data.get("connect_timeout", 15)),
            statement_timeout=int(db_data.get("statement_timeout", 300000)),
            schema_filter=db_data.get("schema_filter", "*"),
            driver=db_data.get("driver", "ODBC Driver 17 for SQL Server"),
            service_name=db_data.get("service_name", ""),
            bw_table_filter=db_data.get("bw_table_filter", ["/BIC/A", "/BIC/F"]),
            bw_description_lang=db_data.get("bw_description_lang", "TR"),
        )
```

- [ ] **Step 4: Update config.example.yaml**

Add HANA BW example block after the Oracle example:

```yaml
  # SAP HANA BW ornegi:
  # sap_bw:
  #   db_type: "hanabw"
  #   host: "HOSTNAME"
  #   port: 31015                       # HANA portu (3<instance_no>15)
  #   dbname: ""                        # HANA icin kullanilmaz
  #   user: "USERNAME"
  #   password: "PASSWORD"
  #   connect_timeout: 15
  #   statement_timeout: 300000
  #   schema_filter: "SAPABAP1"         # SAP BW schema
  #   bw_table_filter: ["/BIC/A", "/BIC/F"]  # DSO aktif + InfoCube fact
  #   bw_description_lang: "TR"         # Aciklama dili: TR veya EN
```

- [ ] **Step 5: Commit**

```bash
git add src/config_loader.py config/config.example.yaml requirements.txt
git commit -m "feat(hanabw): config altyapisi - DatabaseConfig, VALID_DB_TYPES, dbname relaxation"
```

---

## Task 2: SqlLoader Identifier Validation Fix

**Files:**
- Modify: `src/sql_loader.py`

**Problem:** SAP BW table names like `/BIC/ATABLENAME` contain `/` characters. The current identifier regex `^[a-zA-Z_][a-zA-Z0-9_]*$` rejects these. HANA requires double-quoting such identifiers: `"/BIC/ATABLENAME"`.

- [ ] **Step 1: Extend identifier validation for HANA BW**

In `src/sql_loader.py`:

Replace the single regex approach with a db_type-aware validation. Add a HANA-compatible regex:

```python
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_HANA_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_/][a-zA-Z0-9_/]*$")
```

Update `validate_identifier` method to accept HANA BW identifiers:

```python
    def validate_identifier(self, name: str) -> str:
        """
        SQL identifier'ini dogrula ve dialect'e gore quote et.
        PostgreSQL/Oracle: "name", MSSQL: [name], HANA: "name" (/ karakterine izin verir)
        """
        if self.db_type == "hanabw":
            if not _HANA_IDENTIFIER_RE.match(name):
                raise ValueError(
                    f"Gecersiz SQL identifier: '{name}'. "
                    "HANA icin harf, rakam, alt cizgi ve / kabul edilir."
                )
        else:
            if not _IDENTIFIER_RE.match(name):
                raise ValueError(
                    f"Gecersiz SQL identifier: '{name}'. "
                    "Sadece harf, rakam ve alt cizgi kabul edilir."
                )
        if self.db_type == "mssql":
            return f"[{name}]"
        return f'"{name}"'
```

- [ ] **Step 2: Commit**

```bash
git add src/sql_loader.py
git commit -m "feat(hanabw): SqlLoader /BIC/ identifier desteği"
```

---

## Task 3: SQL Templates

**Files:**
- Create: `sql/hanabw/` directory with 9 `.sql` files

All templates use `?` positional binding and `"identifier"` double-quote quoting (handled by SqlLoader). Reference the existing MSSQL templates for positional `?` syntax and Oracle templates for PERCENTILE_CONT syntax.

- [ ] **Step 1: Create sql/hanabw/ directory**

```bash
mkdir -p sql/hanabw
```

- [ ] **Step 2: Create row_count.sql**

```sql
SELECT COUNT(*) AS row_count
FROM {schema_name}.{table_name}
```

- [ ] **Step 3: Create metadata.sql**

This is the most complex template — joins HANA system catalog with BW description tables.

```sql
-- HANA BW metadata: kolon bilgisi + BW aciklamalari
-- Value params: ? (schema_name)
SELECT
    c.TABLE_NAME                                          AS table_name,
    c.COLUMN_NAME                                         AS column_name,
    LOWER(c.DATA_TYPE_NAME)                               AS data_type,
    c.LENGTH                                              AS character_maximum_length,
    CASE WHEN c.IS_NULLABLE = 'TRUE' THEN 'YES' ELSE 'NO' END AS is_nullable,
    c.POSITION                                            AS ordinal_position,
    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
    0                                                     AS is_foreign_key,
    pk.CONSTRAINT_NAME                                    AS pk_constraint,
    NULL                                                  AS fk_constraint,
    NULL                                                  AS referenced_schema,
    NULL                                                  AS referenced_table,
    NULL                                                  AS referenced_column,
    dt.TXTLG                                              AS column_description
FROM TABLE_COLUMNS c
LEFT JOIN (
    SELECT cc.SCHEMA_NAME, cc.TABLE_NAME, cc.COLUMN_NAME, cc.CONSTRAINT_NAME
    FROM CONSTRAINTS cc
    WHERE cc.IS_PRIMARY_KEY = 'TRUE'
) pk ON pk.SCHEMA_NAME = c.SCHEMA_NAME
    AND pk.TABLE_NAME = c.TABLE_NAME
    AND pk.COLUMN_NAME = c.COLUMN_NAME
LEFT JOIN RSDIOBJT dt
    ON UPPER(c.COLUMN_NAME) = dt.IOBJNM
    AND dt.OBJVERS = 'A'
    AND dt.LANGU = ?
WHERE c.SCHEMA_NAME = ?
ORDER BY c.TABLE_NAME, c.POSITION
```

Note: The metadata template takes 2 positional params: `[lang_code, schema]`. The profiler execution branch must pass these.

**Important:** `RSDIOBJT` lives in the same schema (SAPABAP1). The join matches column names to InfoObject technical names. `LANGU` uses SAP language codes: `'T'` for Turkish, `'E'` for English. The `bw_description_lang` config maps `"TR"` → `'T'`, `"EN"` → `'E'`.

- [ ] **Step 4: Create null_ratio.sql**

```sql
SELECT
    COUNT(*)                                                    AS total_count,
    COUNT({column_name})                                        AS non_null_count,
    COUNT(*) - COUNT({column_name})                             AS null_count,
    CASE WHEN COUNT(*) > 0
         THEN ROUND(CAST(COUNT(*) - COUNT({column_name}) AS DECIMAL) / COUNT(*), 6)
         ELSE 0 END                                             AS null_ratio,
    COUNT(DISTINCT {column_name})                               AS distinct_count,
    CASE WHEN COUNT(*) > 0
         THEN ROUND(CAST(COUNT(DISTINCT {column_name}) AS DECIMAL) / COUNT(*), 6)
         ELSE 0 END                                             AS distinct_ratio
FROM {schema_name}.{table_name}
```

- [ ] **Step 5: Create min_max.sql**

```sql
SELECT
    CAST(MIN({column_name}) AS NVARCHAR(5000)) AS min_value,
    CAST(MAX({column_name}) AS NVARCHAR(5000)) AS max_value
FROM {schema_name}.{table_name}
WHERE {column_name} IS NOT NULL
```

- [ ] **Step 6: Create top_n_values.sql**

```sql
-- En sik N deger
-- Value params: ? (total_count), ? (top_n)
SELECT
    CAST({column_name} AS NVARCHAR(5000)) AS value,
    COUNT(*) AS frequency,
    ROUND(CAST(COUNT(*) AS DECIMAL) / ?, 6) AS pct
FROM {schema_name}.{table_name}
WHERE {column_name} IS NOT NULL
GROUP BY {column_name}
ORDER BY frequency DESC
LIMIT ?
```

- [ ] **Step 7: Create numeric_stats.sql**

```sql
SELECT
    AVG(CAST({column_name} AS DOUBLE))                                                     AS mean_value,
    STDDEV(CAST({column_name} AS DOUBLE))                                                  AS stddev_value,
    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p01,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p05,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE))            AS p99
FROM {schema_name}.{table_name}
WHERE {column_name} IS NOT NULL
```

- [ ] **Step 8: Create histogram.sql**

```sql
-- Numerik histogram (HANA - manual bucket, no WIDTH_BUCKET)
-- Literal substitution: {buckets}
WITH stats AS (
    SELECT
        MIN(CAST({column_name} AS DOUBLE)) AS min_val,
        MAX(CAST({column_name} AS DOUBLE)) AS max_val
    FROM {schema_name}.{table_name}
    WHERE {column_name} IS NOT NULL
),
bucketed AS (
    SELECT
        CASE
            WHEN s.max_val = s.min_val THEN 1
            ELSE CAST(
                FLOOR(
                    (CAST({column_name} AS DOUBLE) - s.min_val)
                    / NULLIF(s.max_val - s.min_val, 0) * {buckets}
                ) AS INT
            ) + 1
        END AS bucket,
        s.min_val,
        s.max_val
    FROM {schema_name}.{table_name} t, stats s
    WHERE {column_name} IS NOT NULL
)
SELECT
    b.bucket,
    MIN(b.min_val) + (b.bucket - 1) * (MAX(b.max_val) - MIN(b.min_val)) / {buckets} AS lower_bound,
    MIN(b.min_val) + b.bucket * (MAX(b.max_val) - MIN(b.min_val)) / {buckets} AS upper_bound,
    COUNT(*) AS freq
FROM bucketed b
WHERE b.bucket BETWEEN 1 AND {buckets}
GROUP BY b.bucket
ORDER BY b.bucket
```

- [ ] **Step 9: Create pattern_analysis.sql**

```sql
-- HANA pattern analizi (placeholder - gercek SQL, PatternAnalyzer._build_hana_pattern_cases() tarafindan uretilir)
-- Bu dosya kullanilmaz; pattern.py dogrudan SQL uretir (MSSQL/Oracle ile ayni pattern)
SELECT 1
```

Note: Like MSSQL and Oracle, the pattern analysis SQL is dynamically generated by `PatternAnalyzer._build_hana_pattern_cases()` in Python. This file exists for directory completeness but isn't loaded by SqlLoader for pattern analysis.

- [ ] **Step 10: Create outlier_detection.sql**

```sql
-- IQR tabanli outlier tespiti (HANA)
-- Value params: ? (iqr_multiplier x2)
WITH quartiles AS (
    SELECT DISTINCT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE)) OVER() AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST({column_name} AS DOUBLE)) OVER() AS q3
    FROM {schema_name}.{table_name}
    WHERE {column_name} IS NOT NULL
),
bounds AS (
    SELECT q1, q3,
        q3 - q1 AS iqr,
        q1 - (q3 - q1) * ? AS lower_bound,
        q3 + (q3 - q1) * ? AS upper_bound
    FROM quartiles
    LIMIT 1
)
SELECT
    b.q1, b.q3, b.iqr, b.lower_bound, b.upper_bound,
    COUNT(CASE WHEN CAST(t.{column_name} AS DOUBLE) < b.lower_bound
                 OR CAST(t.{column_name} AS DOUBLE) > b.upper_bound
               THEN 1 END) AS outlier_count,
    COUNT(t.{column_name}) AS total_non_null
FROM {schema_name}.{table_name} t, bounds b
WHERE t.{column_name} IS NOT NULL
GROUP BY b.q1, b.q3, b.iqr, b.lower_bound, b.upper_bound
```

- [ ] **Step 11: Commit**

```bash
git add sql/hanabw/
git commit -m "feat(hanabw): 9 HANA SQL template"
```

---

## Task 4: HANA BW Connector

**Files:**
- Create: `src/hana_bw_connector.py`

- [ ] **Step 1: Create the connector class**

Create `src/hana_bw_connector.py` — follows the Oracle connector pattern closely:

```python
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
            # Read-only koruma
            cursor.execute("SET TRANSACTION READ ONLY")
            # Statement timeout (ms)
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

        # BW tablo prefix filtresi uygula
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
```

- [ ] **Step 2: Commit**

```bash
git add src/hana_bw_connector.py
git commit -m "feat(hanabw): HanaBwConnector - baglanti, discovery, BW filtreleme"
```

---

## Task 5: hdbcli Cursor Context Manager Compatibility

**Files:**
- None (verification only)

**Context:** The profiler and metrics modules use `with conn.cursor() as cur:` context manager pattern. hdbcli cursors support `__enter__`/`__exit__` since hdbcli 2.14+. Since we require `hdbcli>=2.18.0` (Task 1), this is safe. However, if at runtime the context manager fails, a wrapper will be needed.

- [ ] **Step 1: Verify hdbcli cursor context manager support**

```bash
python -c "
from hdbcli import dbapi
c = dbapi.Cursor
print('__enter__' in dir(c), '__exit__' in dir(c))
print('hdbcli version:', dbapi.__version__ if hasattr(dbapi, '__version__') else 'unknown')
"
```

If this prints `True True`, no changes needed. If `False`, add a cursor wrapper class in `src/hana_bw_connector.py`:

```python
class _HanaCursorWrapper:
    \"\"\"hdbcli cursor icin context manager desteği.\"\"\"
    def __init__(self, cursor):
        self._cursor = cursor
    def __enter__(self):
        return self._cursor
    def __exit__(self, *args):
        self._cursor.close()
    def __getattr__(self, name):
        return getattr(self._cursor, name)
```

And override `connection()` to monkey-patch `conn.cursor`:

```python
    # Inside connection() context manager, before yield:
    _orig_cursor = conn.cursor
    def _wrapped_cursor():
        return _HanaCursorWrapper(_orig_cursor())
    conn.cursor = _wrapped_cursor
```

- [ ] **Step 2: Commit if wrapper was needed**

```bash
git add src/hana_bw_connector.py
git commit -m "fix(hanabw): hdbcli cursor context manager wrapper"
```

---

## Task 6: Connector Factory Registration

**Files:**
- Modify: `src/connector_factory.py`

- [ ] **Step 1: Add hanabw branch**

Add before the `else` block (after the oracle elif):

```python
    elif config.db_type == "hanabw":
        from src.hana_bw_connector import HanaBwConnector
        return HanaBwConnector(config)
```

- [ ] **Step 2: Commit**

```bash
git add src/connector_factory.py
git commit -m "feat(hanabw): connector factory kaydı"
```

---

## Task 7: Metrics Integration

**Files:**
- Modify: `src/metrics/distribution.py`
- Modify: `src/metrics/pattern.py`
- Modify: `src/metrics/outlier.py`

- [ ] **Step 1: Add HANA numeric types to distribution.py**

In `src/metrics/distribution.py`, update `NUMERIC_TYPES` set (line 11-20):

Add these HANA types (some already exist from other dialects, only add missing ones):
```python
NUMERIC_TYPES = {
    # PostgreSQL
    "smallint", "integer", "bigint", "decimal", "numeric",
    "real", "double precision", "serial", "bigserial",
    "int2", "int4", "int8", "float4", "float8", "money",
    # MSSQL
    "int", "tinyint", "float", "bit", "smallmoney",
    # Oracle
    "number", "binary_float", "binary_double",
    # HANA
    "double", "smalldecimal", "decfloat16", "decfloat34",
}
```

Note: `"smallint"`, `"integer"`, `"bigint"`, `"decimal"`, `"real"`, `"tinyint"` already exist from PG/MSSQL. Only truly new HANA types need adding.

- [ ] **Step 2: Add HANA execution branch in distribution.py get_top_n**

In `src/metrics/distribution.py`, `get_top_n()` method (around line 52-59), add hanabw branch:

```python
                if self.db_type == "mssql":
                    # MSSQL: TOP (?), ? -> top_n, total_count
                    cur.execute(sql, [top_n, row_count])
                elif self.db_type == "oracle":
                    # Oracle: :total_count, :top_n named binds
                    cur.execute(sql, {"total_count": row_count, "top_n": top_n})
                elif self.db_type == "hanabw":
                    # HANA: ? positional -> total_count, top_n
                    cur.execute(sql, [row_count, top_n])
                else:
                    cur.execute(sql, {"total_count": row_count, "top_n": top_n})
```

- [ ] **Step 3: Add HANA string types to pattern.py**

In `src/metrics/pattern.py`, update `STRING_TYPES` set (line 11-19):

```python
STRING_TYPES = {
    # PostgreSQL
    "character varying", "varchar", "character", "char", "text",
    "name", "citext", "bpchar",
    # MSSQL
    "nvarchar", "nchar", "ntext",
    # Oracle
    "varchar2", "nvarchar2", "clob", "nclob", "long",
    # HANA
    "nclob", "shorttext", "alphanum",
}
```

Note: `"nvarchar"`, `"nclob"` already exist. Only `"shorttext"` and `"alphanum"` are HANA-specific.

- [ ] **Step 4: Add HANA LIKE_REGEXPR pattern map to pattern.py**

After `_ORACLE_PATTERN_MAP` (after line 56), add:

```python
# HANA icin LIKE_REGEXPR tabanli pattern karsiliklari
_HANA_PATTERN_MAP = {
    "email": "LIKE_REGEXPR('.+@.+\\..+', val) = 1",
    "phone_tr": (
        "(LIKE_REGEXPR('^\\+90[0-9]{10}$', val) = 1"
        " OR LIKE_REGEXPR('^0[0-9]{10}$', val) = 1"
        " OR (LENGTH(val) = 10 AND LIKE_REGEXPR('^[0-9]+$', val) = 1))"
    ),
    "tc_kimlik": "(LENGTH(val) = 11 AND SUBSTR(val,1,1) != '0' AND LIKE_REGEXPR('^[0-9]+$', val) = 1)",
    "uuid": "LIKE_REGEXPR('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', val) = 1",
    "iso_date": "LIKE_REGEXPR('^[0-9]{4}-[0-9]{2}-[0-9]{2}', val) = 1",
    "iso_datetime": "LIKE_REGEXPR('^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}', val) = 1",
    "url": "(val LIKE 'http://%' OR val LIKE 'https://%')",
    "json_object": "(SUBSTR(val,1,1) = '{' AND SUBSTR(val,LENGTH(val),1) = '}')",
    "numeric_string": "(LIKE_REGEXPR('^[0-9.+-]+$', val) = 1 AND LENGTH(val) > 0)",
}
```

- [ ] **Step 5: Add HANA branch in PatternAnalyzer._build_pattern_cases and analyze**

In `src/metrics/pattern.py`:

Update `_build_pattern_cases()` (line 182-188):
```python
    def _build_pattern_cases(self) -> str:
        if self.db_type == "mssql":
            return self._build_mssql_pattern_cases()
        if self.db_type == "oracle":
            return self._build_oracle_pattern_cases()
        if self.db_type == "hanabw":
            return self._build_hana_pattern_cases()
        return self._build_pg_pattern_cases()
```

Add new method after `_build_oracle_pattern_cases()`:
```python
    def _build_hana_pattern_cases(self) -> str:
        """HANA LIKE_REGEXPR tabanli pattern ifadeleri."""
        cases = []
        for pattern_name in self.patterns:
            expr = _HANA_PATTERN_MAP.get(pattern_name, "1=0")
            cases.append(
                f"SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) AS pattern_{pattern_name}"
            )
        return ",\n                ".join(cases)
```

Update `analyze()` method to add HANA branch (after the Oracle branch around line 94-106):
```python
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
```

- [ ] **Step 6: Add HANA branch in outlier.py**

In `src/metrics/outlier.py`, `detect()` method (around line 37-44):

```python
                if self.db_type == "mssql":
                    cur.execute(sql, [iqr_multiplier, iqr_multiplier])
                elif self.db_type == "oracle":
                    cur.execute(sql, {"iqr_multiplier": iqr_multiplier})
                elif self.db_type == "hanabw":
                    # HANA: ? positional (multiplier x2)
                    cur.execute(sql, [iqr_multiplier, iqr_multiplier])
                else:
                    cur.execute(sql, {"iqr_multiplier": iqr_multiplier})
```

- [ ] **Step 7: Commit**

```bash
git add src/metrics/distribution.py src/metrics/pattern.py src/metrics/outlier.py
git commit -m "feat(hanabw): metrik entegrasyonu - NUMERIC/STRING types, pattern map, execution branch'ler"
```

---

## Task 8: Profiler Integration — ColumnProfile & Metadata

**Files:**
- Modify: `src/profiler.py`

- [ ] **Step 1: Add column_description field to ColumnProfile**

In `src/profiler.py`, `ColumnProfile` dataclass (around line 68, after `dwh_targets`):

```python
    # Description (BW enrichment)
    column_description: str = ""
```

- [ ] **Step 2: Add hanabw metadata execution branch**

In `src/profiler.py`, `_fetch_schema_metadata()` method (around line 316-322):

The HANA metadata template takes 2 positional params: `[sap_lang, schema]`. Need to handle this:

```python
            with conn.cursor() as cur:
                if self.db_config.db_type == "mssql":
                    cur.execute(sql, [schema])
                elif self.db_config.db_type == "hanabw":
                    # HANA metadata: ? (lang_code), ? (schema_name)
                    sap_lang = self.connector.get_sap_lang_code()
                    cur.execute(sql, [sap_lang, schema])
                else:
                    # PostgreSQL %(schema_name)s ve Oracle :schema_name
                    cur.execute(sql, {"schema_name": schema})
```

- [ ] **Step 3: Populate column_description from metadata**

In `src/profiler.py`, `_profile_column()` method (around line 443-456), add description extraction:

After building the initial `ColumnProfile`, add:
```python
        col_prof = ColumnProfile(
            column_name=col_name,
            ordinal_position=col_meta.get("ordinal_position", 0),
            data_type=data_type,
            max_length=col_meta.get("character_maximum_length"),
            is_nullable=col_meta.get("is_nullable", "YES"),
            is_primary_key=bool(col_meta.get("is_primary_key", False)),
            is_foreign_key=bool(col_meta.get("is_foreign_key", False)),
            pk_constraint=col_meta.get("pk_constraint"),
            fk_constraint=col_meta.get("fk_constraint"),
            referenced_schema=col_meta.get("referenced_schema"),
            referenced_table=col_meta.get("referenced_table"),
            referenced_column=col_meta.get("referenced_column"),
            column_description=col_meta.get("column_description") or "",
        )
```

The `column_description` key comes from the metadata SQL template's `column_description` alias. For non-HANA databases, this key won't exist in `col_meta`, so `.get("column_description") or ""` returns empty string.

- [ ] **Step 4: Commit**

```bash
git add src/profiler.py
git commit -m "feat(hanabw): ColumnProfile.column_description + metadata execution branch"
```

---

## Task 9: Excel Report — Description Column

**Files:**
- Modify: `src/report/excel_report.py`

- [ ] **Step 1: Add Aciklama column to Kolon Profil sheet**

In `src/report/excel_report.py`, `_write_column_profile()` method:

Update headers list (around line 181-188) — insert "Aciklama" after "Kolon":
```python
        headers = [
            "Sema", "Tablo", "Kolon", "Aciklama", "Sira", "Veri Tipi", "Max Uzunluk",
            "Nullable", "PK", "FK",
            "NULL Sayisi", "NULL Orani", "Distinct Sayisi", "Distinct Orani",
            "Min", "Max",
            "Ortalama", "Std Sapma", "P25", "P50", "P75",
            "Kalite Skoru", "Kalite Notu", "Kalite Bayraklari",
        ]
```

Replace the entire column write block (the `for col in table.columns:` loop body) with these column assignments:

```python
                    ws.cell(row=row_idx, column=1, value=table.schema_name)
                    ws.cell(row=row_idx, column=2, value=table.table_name)
                    ws.cell(row=row_idx, column=3, value=col.column_name)
                    ws.cell(row=row_idx, column=4, value=col.column_description)
                    ws.cell(row=row_idx, column=5, value=col.ordinal_position)
                    ws.cell(row=row_idx, column=6, value=col.data_type)
                    ws.cell(row=row_idx, column=7, value=col.max_length or "")
                    ws.cell(row=row_idx, column=8, value=col.is_nullable)
                    ws.cell(row=row_idx, column=9, value="PK" if col.is_primary_key else "")
                    ws.cell(row=row_idx, column=10, value="FK" if col.is_foreign_key else "")
                    ws.cell(row=row_idx, column=11, value=col.null_count)
                    ws.cell(row=row_idx, column=12, value=col.null_ratio)
                    ws.cell(row=row_idx, column=13, value=col.distinct_count)
                    ws.cell(row=row_idx, column=14, value=col.distinct_ratio)
                    ws.cell(row=row_idx, column=15, value=col.min_value or "")
                    ws.cell(row=row_idx, column=16, value=col.max_value or "")
                    ws.cell(row=row_idx, column=17, value=col.mean or "")
                    ws.cell(row=row_idx, column=18, value=col.stddev or "")

                    p25 = col.percentiles.get("p25", "") if col.percentiles else ""
                    p50 = col.percentiles.get("p50", "") if col.percentiles else ""
                    p75 = col.percentiles.get("p75", "") if col.percentiles else ""
                    ws.cell(row=row_idx, column=19, value=p25)
                    ws.cell(row=row_idx, column=20, value=p50)
                    ws.cell(row=row_idx, column=21, value=p75)

                    ws.cell(row=row_idx, column=22, value=round(col.quality_score, 4))
                    grade_cell = ws.cell(row=row_idx, column=23, value=col.quality_grade)
                    grade_cell.fill = GRADE_FILLS.get(col.quality_grade, GRADE_FILLS["F"])
                    ws.cell(row=row_idx, column=24, value=", ".join(col.quality_flags))

                    # PK/FK row renklendirme
                    if col.is_primary_key:
                        for c in range(1, 25):
                            ws.cell(row=row_idx, column=c).fill = PK_FILL
                    elif col.is_foreign_key:
                        for c in range(1, 25):
                            ws.cell(row=row_idx, column=c).fill = FK_FILL

                    for c in range(1, 25):
                        ws.cell(row=row_idx, column=c).border = THIN_BORDER

                    row_idx += 1
```

Key changes from original: column 4 = description (new), all subsequent columns shifted +1, range goes to 25 (was 24).

- [ ] **Step 2: Commit**

```bash
git add src/report/excel_report.py
git commit -m "feat(hanabw): Excel Kolon Profil - Aciklama sutunu"
```

---

## Task 10: CLI and Documentation Updates

**Files:**
- Modify: `src/cli.py`

- [ ] **Step 1: Update CLI description**

In `src/cli.py`, line 38-39:
```python
    parser = argparse.ArgumentParser(
        description="Kaynak Tablo Profilleme Araci (PostgreSQL / MSSQL / Oracle / HANA BW)",
```

- [ ] **Step 2: Commit all**

```bash
git add src/cli.py
git commit -m "feat(hanabw): CLI aciklamasi guncelleme"
```

---

## Task 11: Final Verification

- [ ] **Step 1: Verify all files are consistent**

Run a quick syntax check:
```bash
python -c "from src.config_loader import load_config; print('config_loader OK')"
python -c "from src.hana_bw_connector import HanaBwConnector; print('connector OK')"
python -c "from src.connector_factory import create_connector; print('factory OK')"
python -c "from src.profiler import Profiler, ColumnProfile; print('profiler OK')"
python -c "from src.sql_loader import SqlLoader; s = SqlLoader('sql', 'hanabw'); print('sql_loader OK')"
python -c "from src.metrics.distribution import NUMERIC_TYPES; print(f'NUMERIC_TYPES: {len(NUMERIC_TYPES)} types')"
python -c "from src.metrics.pattern import STRING_TYPES; print(f'STRING_TYPES: {len(STRING_TYPES)} types')"
```

Each should print OK without errors.

- [ ] **Step 2: Verify SQL templates load**

```bash
python -c "
from src.sql_loader import SqlLoader
sql = SqlLoader('sql', 'hanabw')
for t in ['row_count','metadata','null_ratio','min_max','top_n_values','histogram','numeric_stats','pattern_analysis','outlier_detection']:
    sql.load(t, schema_name='SAPABAP1', table_name='/BIC/ATESTDSO', column_name='CALMONTH')
    print(f'{t}: OK')
"
```

- [ ] **Step 3: Test dry-run with example config** (requires HANA access)

If HANA is reachable:
```bash
python -m src.cli --config config/config.yaml --db sap_bw --dry-run
```

- [ ] **Step 4: Final commit**

```bash
git add -A
git status
# If any remaining changes:
git commit -m "feat(hanabw): final verification cleanup"
```

---

## Dependency Graph

```
Task 1 (Config) ─┬─> Task 2 (SqlLoader) ─┬─> Task 3 (SQL Templates)
                  │                        │
                  └─> Task 4 (Connector) ──┤
                                           │
                  Task 5 (hdbcli check) ───┤
                                           │
                  Task 6 (Factory) ────────┤
                                           │
                  Task 7 (Metrics) ────────┤
                                           │
                  Task 8 (Profiler) ───────┤
                                           │
                  Task 9 (Excel) ──────────┤
                                           │
                  Task 10 (CLI) ───────────┘
                                           │
                  Task 11 (Verification) ──┘
```

Tasks 1-2 must be done first (foundation). Task 5 must be done after Task 4. Tasks 3-10 can be done in listed order after dependencies. Task 11 is last.
