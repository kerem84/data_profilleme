# SAP HANA BW Connector Design

**Date:** 2026-03-18
**Branch:** feat/oracle-support (extend with HANA BW support)
**Status:** Approved

## Overview

Add SAP HANA BW as the fourth database backend to the yolcu_profil data profiling tool. The connector targets classic SAP BW on HANA systems, profiling DSO active tables (`/BIC/A*`) and InfoCube fact tables (`/BIC/F*`) with BW description enrichment from SAP dictionary tables.

## Approach

**Yaklaşım A: Tam Bağımsız Connector** — Same pattern as PostgreSQL, MSSQL, Oracle. New connector class + SQL templates + factory registration. No inheritance from existing connectors.

## Architecture

### 1. Connector: `src/hana_bw_connector.py`

Implements `BaseConnector` abstract interface.

**Connection:**
- Library: `hdbcli.dbapi` (SAP HANA native Python driver)
- Connection: `dbapi.connect(address=host, port=port, user=user, password=password)`
- Read-only protection: `SET TRANSACTION READ ONLY` after connect
- Statement timeout: `cursor.execute("SET 'statement_timeout' = '<ms>'")`
- Timeout exception: `hdbcli.dbapi.Error`

**Schema discovery:**
- Source: `SYS.SCHEMAS` view
- Default filter: `SAPABAP1` (configurable via `schema_filter`)
- System schema exclusion: `SYS`, `_SYS_*`, `SYSTEM`

**Table discovery:**
- Source: `TABLES` system view
- BW table filtering: prefix-based (`/BIC/A*` for DSO active, `/BIC/F*` for InfoCube fact)
- Configurable via `bw_table_filter` config field
- Row count estimate: `TABLES.RECORD_COUNT` or `M_TABLES`

**BW Description Enrichment:**
- `RSDIOBJT` — InfoObject descriptions (`TXTLG`, `LANGU = 'T'` for Turkish, `'E'` fallback)
- `RSDCUBET` — InfoCube descriptions
- `RSZELTTXT` — Query/element descriptions
- Graceful fallback: if description not found, technical name only

**Parameter binding:** `?` positional throughout all SQL templates and connector internal queries. hdbcli supports both positional `?` and named `:param`, but we use positional `?` consistently (same as MSSQL connector) for uniformity.

**Identifier quoting:** `"identifier"` (double quotes, HANA standard SQL)

### 2. SQL Templates: `sql/hanabw/`

9 SQL templates adapted to HANA SQL dialect:

| Template | HANA Notes |
|----------|-----------|
| `row_count.sql` | Standard `COUNT(*)` |
| `metadata.sql` | `TABLE_COLUMNS` + `CONSTRAINTS` system views, joined with `RSDIOBJT` for BW descriptions |
| `null_ratio.sql` | `CAST(... AS DECIMAL)` |
| `min_max.sql` | `CAST(... AS NVARCHAR(5000))` |
| `top_n_values.sql` | `LIMIT ?` (HANA native) |
| `histogram.sql` | `CASE WHEN` manual bucketing (no WIDTH_BUCKET in HANA) |
| `numeric_stats.sql` | `PERCENTILE_CONT` / `PERCENTILE_DISC` (HANA supports) |
| `pattern_analysis.sql` | `LIKE_REGEXPR(?, {column_name}) = 1` (positional binding) |
| `outlier_detection.sql` | Window function + `PERCENTILE_CONT` for quartiles |

### 3. Configuration

**New `config.yaml` block:**
```yaml
databases:
  sap_bw:
    db_type: "hanabw"
    host: "172.24.243.100"
    port: 31015
    dbname: ""                         # Not used for HANA (host+port sufficient), but required by config schema
    user: "jdbc_read_user"
    password: "***"
    schema_filter: "SAPABAP1"
    bw_table_filter: ["/BIC/A", "/BIC/F"]   # Full prefix including /BIC/
    bw_description_lang: "TR"
```

**Config validation for `dbname`:** HANA connections don't use a database name (tenant is determined by port). The `dbname` field remains in config for schema compatibility but is ignored by the HANA connector. Config validation must allow empty string for `hanabw` db_type.

**New DatabaseConfig fields:**
- `bw_table_filter: List[str]` — BW table prefix filters including full path (default: `["/BIC/A", "/BIC/F"]`). Tables whose names start with any of these prefixes will be included.
- `bw_description_lang: str` — SAP language code for descriptions: `"TR"` for Turkish (LANGU='T'), `"EN"` for English (LANGU='E'). Default: `"TR"`.

These fields are only relevant when `db_type = "hanabw"`.

### 4. Integration Points

**Connector Factory (`src/connector_factory.py`):**
```python
elif config.db_type == "hanabw":
    from src.hana_bw_connector import HanaBwConnector
    return HanaBwConnector(config)
```

**Config Validation (`src/config_loader.py`):**
- Add `"hanabw"` to `VALID_DB_TYPES`
- Parse `bw_table_filter` and `bw_description_lang` from YAML

**Metrics Compatibility:**

`distribution.py` NUMERIC_TYPES additions:
- `"TINYINT"`, `"SMALLINT"`, `"INTEGER"`, `"BIGINT"`, `"DECIMAL"`, `"REAL"`, `"DOUBLE"`, `"SMALLDECIMAL"`

`pattern.py` HANA dialect:
- Uses `LIKE_REGEXPR(?, {column_name}) = 1` syntax (positional binding, consistent with all templates)
- New branch alongside PostgreSQL (`~`), MSSQL (`PATINDEX`), Oracle (`REGEXP_LIKE`)

### 5. Data Model Change

**ColumnProfile addition:**
```python
column_description: str = ""  # BW description (populated for hanabw, empty for others)
```

**Excel/HTML reports:**
- New "Aciklama" (Description) column in column detail sheets
- Populated from `RSDIOBJT` for HANA BW, empty for other database types
- Column always present, just empty when no description available

### 6. CLI

- `--db-type` choices: add `hanabw`
- Existing `--schema`, `--table` filters work as-is

### 7. Dependencies

- `hdbcli` added to `requirements.txt`
- No other new dependencies

## Existing Code from `sap_bw/` Directory

The `sap_bw/` directory contains working exploration scripts that connect to the target HANA system (`172.24.243.100:31015`, schema `SAPABAP1`). Key patterns to reuse:

- **Connection pattern:** `hdbcli.dbapi.connect(address=, port=, user=, password=)`
- **Catalog queries:** `TABLE_COLUMNS`, `TABLES`, `SYS.SCHEMAS`
- **BW description joins:** `RSDIOBJT`, `RSDCUBET`, `RSZELTTXT` with `OBJVERS = 'A'` and `LANGU = 'T'`
- **BW stat tables:** `RSDDSTATHEADER`, `RSDDSTATINFO` (for future BW usage analytics)
- **SQL syntax confirmed working:** `TOP N`, `LIMIT`, `TO_NVARCHAR`, `ADD_DAYS`, `LIKE_REGEXPR`

## Scope Exclusions

- No BW query usage analytics (the `bw_most_used_reports.sql` queries are separate tooling, not part of profiling)
- No SID tables, change logs, or dimension tables
- No master data tables (`/BI0/P*`, `/BIC/P*`) — only DSO active + InfoCube fact
- No SAP RFC integration
- No SSL/TLS certificate support (standard connection only)

## Error Handling

Follows existing project pattern (fault-tolerant):

| Scenario | Behavior |
|----------|----------|
| HANA connection failed | Log error, skip database |
| BW description table not accessible | Fallback to technical names only |
| Query timeout | Use `TABLES.RECORD_COUNT` estimate |
| Schema not found | Log warning, skip |
| Empty table (0 rows) | Quality = N/A |
