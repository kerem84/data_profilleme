"""YAML konfigurasyonu yukleyici ve dogrulayici."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import yaml


VALID_DB_TYPES = ("postgresql", "mssql", "oracle", "hanabw")


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


@dataclass
class ProfilingConfig:
    top_n_values: int = 20
    sample_threshold: int = 5_000_000
    sample_percent: int = 10
    numeric_percentiles: List[float] = field(
        default_factory=lambda: [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]
    )
    max_pattern_sample: int = 100_000
    outlier_iqr_multiplier: float = 1.5
    quality_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "completeness": 0.35,
            "uniqueness": 0.20,
            "consistency": 0.25,
            "validity": 0.20,
        }
    )
    string_patterns: Dict[str, str] = field(default_factory=dict)


@dataclass
class ReportingConfig:
    excel_enabled: bool = True
    excel_filename_template: str = "profil_{db_alias}_{timestamp}.xlsx"
    html_enabled: bool = True
    html_filename_template: str = "profil_{db_alias}_{timestamp}.html"
    embed_assets: bool = True
    combined_report: bool = True


@dataclass
class MappingConfig:
    enabled: bool = False
    mapping_file: str = ""


@dataclass
class AppConfig:
    project_name: str
    output_dir: str
    databases: Dict[str, DatabaseConfig]
    profiling: ProfilingConfig
    mapping: MappingConfig
    reporting: ReportingConfig
    log_level: str = "INFO"
    log_file: str = "./output/profil.log"


class ConfigError(Exception):
    pass


def load_config(path: str) -> AppConfig:
    """YAML config dosyasini yukle ve dogrula."""
    if not os.path.exists(path):
        raise ConfigError(f"Config dosyasi bulunamadi: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or not isinstance(raw, dict):
        raise ConfigError("Config dosyasi bos veya gecersiz.")

    # Project
    project = raw.get("project", {})
    project_name = project.get("name", "Profilleme")
    output_dir = project.get("output_dir", "./output")

    # Databases
    db_raw = raw.get("databases", {})
    if not db_raw:
        raise ConfigError("En az bir veritabani tanimlanmali (databases bolumu).")

    databases: Dict[str, DatabaseConfig] = {}
    for alias, db_data in db_raw.items():
        db_type = db_data.get("db_type", "postgresql")
        if db_type == "hanabw":
            _require_keys(db_data, ["host", "port", "user", "password"], f"databases.{alias}")
            dbname = db_data.get("dbname", "")
        else:
            _require_keys(db_data, ["host", "port", "dbname", "user", "password"], f"databases.{alias}")
            dbname = db_data["dbname"]
        if db_type not in VALID_DB_TYPES:
            raise ConfigError(
                f"Gecersiz db_type: '{db_type}' (databases.{alias}). "
                f"Gecerli degerler: {', '.join(VALID_DB_TYPES)}"
            )
        databases[alias] = DatabaseConfig(
            alias=alias,
            host=db_data["host"],
            port=int(db_data["port"]),
            dbname=dbname,
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

    # Profiling
    prof_raw = raw.get("profiling", {})
    profiling = ProfilingConfig(
        top_n_values=int(prof_raw.get("top_n_values", 20)),
        sample_threshold=int(prof_raw.get("sample_threshold", 5_000_000)),
        sample_percent=int(prof_raw.get("sample_percent", 10)),
        numeric_percentiles=prof_raw.get(
            "numeric_percentiles", [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]
        ),
        max_pattern_sample=int(prof_raw.get("max_pattern_sample", 100_000)),
        outlier_iqr_multiplier=float(prof_raw.get("outlier_iqr_multiplier", 1.5)),
        quality_weights=prof_raw.get(
            "quality_weights",
            {"completeness": 0.35, "uniqueness": 0.20, "consistency": 0.25, "validity": 0.20},
        ),
        string_patterns=prof_raw.get("string_patterns", {}),
    )

    # Mapping
    map_raw = raw.get("mapping", {})
    mapping = MappingConfig(
        enabled=bool(map_raw.get("enabled", False)),
        mapping_file=map_raw.get("mapping_file", ""),
    )

    # Reporting
    rep_raw = raw.get("reporting", {})
    excel_raw = rep_raw.get("excel", {})
    html_raw = rep_raw.get("html", {})
    reporting = ReportingConfig(
        excel_enabled=bool(excel_raw.get("enabled", True)),
        excel_filename_template=excel_raw.get("filename_template", "profil_{db_alias}_{timestamp}.xlsx"),
        html_enabled=bool(html_raw.get("enabled", True)),
        html_filename_template=html_raw.get("filename_template", "profil_{db_alias}_{timestamp}.html"),
        embed_assets=bool(html_raw.get("embed_assets", True)),
        combined_report=bool(rep_raw.get("combined_report", True)),
    )

    # Logging
    log_raw = raw.get("logging", {})

    return AppConfig(
        project_name=project_name,
        output_dir=output_dir,
        databases=databases,
        profiling=profiling,
        mapping=mapping,
        reporting=reporting,
        log_level=log_raw.get("level", "INFO"),
        log_file=log_raw.get("file", "./output/profil.log"),
    )


def _require_keys(data: Dict[str, Any], keys: List[str], section: str) -> None:
    for key in keys:
        if key not in data:
            raise ConfigError(f"Zorunlu alan eksik: {section}.{key}")
