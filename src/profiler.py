"""Ana profilleme orkestratoru."""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from tqdm import tqdm

from src.config_loader import AppConfig
from src.connector_factory import create_connector
from src.metrics.basic import BasicMetrics
from src.metrics.distribution import DistributionMetrics, is_numeric_type
from src.metrics.pattern import PatternAnalyzer, is_string_type
from src.metrics.outlier import OutlierDetector
from src.metrics.quality import QualityScorer
from src.sql_loader import SqlLoader

logger = logging.getLogger(__name__)


# ---------- Data classes ----------

@dataclass
class ColumnProfile:
    column_name: str
    ordinal_position: int
    data_type: str
    max_length: Optional[int]
    is_nullable: str
    is_primary_key: bool
    is_foreign_key: bool
    pk_constraint: Optional[str] = None
    fk_constraint: Optional[str] = None
    referenced_schema: Optional[str] = None
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None
    # Basic
    null_count: int = 0
    null_ratio: float = 0.0
    distinct_count: int = 0
    distinct_ratio: float = 0.0
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    # Numeric
    mean: Optional[float] = None
    stddev: Optional[float] = None
    percentiles: Optional[Dict[str, float]] = None
    # Distribution
    top_n_values: List[Dict[str, Any]] = field(default_factory=list)
    histogram: Optional[List[Dict]] = None
    # Pattern
    detected_patterns: Optional[Dict[str, float]] = None
    dominant_pattern: Optional[str] = None
    # Outlier
    outlier_count: Optional[int] = None
    outlier_ratio: Optional[float] = None
    outlier_bounds: Optional[Dict[str, float]] = None
    # Quality
    quality_score: float = 0.0
    quality_grade: str = "F"
    quality_flags: List[str] = field(default_factory=list)
    # Mapping
    dwh_mapped: bool = False
    dwh_targets: List[str] = field(default_factory=list)


@dataclass
class TableProfile:
    schema_name: str
    table_name: str
    table_type: str
    row_count: int
    estimated_rows: int
    row_count_estimated: bool
    table_size_bytes: Optional[int] = None
    table_size_display: str = ""
    column_count: int = 0
    columns: List[ColumnProfile] = field(default_factory=list)
    profiled_at: str = ""
    profile_duration_sec: float = 0.0
    sampled: bool = False
    sample_percent: Optional[int] = None
    table_quality_score: float = 0.0
    table_quality_grade: str = "F"
    dwh_mapped: bool = False
    dwh_target_tables: List[str] = field(default_factory=list)


@dataclass
class SchemaProfile:
    schema_name: str
    table_count: int = 0
    total_rows: int = 0
    total_size_bytes: Optional[int] = None
    total_size_display: str = ""
    tables: List[TableProfile] = field(default_factory=list)
    schema_quality_score: float = 0.0


@dataclass
class DatabaseProfile:
    db_alias: str
    db_name: str
    host: str
    profiled_at: str = ""
    total_schemas: int = 0
    total_tables: int = 0
    total_columns: int = 0
    total_rows: int = 0
    total_size_bytes: Optional[int] = None
    total_size_display: str = ""
    schemas: List[SchemaProfile] = field(default_factory=list)
    overall_quality_score: float = 0.0


# ---------- Profiler ----------

class Profiler:
    """Veritabani profilleme orkestratoru."""

    def __init__(self, config: AppConfig, db_key: str):
        self.config = config
        self.db_config = config.databases[db_key]
        self.connector = create_connector(self.db_config)
        self.sql = SqlLoader(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql"),
            db_type=self.db_config.db_type,
        )
        self.basic = BasicMetrics(self.sql, self.connector)
        self.distribution = DistributionMetrics(self.sql, self.connector)
        self.pattern = PatternAnalyzer(
            self.sql,
            config.profiling.string_patterns,
            config.profiling.max_pattern_sample,
            db_type=self.db_config.db_type,
        )
        self.outlier = OutlierDetector(self.sql, self.connector)
        self.quality = QualityScorer(config.profiling.quality_weights)
        self.prof_config = config.profiling

    def profile_database(
        self,
        resumed_profile: Optional[DatabaseProfile] = None,
        checkpoint_dir: Optional[str] = None,
    ) -> DatabaseProfile:
        """Tum veritabanini profille. Resume ve checkpoint destegi."""
        db_profile = DatabaseProfile(
            db_alias=self.db_config.alias,
            db_name=self.db_config.dbname,
            host=self.db_config.host,
            profiled_at=datetime.now().isoformat(),
        )

        if not self.connector.test_connection():
            logger.error("[%s] Baglanti kurulamadi, profilleme iptal.", self.db_config.alias)
            return db_profile

        # DB tipi dogrulama
        with self.connector.connection() as conn:
            if not self.connector.validate_db_type(conn):
                logger.error(
                    "[%s] db_type=%s ile sunucu uyumsuz, profilleme iptal.",
                    self.db_config.alias, self.db_config.db_type,
                )
                return db_profile

        schemas = self.connector.discover_schemas()
        logger.info("[%s] %d sema kesfedildi: %s", self.db_config.alias, len(schemas), schemas)

        # Resume: tamamlanmis semalari belirle
        completed_schemas: Dict[str, SchemaProfile] = {}
        if resumed_profile:
            for sp in resumed_profile.schemas:
                completed_schemas[sp.schema_name] = sp
            logger.info(
                "[%s] Resume: %d sema zaten tamamlanmis, atlaniyor: %s",
                self.db_config.alias,
                len(completed_schemas),
                list(completed_schemas.keys()),
            )

        # Tum tablolari say (progress bar icin)
        total_tables = 0
        completed_table_count = 0
        schema_tables: Dict[str, List[Dict]] = {}
        for schema in schemas:
            if schema in completed_schemas:
                completed_table_count += completed_schemas[schema].table_count
                continue
            tables = self.connector.discover_tables(schema)
            schema_tables[schema] = tables
            total_tables += len(tables)

        logger.info(
            "[%s] Toplam %d tablo profillecek%s.",
            self.db_config.alias,
            total_tables,
            f" ({completed_table_count} tablo onceden tamamlanmis)" if completed_table_count else "",
        )

        pbar = tqdm(
            total=total_tables + completed_table_count,
            initial=completed_table_count,
            desc=f"[{self.db_config.alias}] Profilleme",
        )
        table_idx = 0

        for schema in schemas:
            if schema in completed_schemas:
                db_profile.schemas.append(completed_schemas[schema])
                continue

            tables = schema_tables[schema]
            schema_prof = self._profile_schema(schema, tables, pbar, table_idx, total_tables)
            db_profile.schemas.append(schema_prof)
            table_idx += len(tables)

            # Checkpoint kaydet
            if checkpoint_dir:
                self.save_checkpoint(db_profile, checkpoint_dir)

        pbar.close()

        # Aggregation
        db_profile.total_schemas = len(db_profile.schemas)
        db_profile.total_tables = sum(s.table_count for s in db_profile.schemas)
        db_profile.total_columns = sum(
            sum(t.column_count for t in s.tables) for s in db_profile.schemas
        )
        db_profile.total_rows = sum(s.total_rows for s in db_profile.schemas)

        # Total size aggregation
        sizes = [s.total_size_bytes for s in db_profile.schemas if s.total_size_bytes is not None]
        if sizes:
            db_profile.total_size_bytes = sum(sizes)
            db_profile.total_size_display = self._format_size(db_profile.total_size_bytes)

        scored_schemas = [
            s for s in db_profile.schemas
            if s.schema_quality_score > 0
        ]
        if scored_schemas:
            db_profile.overall_quality_score = sum(
                s.schema_quality_score for s in scored_schemas
            ) / len(scored_schemas)

        return db_profile

    def _profile_schema(
        self,
        schema: str,
        tables: List[Dict],
        pbar: tqdm,
        start_idx: int,
        total: int,
    ) -> SchemaProfile:
        """Bir semayi profille."""
        schema_prof = SchemaProfile(schema_name=schema, table_count=len(tables))

        with self.connector.connection() as conn:
            # Metadata prefetch
            metadata = self._fetch_schema_metadata(conn, schema)

            for i, table_info in enumerate(tables):
                table_name = table_info["table_name"]
                table_type = table_info.get("table_type", "BASE TABLE")
                estimated = int(table_info.get("estimated_rows", 0))

                pbar.set_postfix_str(f"{schema}.{table_name}")

                try:
                    table_prof = self._profile_table(
                        conn, schema, table_name, table_type, estimated, metadata
                    )
                    schema_prof.tables.append(table_prof)
                    schema_prof.total_rows += table_prof.row_count
                except Exception as e:
                    logger.error(
                        "[%s.%s] Tablo profilleme hatasi: %s", schema, table_name, e
                    )

                pbar.update(1)

        # Schema size aggregation
        sizes = [t.table_size_bytes for t in schema_prof.tables if t.table_size_bytes is not None]
        if sizes:
            schema_prof.total_size_bytes = sum(sizes)
            schema_prof.total_size_display = self._format_size(schema_prof.total_size_bytes)

        # Schema quality (bos tablolar haric)
        scored_tables = [
            t for t in schema_prof.tables
            if t.row_count > 0 and t.table_quality_grade != "N/A"
        ]
        if scored_tables:
            schema_prof.schema_quality_score = sum(
                t.table_quality_score for t in scored_tables
            ) / len(scored_tables)

        return schema_prof

    def _fetch_schema_metadata(
        self,
        conn,
        schema: str,
    ) -> Dict[str, List[Dict]]:
        """Sema icin tum kolon metadata'sini tek sorguda cek."""
        sql = self.sql.load("metadata")
        try:
            with conn.cursor() as cur:
                if self.db_config.db_type == "mssql":
                    cur.execute(sql, [schema])
                else:
                    cur.execute(sql, {"schema_name": schema})
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
        except Exception as e:
            logger.warning("[%s] Metadata cekme hatasi: %s", schema, e)
            return {}

        # Tablo bazinda grupla
        metadata: Dict[str, List[Dict]] = {}
        for row in rows:
            row_dict = dict(zip(cols, row))
            tname = row_dict["table_name"]
            if tname not in metadata:
                metadata[tname] = []
            metadata[tname].append(row_dict)

        return metadata

    def _profile_table(
        self,
        conn,
        schema: str,
        table: str,
        table_type: str,
        estimated_rows: int,
        metadata: Dict[str, List[Dict]],
    ) -> TableProfile:
        """Bir tabloyu profille."""
        start_time = time.time()

        # Row count
        rc = self.basic.get_row_count(conn, schema, table)
        row_count = rc["row_count"]
        row_estimated = rc["estimated"]

        # Table size
        size_bytes = self.connector.get_table_size(conn, schema, table)
        size_display = self._format_size(size_bytes) if size_bytes is not None else ""

        # Sampling karar
        sampled = row_count > self.prof_config.sample_threshold
        sample_pct = self.prof_config.sample_percent if sampled else None

        # Column metadata
        col_meta = metadata.get(table, [])
        if not col_meta:
            return TableProfile(
                schema_name=schema,
                table_name=table,
                table_type=table_type,
                row_count=row_count,
                estimated_rows=estimated_rows,
                row_count_estimated=row_estimated,
                table_size_bytes=size_bytes,
                table_size_display=size_display,
                column_count=0,
                profiled_at=datetime.now().isoformat(),
                profile_duration_sec=time.time() - start_time,
                sampled=sampled,
                sample_percent=sample_pct,
            )

        columns: List[ColumnProfile] = []
        for cm in col_meta:
            try:
                col_prof = self._profile_column(conn, schema, table, cm, row_count)
                columns.append(col_prof)
            except Exception as e:
                logger.warning(
                    "[%s.%s.%s] Kolon profilleme hatasi: %s",
                    schema, table, cm.get("column_name", "?"), e,
                )

        # Table quality (bos tablolar N/A olur, ortalamaya dahil edilmez)
        if row_count == 0:
            tq_score = 0.0
            tq_grade = "N/A"
        else:
            scored_cols = [c for c in columns if c.quality_score > 0]
            tq_score = 0.0
            tq_grade = "N/A"
            if scored_cols:
                tq_score = sum(c.quality_score for c in scored_cols) / len(scored_cols)
                tq_grade = self.quality.grade(tq_score)

        duration = time.time() - start_time

        return TableProfile(
            schema_name=schema,
            table_name=table,
            table_type=table_type,
            row_count=row_count,
            estimated_rows=estimated_rows,
            row_count_estimated=row_estimated,
            table_size_bytes=size_bytes,
            table_size_display=size_display,
            column_count=len(columns),
            columns=columns,
            profiled_at=datetime.now().isoformat(),
            profile_duration_sec=round(duration, 2),
            sampled=sampled,
            sample_percent=sample_pct,
            table_quality_score=tq_score,
            table_quality_grade=tq_grade,
        )

    def _profile_column(
        self,
        conn,
        schema: str,
        table: str,
        col_meta: Dict[str, Any],
        row_count: int,
    ) -> ColumnProfile:
        """Bir kolonu profille."""
        col_name = col_meta["column_name"]
        data_type = col_meta["data_type"]

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
        )

        if row_count == 0:
            col_prof.quality_flags.append("empty_table")
            col_prof.quality_grade = "N/A"
            return col_prof

        # Basic metrics
        basics = self.basic.get_column_basics(conn, schema, table, col_name, row_count)
        col_prof.null_count = basics["null_count"]
        col_prof.null_ratio = basics["null_ratio"]
        col_prof.distinct_count = basics["distinct_count"]
        col_prof.distinct_ratio = basics["distinct_ratio"]
        col_prof.min_value = basics["min_value"]
        col_prof.max_value = basics["max_value"]

        # Top N values
        col_prof.top_n_values = self.distribution.get_top_n(
            conn, schema, table, col_name,
            self.prof_config.top_n_values, row_count,
        )

        # Numeric specific
        if is_numeric_type(data_type):
            stats = self.distribution.get_numeric_stats(conn, schema, table, col_name)
            if stats:
                col_prof.mean = stats.get("mean")
                col_prof.stddev = stats.get("stddev")
                col_prof.percentiles = {
                    k: v for k, v in stats.items() if k.startswith("p")
                }

            col_prof.histogram = self.distribution.get_histogram(
                conn, schema, table, col_name
            )

            # Outlier detection
            outlier_result = self.outlier.detect(
                conn, schema, table, col_name,
                self.prof_config.outlier_iqr_multiplier,
            )
            if outlier_result:
                col_prof.outlier_count = outlier_result.get("outlier_count")
                col_prof.outlier_ratio = outlier_result.get("outlier_ratio")
                col_prof.outlier_bounds = {
                    "lower": outlier_result.get("lower_bound"),
                    "upper": outlier_result.get("upper_bound"),
                    "q1": outlier_result.get("q1"),
                    "q3": outlier_result.get("q3"),
                    "iqr": outlier_result.get("iqr"),
                }

        # String pattern analysis
        if is_string_type(data_type):
            pattern_result = self.pattern.analyze(
                conn, schema, table, col_name, row_count
            )
            if pattern_result:
                col_prof.detected_patterns = pattern_result.get("patterns")
                col_prof.dominant_pattern = pattern_result.get("dominant_pattern")

        # Quality scoring
        score, grade, flags = self.quality.score_column(col_prof)
        col_prof.quality_score = score
        col_prof.quality_grade = grade
        col_prof.quality_flags = flags

        return col_prof

    @staticmethod
    def _format_size(size_bytes: Optional[int]) -> str:
        """Byte boyutunu okunabilir formata cevir."""
        if size_bytes is None or size_bytes < 0:
            return ""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"

    def save_checkpoint(self, profile: DatabaseProfile, output_dir: str) -> str:
        """Sema bazinda checkpoint kaydet (atomic write)."""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"profil_{profile.db_alias}_checkpoint.json"
        filepath = os.path.join(output_dir, filename)

        data = asdict(profile)
        data["_checkpoint"] = {
            "completed_schemas": [s.schema_name for s in profile.schemas],
            "saved_at": datetime.now().isoformat(),
            "is_complete": False,
        }

        tmp_path = filepath + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp_path, filepath)

        logger.info(
            "Checkpoint kaydedildi: %s (%d sema)",
            filepath, len(profile.schemas),
        )
        return filepath

    def save_intermediate(self, profile: DatabaseProfile, output_dir: str) -> str:
        """Profilleme sonuclarini JSON olarak kaydet."""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"profil_{profile.db_alias}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(asdict(profile), f, ensure_ascii=False, indent=2, default=str)

        logger.info("Ara sonuc kaydedildi: %s", filepath)
        return filepath
