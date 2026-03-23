"""CLI giris noktasi."""

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict
from datetime import datetime

from src.config_loader import AppConfig, ConfigError, load_config
from src.mapping_annotator import MappingAnnotator
from src.profiler import DatabaseProfile, Profiler
from src.report.excel_report import ExcelReportGenerator
from src.report.html_report import HtmlReportGenerator

logger = logging.getLogger("profil")


def setup_logging(level: str, log_file: str) -> None:
    """Logging yapilandirmasi."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


def parse_args() -> argparse.Namespace:
    """CLI argumanlari."""
    parser = argparse.ArgumentParser(
        description="Kaynak Tablo Profilleme Araci (PostgreSQL / MSSQL / Oracle / HANA BW)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Config YAML dosya yolu",
    )
    parser.add_argument(
        "--db",
        help="Hedef veritabani alias'i (config'deki isim). Belirtilmezse tumu profillenir.",
    )
    parser.add_argument(
        "--schema",
        help="Hedef sema (--db ile birlikte kullanilir)",
    )
    parser.add_argument(
        "--table",
        help="Hedef tablo (schema.table formatinda, --db ile birlikte)",
    )
    parser.add_argument(
        "--report-only",
        metavar="JSON_PATH",
        help="Profilleme atla, mevcut JSON'dan rapor uret",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Profilleme yapma, sadece tablo listesini goster",
    )
    parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Excel rapor uretme",
    )
    parser.add_argument(
        "--no-html",
        action="store_true",
        help="HTML rapor uretme",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Detayli log (DEBUG)",
    )
    parser.add_argument(
        "--resume",
        nargs="?",
        const=True,
        default=False,
        metavar="CHECKPOINT_PATH",
        help="Onceki checkpoint'tan devam et. Yol verilmezse output_dir'den otomatik aranir.",
    )
    return parser.parse_args()


def run_dry_run(config: AppConfig, db_key: str = None) -> None:
    """Tablo listesini goster, profilleme yapma."""
    from src.connector_factory import create_connector

    db_keys = [db_key] if db_key else list(config.databases.keys())

    for key in db_keys:
        db_config = config.databases[key]
        connector = create_connector(db_config)

        if not connector.test_connection():
            print(f"[{key}] Baglanti kurulamadi!")
            continue

        schemas = connector.discover_schemas()
        total = 0
        total_size = 0
        with connector.connection() as conn:
            for schema in schemas:
                tables = connector.discover_tables(schema)
                total += len(tables)
                print(f"\n[{key}] {schema} ({len(tables)} tablo):")
                for t in tables:
                    est = t.get("estimated_rows", 0)
                    size = connector.get_table_size(conn, schema, t["table_name"])
                    size_str = Profiler._format_size(size) if size is not None else "-"
                    if size:
                        total_size += size
                    print(f"  {t['table_name']:40s} ~{est:>12,} satir  {size_str:>10s}  ({t.get('table_type', '')})")

        total_size_str = Profiler._format_size(total_size) if total_size else "-"
        print(f"\n[{key}] Toplam: {len(schemas)} sema, {total} tablo, {total_size_str}")


def generate_reports(
    config: AppConfig,
    profile: DatabaseProfile,
    no_excel: bool = False,
    no_html: bool = False,
) -> None:
    """Raporlari uret."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = config.output_dir
    os.makedirs(output_dir, exist_ok=True)

    if not no_excel and config.reporting.excel_enabled:
        filename = config.reporting.excel_filename_template.format(
            db_alias=profile.db_alias, timestamp=timestamp
        )
        excel_path = os.path.join(output_dir, filename)
        excel_gen = ExcelReportGenerator(mapping_enabled=config.mapping.enabled)
        excel_gen.generate(profile, excel_path)

    if not no_html and config.reporting.html_enabled:
        filename = config.reporting.html_filename_template.format(
            db_alias=profile.db_alias, timestamp=timestamp
        )
        html_path = os.path.join(output_dir, filename)
        template_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "templates"
        )
        html_gen = HtmlReportGenerator(
            template_dir=template_dir,
            embed_assets=config.reporting.embed_assets,
        )
        html_gen.generate(profile, html_path)


def annotate_with_mapping(config: AppConfig, profile: DatabaseProfile) -> None:
    """Profilleme sonuclarina DWH mapping bilgisi ekle."""
    if not config.mapping.enabled or not config.mapping.mapping_file:
        return

    annotator = MappingAnnotator(config.mapping.mapping_file)
    for schema in profile.schemas:
        for table in schema.tables:
            table_ann = annotator.annotate_table(schema.schema_name, table.table_name)
            table.dwh_mapped = table_ann["dwh_mapped"]
            table.dwh_target_tables = table_ann["dwh_target_tables"]

            for col in table.columns:
                col_ann = annotator.annotate_column(
                    schema.schema_name, table.table_name, col.column_name
                )
                col.dwh_mapped = col_ann["dwh_mapped"]
                col.dwh_targets = col_ann["dwh_targets"]


def main() -> None:
    """Ana giris noktasi."""
    args = parse_args()

    # Config yukle
    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"Config hatasi: {e}", file=sys.stderr)
        sys.exit(1)

    # Logging
    log_level = "DEBUG" if args.verbose else config.log_level
    setup_logging(log_level, config.log_file)

    # DB dogrulama
    if args.db and args.db not in config.databases:
        print(f"Veritabani alias'i bulunamadi: '{args.db}'", file=sys.stderr)
        print(f"Mevcut alias'lar: {list(config.databases.keys())}", file=sys.stderr)
        sys.exit(1)

    # Dry run
    if args.dry_run:
        run_dry_run(config, args.db)
        return

    # Report-only mode
    if args.report_only:
        logger.info("Report-only modu: %s", args.report_only)
        with open(args.report_only, "r", encoding="utf-8") as f:
            data = json.load(f)
        from src.profiler import (
            ColumnProfile, DatabaseProfile, SchemaProfile, TableProfile,
        )
        profile = _dict_to_profile(data)
        annotate_with_mapping(config, profile)
        generate_reports(config, profile, args.no_excel, args.no_html)
        return

    # Profilleme
    db_keys = [args.db] if args.db else list(config.databases.keys())

    for db_key in db_keys:
        logger.info("=== Profilleme basliyor: %s ===", db_key)
        profiler = Profiler(config, db_key)

        # Schema/table filtresi
        if args.table:
            # format: schema.table_name
            parts = args.table.split(".", 1)
            if len(parts) == 2:
                config.databases[db_key].schema_filter = [parts[0]]
                table_filter = parts[1]
            else:
                table_filter = parts[0]
        else:
            table_filter = None
            if args.schema:
                config.databases[db_key].schema_filter = [args.schema]

        # Resume: checkpoint yukle
        resumed_profile = None
        if args.resume:
            resumed_profile = _load_checkpoint(args.resume, db_key, config.output_dir)

        profile = profiler.profile_database(
            resumed_profile=resumed_profile,
            checkpoint_dir=config.output_dir,
            table_filter=table_filter,
        )

        # Mapping annotasyonu
        annotate_with_mapping(config, profile)

        # Ara sonuc kaydet
        json_path = profiler.save_intermediate(profile, config.output_dir)

        # Checkpoint temizle
        checkpoint_path = os.path.join(
            config.output_dir, f"profil_{db_key}_checkpoint.json"
        )
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            logger.info("Checkpoint temizlendi: %s", checkpoint_path)

        # Raporlar
        generate_reports(config, profile, args.no_excel, args.no_html)

        logger.info(
            "=== %s tamamlandi: %d sema, %d tablo, %d kolon, %s satir, %s, kalite: %.1f%% ===",
            db_key,
            profile.total_schemas,
            profile.total_tables,
            profile.total_columns,
            f"{profile.total_rows:,}",
            profile.total_size_display or "-",
            profile.overall_quality_score * 100,
        )

    print("\nProfilleme tamamlandi. Raporlar:", config.output_dir)


def _load_checkpoint(resume_arg, db_alias: str, output_dir: str):
    """Checkpoint dosyasini yukle. Bulunamazsa None dondur."""
    if isinstance(resume_arg, str) and resume_arg is not True:
        checkpoint_path = resume_arg
    else:
        checkpoint_path = os.path.join(output_dir, f"profil_{db_alias}_checkpoint.json")

    if not os.path.exists(checkpoint_path):
        logger.info("Checkpoint bulunamadi: %s (sifirdan baslanacak)", checkpoint_path)
        return None

    logger.info("Checkpoint yukleniyor: %s", checkpoint_path)
    try:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Checkpoint okunamadi: %s (%s). Sifirdan baslanacak.", checkpoint_path, e)
        return None

    if data.get("db_alias") != db_alias:
        logger.warning(
            "Checkpoint db_alias uyumsuz: beklenen=%s, bulunan=%s. Atlaniyor.",
            db_alias, data.get("db_alias"),
        )
        return None

    profile = _dict_to_profile(data)
    checkpoint_meta = data.get("_checkpoint", {})
    completed = checkpoint_meta.get("completed_schemas", [])
    logger.info("Checkpoint yuklendi: %d sema tamamlanmis (%s)", len(completed), ", ".join(completed))
    return profile


def _dict_to_profile(data: dict) -> DatabaseProfile:
    """JSON dict'i DatabaseProfile'a cevir."""
    from src.profiler import ColumnProfile, SchemaProfile, TableProfile

    schemas = []
    for s_data in data.get("schemas", []):
        tables = []
        for t_data in s_data.get("tables", []):
            columns = []
            for c_data in t_data.get("columns", []):
                columns.append(ColumnProfile(**{
                    k: v for k, v in c_data.items()
                    if k in ColumnProfile.__dataclass_fields__
                }))
            t_data_clean = {
                k: v for k, v in t_data.items()
                if k in TableProfile.__dataclass_fields__ and k != "columns"
            }
            tables.append(TableProfile(**t_data_clean, columns=columns))
        s_data_clean = {
            k: v for k, v in s_data.items()
            if k in SchemaProfile.__dataclass_fields__ and k != "tables"
        }
        schemas.append(SchemaProfile(**s_data_clean, tables=tables))

    db_data = {
        k: v for k, v in data.items()
        if k in DatabaseProfile.__dataclass_fields__ and k != "schemas"
    }
    profile = DatabaseProfile(**db_data, schemas=schemas)
    _recalc_grades(profile)
    return profile


def _recalc_grades(profile: DatabaseProfile) -> None:
    """JSON'dan yuklenen profilde grade'leri yeniden hesapla."""
    from src.metrics.quality import QualityScorer

    for schema in profile.schemas:
        scored = [t for t in schema.tables if t.row_count > 0 and t.column_count > 0 and t.table_quality_grade != "N/A"]
        if scored:
            schema.schema_quality_grade = QualityScorer.grade(schema.schema_quality_score)
        else:
            schema.schema_quality_grade = "N/A"


if __name__ == "__main__":
    main()
