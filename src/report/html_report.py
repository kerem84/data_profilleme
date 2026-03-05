"""Interaktif HTML rapor uretici."""

import logging
import os
from collections import Counter
from typing import Any, Dict, List

import jinja2

from src.metrics.quality import QualityScorer
from src.profiler import DatabaseProfile

logger = logging.getLogger(__name__)


class HtmlReportGenerator:
    """Jinja2 tabanli self-contained HTML rapor uretici."""

    def __init__(self, template_dir: str, embed_assets: bool = True):
        self.template_dir = template_dir
        self.embed_assets = embed_assets
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir),
            autoescape=True,
        )

    def generate(self, profile: DatabaseProfile, output_path: str) -> str:
        """HTML raporu uret. Dosya yolunu dondur."""
        template = self.env.get_template("report.html.j2")

        # Asset iceriklerini oku
        css_content = ""
        js_content = ""
        if self.embed_assets:
            css_path = os.path.join(self.template_dir, "assets", "style.css")
            js_path = os.path.join(self.template_dir, "assets", "charts.js")
            if os.path.exists(css_path):
                with open(css_path, "r", encoding="utf-8") as f:
                    css_content = f.read()
            if os.path.exists(js_path):
                with open(js_path, "r", encoding="utf-8") as f:
                    js_content = f.read()

        # Grade dagilimi
        grade_dist = self._calc_grade_distribution(profile)

        # Top 10 tablo (satir sayisina gore)
        top_tables = self._get_top_tables(profile, limit=10)

        # Overall grade
        overall_grade = QualityScorer.grade(profile.overall_quality_score)

        # Profile'i dict'e cevir (Jinja2 uyumlulugu icin)
        profile_dict = self._profile_to_dict(profile)

        html = template.render(
            profile=profile_dict,
            embed_assets=self.embed_assets,
            css_content=css_content,
            js_content=js_content,
            grade_distribution=grade_dist,
            top_tables=top_tables,
            overall_grade=overall_grade,
            grade_fn=QualityScorer.grade,
        )

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info("HTML rapor olusturuldu: %s", output_path)
        return output_path

    def _calc_grade_distribution(self, profile: DatabaseProfile) -> Dict[str, int]:
        """Tablo bazinda kalite notu dagilimi (bos tablolar haric)."""
        grades: List[str] = []
        for schema in profile.schemas:
            for table in schema.tables:
                if table.table_quality_grade != "N/A":
                    grades.append(table.table_quality_grade)
        counter = Counter(grades)
        return {g: counter.get(g, 0) for g in ["A", "B", "C", "D", "F"]}

    def _get_top_tables(
        self, profile: DatabaseProfile, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """En buyuk tablolar (satir sayisina gore)."""
        all_tables = []
        for schema in profile.schemas:
            for table in schema.tables:
                all_tables.append({
                    "name": f"{table.schema_name}.{table.table_name}",
                    "rows": table.row_count,
                })
        all_tables.sort(key=lambda x: x["rows"], reverse=True)
        return all_tables[:limit]

    def _profile_to_dict(self, profile: DatabaseProfile) -> Dict[str, Any]:
        """DatabaseProfile'i Jinja2 uyumlu dict'e cevir."""
        from dataclasses import asdict
        return asdict(profile)
