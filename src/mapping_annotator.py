"""DWH mapping annotasyonu (opsiyonel modul)."""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MappingAnnotator:
    """
    Profilleme sonuclarini DWH mapping verisiyle eslestirir.
    Mapping JSON dosyasi yoksa veya devre disi ise bu modul atlanir.
    """

    def __init__(self, mapping_file: str):
        self.mapping_file = mapping_file
        self.mapping_data: List[Dict[str, Any]] = []
        self._index: Dict[str, List[Dict[str, str]]] = {}
        self._table_index: Dict[str, List[str]] = {}
        self._load()

    def _load(self) -> None:
        """Mapping JSON dosyasini yukle ve indeksle."""
        try:
            with open(self.mapping_file, "r", encoding="utf-8") as f:
                self.mapping_data = json.load(f)
            logger.info("Mapping dosyasi yuklendi: %d kayit", len(self.mapping_data))
            self._build_index()
        except FileNotFoundError:
            logger.warning("Mapping dosyasi bulunamadi: %s", self.mapping_file)
        except Exception as e:
            logger.error("Mapping dosyasi okuma hatasi: %s", e)

    def _build_index(self) -> None:
        """(schema, table, column) -> [DWH hedefler] indeksi olustur."""
        for record in self.mapping_data:
            src_schema = (record.get("kaynak_sema") or "").lower().strip()
            src_table = (record.get("kaynak_tablo") or "").lower().strip()
            src_column = (record.get("kaynak_kolon") or "").lower().strip()
            tgt_table = record.get("hedef_tablo", "")
            tgt_column = record.get("hedef_kolon", "")

            if not src_table or not src_column:
                continue

            # Kolon indeksi
            key = f"{src_schema}.{src_table}.{src_column}"
            if key not in self._index:
                self._index[key] = []
            self._index[key].append({
                "target_table": tgt_table,
                "target_column": tgt_column,
            })

            # Tablo indeksi
            tbl_key = f"{src_schema}.{src_table}"
            if tbl_key not in self._table_index:
                self._table_index[tbl_key] = []
            target = f"{tgt_table}"
            if target not in self._table_index[tbl_key]:
                self._table_index[tbl_key].append(target)

    def annotate_column(
        self, schema: str, table: str, column: str
    ) -> Dict[str, Any]:
        """Kolon icin DWH mapping bilgisi dondur."""
        key = f"{schema.lower()}.{table.lower()}.{column.lower()}"
        targets = self._index.get(key, [])
        return {
            "dwh_mapped": len(targets) > 0,
            "dwh_targets": [
                f"{t['target_table']}.{t['target_column']}" for t in targets
            ],
        }

    def annotate_table(self, schema: str, table: str) -> Dict[str, Any]:
        """Tablo icin DWH mapping ozeti dondur."""
        key = f"{schema.lower()}.{table.lower()}"
        targets = self._table_index.get(key, [])
        return {
            "dwh_mapped": len(targets) > 0,
            "dwh_target_tables": targets,
        }

    def get_unmapped_summary(
        self,
        all_columns: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        """
        Eslestirilmemis kolonlari dondur.
        all_columns: [{"schema": ..., "table": ..., "column": ...}, ...]
        """
        unmapped = []
        for col in all_columns:
            key = f"{col['schema'].lower()}.{col['table'].lower()}.{col['column'].lower()}"
            if key not in self._index:
                unmapped.append(col)
        return unmapped
