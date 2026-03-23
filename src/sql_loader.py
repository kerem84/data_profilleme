"""SQL template yukleyici ve parametre enjeksiyonu."""

import os
import re
from typing import Dict

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_HANA_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_/][a-zA-Z0-9_/]*$")


class SqlLoader:
    """SQL dosyalarini yukler, identifier parametrelerini guvenli sekilde yerlestirir."""

    def __init__(self, sql_dir: str, db_type: str = "postgresql"):
        self.sql_dir = os.path.join(sql_dir, db_type)
        self.db_type = db_type
        self._cache: Dict[str, str] = {}

        if not os.path.isdir(self.sql_dir):
            raise FileNotFoundError(
                f"SQL sablon dizini bulunamadi: {self.sql_dir}"
            )

    def load(self, template_name: str, **identifier_params: str) -> str:
        """
        SQL sablonunu yukle, identifier parametrelerini yerlestirir.

        identifier_params: schema_name, table_name, column_name gibi SQL identifier'lari.
        Bunlar {param} formatinda sablonda yer alir ve validate edilir.
        psycopg2 %(param)s ve pyodbc ? formatindaki value parametreleri dokunulmaz.
        """
        if template_name not in self._cache:
            file_path = os.path.join(self.sql_dir, f"{template_name}.sql")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"SQL sablonu bulunamadi: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                self._cache[template_name] = f.read()

        sql = self._cache[template_name]

        # Identifier parametrelerini validate ve yerlestirir
        for key, value in identifier_params.items():
            quoted = self.validate_identifier(value)
            sql = sql.replace(f"{{{key}}}", quoted)

        return sql

    def validate_identifier(self, name: str) -> str:
        """
        SQL identifier'ini dogrula ve dialect'e gore quote et.
        PostgreSQL: "name", MSSQL: [name], HANA BW: "name" (/ destekli)
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
