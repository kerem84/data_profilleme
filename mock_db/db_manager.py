"""
Mock Database Manager
=====================
3 farklı database'e (PostgreSQL, MSSQL, Oracle) bağlanıp
örnek tablolar oluşturur ve SQL sorguları çalıştırır.

Kullanım:
    pip install -r requirements.txt
    docker compose up -d
    python db_manager.py
"""

import time
import pandas as pd
from sqlalchemy import create_engine, text

# ═══════════════════════════════════════════════════════
# CONNECTION STRINGS
# ═══════════════════════════════════════════════════════

CONNECTIONS = {
    "postgresql": "postgresql+psycopg2://testuser:Test1234!@localhost:5432/mockdb",
    "mssql":      "mssql+pymssql://sa:Test1234!@localhost:1433/master",
    "oracle":     "oracle+oracledb://system:Test1234!@localhost:1521/?service_name=XEPDB1",
}


# ═══════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════

def get_engine(db_name: str):
    """Belirtilen database için SQLAlchemy engine döner."""
    return create_engine(CONNECTIONS[db_name])


def wait_for_db(db_name: str, retries: int = 30, delay: int = 5):
    """Database hazır olana kadar bekler."""
    engine = get_engine(db_name)
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"  ✓ {db_name} bağlantısı başarılı!")
            return engine
        except Exception as e:
            print(f"  ⏳ {db_name} bekleniyor... ({attempt}/{retries})")
            time.sleep(delay)
    raise ConnectionError(f"{db_name} bağlantısı kurulamadı!")


def run_query(engine, sql: str, params: dict = None) -> pd.DataFrame:
    """SQL çalıştırıp sonucu DataFrame olarak döner."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        if result.returns_rows:
            return pd.DataFrame(result.fetchall(), columns=result.keys())
        conn.commit()
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════
# SAMPLE TABLE CREATION (her DB için ayrı DDL)
# ═══════════════════════════════════════════════════════

DDL = {
    "postgresql": [
        """
        CREATE TABLE IF NOT EXISTS employees (
            id          SERIAL PRIMARY KEY,
            name        VARCHAR(100),
            department  VARCHAR(50),
            salary      NUMERIC(10, 2),
            hire_date   DATE DEFAULT CURRENT_DATE
        )
        """,
        """
        INSERT INTO employees (name, department, salary, hire_date)
        VALUES
            ('Ahmet Yılmaz',  'Yazılım',   32000, '2023-03-15'),
            ('Elif Demir',    'Pazarlama',  28000, '2022-11-01'),
            ('Mehmet Kaya',   'Yazılım',   35000, '2021-06-20'),
            ('Zeynep Aksoy',  'İK',         27000, '2024-01-10'),
            ('Can Öztürk',    'Finans',    30000, '2023-08-05')
        ON CONFLICT DO NOTHING
        """,
    ],
    "mssql": [
        """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'employees')
        CREATE TABLE employees (
            id          INT IDENTITY(1,1) PRIMARY KEY,
            name        NVARCHAR(100),
            department  NVARCHAR(50),
            salary      DECIMAL(10, 2),
            hire_date   DATE DEFAULT GETDATE()
        )
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM employees)
        INSERT INTO employees (name, department, salary, hire_date)
        VALUES
            (N'Ahmet Yılmaz',  N'Yazılım',   32000, '2023-03-15'),
            (N'Elif Demir',    N'Pazarlama',  28000, '2022-11-01'),
            (N'Mehmet Kaya',   N'Yazılım',   35000, '2021-06-20'),
            (N'Zeynep Aksoy',  N'İK',         27000, '2024-01-10'),
            (N'Can Öztürk',    N'Finans',    30000, '2023-08-05')
        """,
    ],
    "oracle": [
        """
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE employees (
                    id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name        VARCHAR2(100),
                    department  VARCHAR2(50),
                    salary      NUMBER(10, 2),
                    hire_date   DATE DEFAULT SYSDATE
                )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -955 THEN NULL; END IF;
        END;
        """,
        """
        BEGIN
            INSERT INTO employees (name, department, salary, hire_date)
            VALUES ('Ahmet Yılmaz',  'Yazılım',   32000, DATE '2023-03-15');
            INSERT INTO employees (name, department, salary, hire_date)
            VALUES ('Elif Demir',    'Pazarlama',  28000, DATE '2022-11-01');
            INSERT INTO employees (name, department, salary, hire_date)
            VALUES ('Mehmet Kaya',   'Yazılım',   35000, DATE '2021-06-20');
            INSERT INTO employees (name, department, salary, hire_date)
            VALUES ('Zeynep Aksoy',  'İK',         27000, DATE '2024-01-10');
            INSERT INTO employees (name, department, salary, hire_date)
            VALUES ('Can Öztürk',    'Finans',    30000, DATE '2023-08-05');
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        """,
    ],
}


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def setup_all():
    """Tüm database'lere bağlan, tabloları oluştur, örnek veriyi yükle."""
    engines = {}
    for db_name in CONNECTIONS:
        print(f"\n{'─'*50}")
        print(f"🔌 {db_name.upper()} bağlantısı kuruluyor...")
        engine = wait_for_db(db_name)
        engines[db_name] = engine

        print(f"  📦 Tablolar oluşturuluyor...")
        for ddl in DDL[db_name]:
            run_query(engine, ddl)
        print(f"  ✓ {db_name} hazır!")

    return engines


def demo_queries(engines: dict):
    """Her database üzerinde örnek sorgular çalıştır."""
    # Ortak SELECT sorgusu — 3 DB'de de çalışır
    query = "SELECT * FROM employees WHERE salary > :min_salary"

    print(f"\n{'═'*50}")
    print("📊 SORGU: Maaşı 29.000'den yüksek çalışanlar")
    print(f"{'═'*50}")

    for db_name, engine in engines.items():
        print(f"\n── {db_name.upper()} ──")
        df = run_query(engine, query, {"min_salary": 29000})
        print(df.to_string(index=False))

    # DB-specific sorgular
    print(f"\n{'═'*50}")
    print("📊 Departman bazlı ortalama maaş")
    print(f"{'═'*50}")

    agg_queries = {
        "postgresql": """
            SELECT department, COUNT(*) as count, ROUND(AVG(salary), 2) as avg_salary
            FROM employees GROUP BY department ORDER BY avg_salary DESC
        """,
        "mssql": """
            SELECT department, COUNT(*) as count, ROUND(AVG(salary), 2) as avg_salary
            FROM employees GROUP BY department ORDER BY avg_salary DESC
        """,
        "oracle": """
            SELECT department, COUNT(*) as count, ROUND(AVG(salary), 2) as avg_salary
            FROM employees GROUP BY department ORDER BY avg_salary DESC
        """,
    }

    for db_name, engine in engines.items():
        print(f"\n── {db_name.upper()} ──")
        df = run_query(engine, agg_queries[db_name])
        print(df.to_string(index=False))


if __name__ == "__main__":
    print("🚀 Mock Database'ler kuruluyor...\n")
    engines = setup_all()
    demo_queries(engines)

    print(f"\n{'═'*50}")
    print("✅ Tüm database'ler hazır! Kendi sorgularınızı çalıştırabilirsiniz.")
    print()
    print("Örnek kullanım:")
    print('  engine = get_engine("postgresql")')
    print('  df = run_query(engine, "SELECT * FROM employees")')
    print('  print(df)')
    print(f"{'═'*50}")
