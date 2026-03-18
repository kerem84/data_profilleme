# Mock Database Setup (PostgreSQL + MSSQL + Oracle)

## Hızlı Başlangıç

```bash
# 1. Database'leri ayağa kaldır
docker compose up -d

# 2. Python bağımlılıklarını kur
pip install -r requirements.txt

# 3. Tabloları oluştur ve test sorgularını çalıştır
python db_manager.py
```

> ⚠️ **Oracle XE** ilk açılışta ~2-3 dakika sürebilir. Script otomatik olarak bekler.

## Bağlantı Bilgileri

| Database   | Host      | Port | User     | Password   | Database/Service |
|------------|-----------|------|----------|------------|------------------|
| PostgreSQL | localhost | 5432 | testuser | Test1234!  | mockdb           |
| MSSQL      | localhost | 1433 | sa       | Test1234!  | master           |
| Oracle XE  | localhost | 1521 | system   | Test1234!  | XEPDB1           |

## Kendi Sorgularını Çalıştır

```python
from db_manager import get_engine, run_query

# Herhangi bir DB'ye bağlan
engine = get_engine("postgresql")  # veya "mssql", "oracle"

# SELECT → DataFrame döner
df = run_query(engine, "SELECT * FROM employees WHERE department = :dept", {"dept": "Yazılım"})
print(df)

# INSERT / UPDATE / DELETE
run_query(engine, "UPDATE employees SET salary = :s WHERE name = :n", {"s": 40000, "n": "Ahmet Yılmaz"})
```

## Durdur / Temizle

```bash
docker compose down           # Container'ları durdur
docker compose down -v        # Container + verileri sil
```
