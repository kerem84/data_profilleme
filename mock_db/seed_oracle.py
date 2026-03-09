"""Oracle mock DB seed: testuser schema'sinda ornek tablolar olusturur."""

import oracledb
import time
import sys


def wait_for_oracle(host="localhost", port=1521, service="XEPDB1",
                    user="testuser", password="Test1234!", retries=30, delay=10):
    """Oracle hazir olana kadar bekle."""
    dsn = oracledb.makedsn(host, port, service_name=service)
    for attempt in range(1, retries + 1):
        try:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            conn.close()
            print(f"  Oracle baglanti basarili! (deneme {attempt})")
            return True
        except Exception as e:
            print(f"  Bekleniyor... ({attempt}/{retries}) - {e}")
            time.sleep(delay)
    return False


def seed(host="localhost", port=1521, service="XEPDB1",
         user="testuser", password="Test1234!"):
    """Ornek tablolar olustur ve veri yukle."""
    dsn = oracledb.makedsn(host, port, service_name=service)
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cur = conn.cursor()

    # ── employees tablosu ──
    _exec_safe(cur, """
        CREATE TABLE employees (
            id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name        VARCHAR2(100),
            email       VARCHAR2(150),
            department  VARCHAR2(50),
            salary      NUMBER(10, 2),
            hire_date   DATE DEFAULT SYSDATE
        )
    """)

    _exec_safe(cur, """
        INSERT INTO employees (name, email, department, salary, hire_date)
        SELECT 'Ahmet Yilmaz',  'ahmet@tcdd.gov.tr',  'Yazilim',   32000, DATE '2023-03-15' FROM DUAL UNION ALL
        SELECT 'Elif Demir',    'elif@tcdd.gov.tr',   'Pazarlama', 28000, DATE '2022-11-01' FROM DUAL UNION ALL
        SELECT 'Mehmet Kaya',   'mehmet@tcdd.gov.tr',  'Yazilim',   35000, DATE '2021-06-20' FROM DUAL UNION ALL
        SELECT 'Zeynep Aksoy',  'zeynep@tcdd.gov.tr',  'IK',        27000, DATE '2024-01-10' FROM DUAL UNION ALL
        SELECT 'Can Ozturk',    'can@tcdd.gov.tr',     'Finans',    30000, DATE '2023-08-05' FROM DUAL UNION ALL
        SELECT 'Ayse Yildiz',   'ayse@tcdd.gov.tr',    'Yazilim',   33000, DATE '2023-01-20' FROM DUAL UNION ALL
        SELECT 'Ali Koc',       'ali@tcdd.gov.tr',     'Finans',    31000, DATE '2022-09-15' FROM DUAL UNION ALL
        SELECT 'Fatma Celik',   'fatma@tcdd.gov.tr',   'Pazarlama', 29000, DATE '2023-07-01' FROM DUAL UNION ALL
        SELECT 'Burak Sahin',   'burak@tcdd.gov.tr',   'IK',        26000, DATE '2024-02-28' FROM DUAL UNION ALL
        SELECT 'Seda Arslan',   'seda@tcdd.gov.tr',    'Yazilim',   37000, DATE '2020-05-10' FROM DUAL
    """)
    print("  employees: 10 kayit")

    # ── stations tablosu ──
    _exec_safe(cur, """
        CREATE TABLE stations (
            id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            code        VARCHAR2(10) NOT NULL,
            name        VARCHAR2(100),
            city        VARCHAR2(50),
            region      VARCHAR2(50),
            latitude    NUMBER(10, 6),
            longitude   NUMBER(10, 6),
            is_active   NUMBER(1) DEFAULT 1
        )
    """)

    _exec_safe(cur, """
        INSERT INTO stations (code, name, city, region, latitude, longitude, is_active)
        SELECT 'ANK', 'Ankara Gar',       'Ankara',    'Ic Anadolu',  39.933365, 32.856384, 1 FROM DUAL UNION ALL
        SELECT 'IST', 'Istanbul Haydarpasa','Istanbul', 'Marmara',     40.997700, 29.020200, 1 FROM DUAL UNION ALL
        SELECT 'IZM', 'Izmir Alsancak',   'Izmir',     'Ege',         38.435530, 27.143350, 1 FROM DUAL UNION ALL
        SELECT 'ESK', 'Eskisehir',        'Eskisehir', 'Ic Anadolu',  39.776670, 30.520560, 1 FROM DUAL UNION ALL
        SELECT 'KON', 'Konya',            'Konya',     'Ic Anadolu',  37.871500, 32.484930, 1 FROM DUAL UNION ALL
        SELECT 'ADN', 'Adana',            'Adana',     'Akdeniz',     36.991420, 35.330570, 1 FROM DUAL UNION ALL
        SELECT 'KAY', 'Kayseri',          'Kayseri',   'Ic Anadolu',  38.734800, 35.467510, 1 FROM DUAL UNION ALL
        SELECT 'SIV', 'Sivas',            'Sivas',     'Ic Anadolu',  39.747660, 37.017880, 1 FROM DUAL
    """)
    print("  stations: 8 kayit")

    # ── tickets tablosu (FK ile) ──
    _exec_safe(cur, """
        CREATE TABLE tickets (
            id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ticket_no       VARCHAR2(20) NOT NULL,
            passenger_name  VARCHAR2(100),
            passenger_tc    VARCHAR2(11),
            phone           VARCHAR2(15),
            from_station_id NUMBER REFERENCES stations(id),
            to_station_id   NUMBER REFERENCES stations(id),
            travel_date     DATE,
            price           NUMBER(10, 2),
            status          VARCHAR2(20) DEFAULT 'ACTIVE',
            created_at      TIMESTAMP DEFAULT SYSTIMESTAMP
        )
    """)

    # 50 ornek bilet
    _exec_safe(cur, """
        INSERT INTO tickets (ticket_no, passenger_name, passenger_tc, phone,
                             from_station_id, to_station_id, travel_date, price, status)
        SELECT
            'TK' || TO_CHAR(ROWNUM, 'FM00000'),
            CASE MOD(ROWNUM, 5)
                WHEN 0 THEN 'Ali Veli'
                WHEN 1 THEN 'Ayse Fatma'
                WHEN 2 THEN 'Hasan Huseyin'
                WHEN 3 THEN 'Zehra Nur'
                ELSE 'Kemal Sunal'
            END,
            LPAD(TO_CHAR(10000000000 + ROWNUM), 11, '0'),
            '+90' || LPAD(TO_CHAR(5000000000 + ROWNUM * 7), 10, '0'),
            MOD(ROWNUM, 8) + 1,
            MOD(ROWNUM + 3, 8) + 1,
            DATE '2025-01-01' + MOD(ROWNUM, 90),
            ROUND(50 + DBMS_RANDOM.VALUE(0, 200), 2),
            CASE WHEN MOD(ROWNUM, 10) = 0 THEN 'CANCELLED' ELSE 'ACTIVE' END
        FROM DUAL CONNECT BY ROWNUM <= 50
    """)
    print("  tickets: 50 kayit (FK -> stations)")

    # ── empty_table (bos tablo testi) ──
    _exec_safe(cur, """
        CREATE TABLE empty_table (
            id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name    VARCHAR2(50),
            value   NUMBER
        )
    """)
    print("  empty_table: 0 kayit")

    # Stats guncelle (estimated_rows icin)
    _exec_safe(cur, "BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(USER); END;")
    print("  DBMS_STATS toplanildi")

    conn.commit()
    cur.close()
    conn.close()
    print("\nOracle seed tamamlandi!")


def _exec_safe(cur, sql):
    """SQL calistir, hata olursa atla (tablo zaten var vb.)."""
    try:
        cur.execute(sql)
    except oracledb.DatabaseError as e:
        err = e.args[0]
        # ORA-00955: name already in use (tablo zaten var)
        # ORA-00001: unique constraint violated (data zaten var)
        if err.code in (955, 1):
            pass
        else:
            print(f"    WARN: {err.message}")


if __name__ == "__main__":
    print("Oracle seed baslatiliyor...")
    if not wait_for_oracle():
        print("Oracle baglanti kurulamadi!")
        sys.exit(1)
    seed()
