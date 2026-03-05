# Kaynak Tablo Profilleme Araci (PostgreSQL / MSSQL)

PostgreSQL ve MSSQL veritabanlarindaki tablolarin veri kalitesini ve yapisini analiz eden genel amacli bir profilleme araci. Config dosyasi uzerinden herhangi bir veritabani tanimlanabilir, `db_type` alani ile dialect otomatik secilir.

## Ozellikler

- **Coklu veritabani**: PostgreSQL ve MSSQL destegi, ayni config'de karisik tanim
- **Temel metrikler**: Satir sayisi, NULL orani, distinct sayisi, min/max
- **Dagilim analizi**: Top N degerler, numerik istatistikler (percentile), histogram
- **Pattern tespiti**: String pattern analizi (email, telefon, TC kimlik, UUID, tarih vb.)
- **Outlier tespiti**: IQR yontemiyle numerik outlier
- **Kalite skorlama**: Kolon bazinda 0-100 skor, A-F grade, kalite bayraklari
- **DWH mapping**: Opsiyonel kaynak-hedef eslestirme annotasyonu
- **Raporlama**: Excel (.xlsx) + interaktif HTML (Chart.js grafikleri)
- **Buyuk tablo destegi**: 5M+ satirli tablolarda otomatik sampling
- **DB tipi dogrulama**: Baglanti sonrasi sunucunun config'deki `db_type` ile eslestigi kontrol edilir

## Kurulum

```bash
# Python 3.9+ gerekli
pip install -r requirements.txt
```

### Gereksinimler

- Python 3.9+
- **PostgreSQL**: `psycopg2` (pip ile otomatik gelir)
- **MSSQL**: `pyodbc` (pip ile otomatik gelir) + ODBC driver

### MSSQL ODBC Driver Kurulumu

MSSQL profilleme icin sistemde ODBC driver yuklu olmalidir.

**Windows:**
```
Microsoft ODBC Driver 17 for SQL Server
```
[Microsoft indirme sayfasindan](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) yukleyin.

**Linux (Debian/Ubuntu):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
sudo apt-get update
sudo apt-get install msodbcsql17
```

**macOS:**
```bash
brew install microsoft/mssql-release/msodbcsql17
```

## Konfigurasyon

Ornek config dosyasini kopyalayip duzenleyin:

```bash
cp config/config.example.yaml config/config.yaml
```

### PostgreSQL Veritabani

```yaml
databases:
  my_pg_db:
    db_type: "postgresql"            # Zorunlu degil, varsayilan "postgresql"
    host: "hostname"
    port: 5432
    dbname: "database_name"
    user: "username"
    password: "password"
    connect_timeout: 15
    statement_timeout: 300000        # ms (sorgu timeout)
    schema_filter: "*"               # "*" = tum non-system semalar
                                     # veya liste: ["schema1", "schema2"]
```

### MSSQL Veritabani

```yaml
databases:
  my_mssql_db:
    db_type: "mssql"                 # MSSQL icin zorunlu
    host: "hostname"
    port: 1433
    dbname: "database_name"
    user: "username"
    password: "password"
    driver: "ODBC Driver 17 for SQL Server"   # Sistemdeki ODBC driver adi
    connect_timeout: 15
    statement_timeout: 300000        # ms (sorgu timeout)
    schema_filter: "*"
```

Birden fazla veritabani (farkli tipler dahil) ayni config'de tanimlanabilir. `schema_filter` ile belirli semalari filtreleyebilirsiniz.

### Profilleme Ayarlari

```yaml
profiling:
  top_n_values: 20               # En sik N deger
  sample_threshold: 5000000      # Bu satir sayisinin ustunde sampling
  sample_percent: 10             # Sampling yuzdesi
  outlier_iqr_multiplier: 1.5    # IQR carpani
  quality_weights:               # Kalite skoru agirliklari
    completeness: 0.35
    uniqueness: 0.20
    consistency: 0.25
    validity: 0.20
  string_patterns:               # Pattern tanimlari
    email: "^[a-zA-Z0-9._%+-]+@..."
    phone_tr: "^(\\+90|0)?[0-9]{10}$"
    # ... istenen pattern eklenebilir
```

> **Not:** MSSQL'de native regex destegi yoktur. Config'deki bilinen pattern isimleri (`email`, `phone_tr`, `tc_kimlik`, `uuid`, `iso_date`, `iso_datetime`, `url`, `json_object`, `numeric_string`) icin PATINDEX/LIKE tabanli yaklasik eslesme kullanilir. Kullanici tanimli ozel regex'ler MSSQL'de atlanir.

## Kullanim

### Tum veritabanlarini profille

```bash
python -m src.cli --config config/config.yaml
```

### Tek veritabani

```bash
python -m src.cli --config config/config.yaml --db my_db
```

### Tek sema

```bash
python -m src.cli --config config/config.yaml --db my_db --schema public
```

### Tek tablo

```bash
python -m src.cli --config config/config.yaml --db my_db --table public.users
```

### Dry run (profilleme yapmadan tablo listesi)

```bash
python -m src.cli --config config/config.yaml --dry-run
```

### Mevcut JSON'dan rapor uret

```bash
python -m src.cli --config config/config.yaml --report-only output/profil_data.json
```

### Diger secenekler

| Parametre | Aciklama |
|-----------|----------|
| `--config`, `-c` | Config YAML dosya yolu (zorunlu) |
| `--db` | Hedef veritabani alias'i |
| `--schema` | Hedef sema (`--db` ile birlikte) |
| `--table` | Hedef tablo (`schema.table` formatinda) |
| `--report-only` | Profilleme atla, mevcut JSON'dan rapor uret |
| `--dry-run` | Sadece tablo listesini goster |
| `--no-excel` | Excel rapor uretme |
| `--no-html` | HTML rapor uretme |
| `--verbose`, `-v` | Detayli log (DEBUG) |

## Ciktilar

Raporlar `output/` dizinine uretilir:

| Dosya | Icerik |
|-------|--------|
| `profil_{alias}_{timestamp}.xlsx` | Excel rapor (coklu sheet) |
| `profil_{alias}_{timestamp}.html` | Interaktif HTML rapor |
| `profil_{alias}_{timestamp}.json` | Ara veri (tekrar rapor uretmek icin) |
| `profil.log` | Islem logu |

### Excel Sheet'leri

| Sheet | Icerik |
|-------|--------|
| **Ozet** | Veritabani geneli: toplam sema/tablo/kolon/satir sayilari, genel kalite skoru |
| **Schema Ozet** | Sema bazinda tablo sayisi, toplam satir, ortalama kalite skoru |
| **Tablo Profil** | Her tablo icin satir sayisi, kolon sayisi, kalite notu, profilleme suresi |
| **Kolon Profil** | Kolon bazinda NULL orani, distinct sayisi, min/max, PK/FK bilgisi, kalite skoru ve bayraklar |
| **Top Degerler** | Her kolondaki en sik tekrar eden N deger ve tekrar sayilari (kardinalite analizi) |
| **Pattern Analiz** | String kolonlarda tespit edilen veri desenleri (email, telefon, TC kimlik, UUID, tarih, URL vb.) ve esleme oranlari. Ornegin bir kolondaki verilerin %92'si email formatinda gibi |
| **Outlier Rapor** | Numerik kolonlarda IQR (Interquartile Range) yontemiyle tespit edilen asiri sapan degerler, alt/ust sinirlar ve outlier orani |

### Kalite Notu

Kolon bazinda 0-100 puan, dort boyutun agirlikli ortalamasi:

- **Completeness (35%)**: NULL orani
- **Uniqueness (20%)**: Distinct oran
- **Consistency (25%)**: Pattern tutarliligi
- **Validity (20%)**: Outlier ve gecerlilik

| Grade | Aralik |
|-------|--------|
| A | 90-100 |
| B | 75-89 |
| C | 60-74 |
| D | 40-59 |
| F | 0-39 |
| N/A | Bos tablo (satir=0, ortalamaya dahil edilmez) |

## Proje Yapisi

```
yolcu_profil/
├── config/
│   ├── config.yaml              # Aktif konfigurasyon
│   └── config.example.yaml      # Ornek sablon
├── sql/
│   ├── postgresql/              # PostgreSQL SQL sablonlari
│   └── mssql/                   # MSSQL SQL sablonlari
├── src/
│   ├── cli.py                   # CLI giris noktasi
│   ├── config_loader.py         # YAML config yukleyici
│   ├── base_connector.py        # Abstract connector arayuzu
│   ├── db_connector.py          # PostgreSQL connector
│   ├── mssql_connector.py       # MSSQL connector
│   ├── connector_factory.py     # Connector factory
│   ├── sql_loader.py            # SQL template yukleyici (dialect-aware)
│   ├── profiler.py              # Ana orkestrasyon
│   ├── mapping_annotator.py     # DWH mapping (opsiyonel)
│   ├── metrics/
│   │   ├── basic.py             # Temel metrikler
│   │   ├── distribution.py      # Dagilim analizi
│   │   ├── pattern.py           # String pattern
│   │   ├── outlier.py           # Outlier tespiti
│   │   └── quality.py           # Kalite skorlama
│   └── report/
│       ├── excel_report.py      # Excel rapor
│       └── html_report.py       # HTML rapor
├── templates/                   # Jinja2 HTML sablonlari
├── output/                      # Uretilen raporlar
└── requirements.txt
```

## DB Tipi Dogrulama

Profilleme baslamadan once, baglanti kurulan sunucunun config'deki `db_type` ile eslestigi kontrol edilir:

- **PostgreSQL**: `SELECT version()` sonucu "PostgreSQL" icermeli
- **MSSQL**: `SELECT @@VERSION` sonucu "Microsoft SQL Server" icermeli

Eslesme yoksa profilleme durdurulur ve hata loglanir. Bu, yanlis `db_type` tanimlamalarinin erken tespit edilmesini saglar.

## MSSQL Bilinen Kisitlar

| Konu | Davranis |
|------|----------|
| Regex pattern yok | Bilinen pattern'ler PATINDEX/LIKE ile yaklasik eslenir. Ozel regex'ler MSSQL'de atlanir (0 doner). |
| nvarchar max_length | MSSQL byte cinsinden doner. Metadata'da karakter sayisina cevrilir (byte/2). |
| READ UNCOMMITTED | MSSQL profilleme lock-free calisir (dirty read). Uretim verileri uzerinde guvenlidir. |

## Hata Yonetimi

| Senaryo | Davranis |
|---------|----------|
| Baglanti hatasi | Hata mesaji, dur |
| DB tipi uyumsuzlugu | Hata logla, profillemeyi atla |
| Sorgu timeout | Metrigi atla, devam et |
| Yetki hatasi | Tabloyu atla, uyari logla, devam et |
| Bos tablo | Kalite: N/A, ortalamaya dahil edilmez |
