# PostgreSQL Kaynak Tablo Profilleme Araci

PostgreSQL veritabanlarindaki tablolarin veri kalitesini ve yapisini analiz eden genel amacli bir profilleme araci. Config dosyasi uzerinden herhangi bir PostgreSQL veritabani tanimlanabilir.

## Ozellikler

- **Temel metrikler**: Satir sayisi, NULL orani, distinct sayisi, min/max
- **Dagilim analizi**: Top N degerler, numerik istatistikler (percentile), histogram
- **Pattern tespiti**: Regex tabanli string pattern analizi (email, telefon, TC kimlik, UUID, tarih vb.)
- **Outlier tespiti**: IQR yontemiyle numerik outlier
- **Kalite skorlama**: Kolon bazinda 0-100 skor, A-F grade, kalite bayraklari
- **DWH mapping**: Opsiyonel kaynak-hedef eslestirme annotasyonu
- **Raporlama**: Excel (.xlsx) + interaktif HTML (Chart.js grafikleri)
- **Buyuk tablo destegi**: 5M+ satirli tablolarda otomatik sampling

## Kurulum

```bash
# Python 3.9+ gerekli
pip install -r requirements.txt
```

### Gereksinimler

- Python 3.9+
- PostgreSQL veritabanina erisim (read-only yeterli)

## Konfigürasyon

Ornek config dosyasini kopyalayip duzenleyin:

```bash
cp config/config.example.yaml config/config.yaml
```

`config/config.yaml` icinde veritabani baglanti bilgilerini doldurun:

```yaml
databases:
  my_db:                         # Serbest alias ismi
    host: "hostname"
    port: 5432
    dbname: "database_name"
    user: "username"
    password: "password"
    connect_timeout: 15
    statement_timeout: 300000    # ms (sorgu timeout)
    schema_filter: "*"           # "*" = tum non-system semalar
                                 # veya liste: ["schema1", "schema2"]
```

Birden fazla veritabani tanimlanabilir. `schema_filter` ile belirli semalari filtreleyebilirsiniz.

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
  string_patterns:               # Regex pattern tanimlari
    email: "^[a-zA-Z0-9._%+-]+@..."
    phone_tr: "^(\\+90|0)?[0-9]{10}$"
    # ... istenen pattern eklenebilir
```

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
| Ozet | Veritabani geneli |
| Schema Ozet | Sema bazinda tablo sayisi, satir, kalite |
| Tablo Profil | Tablo bazinda metrikler |
| Kolon Profil | Kolon bazinda tum metrikler |
| Top Degerler | En sik degerler |
| Pattern Analiz | String pattern sonuclari |
| Outlier Rapor | Numerik outlier bilgileri |

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

## Proje Yapisi

```
yolcu_profil/
├── config/
│   ├── config.yaml              # Aktif konfigurasyon
│   └── config.example.yaml      # Ornek sablon
├── sql/                         # SQL sablonlari
├── src/
│   ├── cli.py                   # CLI giris noktasi
│   ├── config_loader.py         # YAML config yukleyici
│   ├── db_connector.py          # PostgreSQL baglanti yonetimi
│   ├── sql_loader.py            # SQL template yukleyici
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

## Hata Yonetimi

| Senaryo | Davranis |
|---------|----------|
| Baglanti hatasi | Hata mesaji, dur |
| Sorgu timeout | Metrigi atla, devam et |
| Yetki hatasi | Tabloyu atla, uyari logla, devam et |
| Bos tablo | Sifir metrik, kalite=0 |
