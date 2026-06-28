# FINAL PROJECT - BIG DATA K3

### Kelas A
| Nama | NRP |
| --- | --- |
| Ahmad Idza Anafin | 5027241017 |
| Aditya Reza Daffansyah | 5027241034 |
| Reza Aziz Simatupang | 5027241051 |
| Ahmad Rafi Fadhillah Dwiputra | 5027241068 |
| Muhammad Rafi' Adly | 5027241082 |

---

# Sistem Audit Ketimpangan Distribusi Fasilitas Kesehatan Balita DKI Jakarta Berbasis Arsitektur Data Lakehouse

Sistem ini adalah platform *Data Lakehouse* end-to-end yang mengintegrasikan data prevalensi gizi buruk balita dengan data sebaran fasilitas kesehatan (Puskesmas, Rumah Sakit, Posyandu) di DKI Jakarta. Sistem ini secara otomatis menghitung *Nutrition Risk Score* dan mengelompokkan wilayah prioritas intervensi menggunakan Machine Learning secara objektif.

---

## Struktur Project

```
fp-bigdata-k3/
├── docker-compose.yml              # Orchestrasi Kafka, HDFS, App
├── requirements.txt                # Python dependencies
├── hadoop.env                      # Konfigurasi Hadoop environment
├── hadoop-conf/
│   └── core-site.xml               # Hadoop HDFS endpoint config
├── data/
│   ├── fallback/                   # CSV fallback dari portal data resmi
│   │   ├── balita_stunting.csv     # Satudata Jakarta 2025
│   │   ├── sebaran_rumahSakit.csv  # BPS DKI Jakarta 2022
│   │   ├── jumlah_nakes.csv        # Satudata Jakarta 2019
│   │   └── lajupertumbuhan.csv     # BPS DKI Jakarta 2024
│   └── shapefiles/                 # GeoJSON/SHP batas wilayah DKI
├── src/
│   ├── ingestion/
│   │   ├── setup_topics.py         # Buat 4 Kafka topics
│   │   ├── producer.py             # CSV → Kafka (4 topik)
│   │   └── consumer_to_hdfs.py     # Kafka → HDFS Bronze (Parquet)
│   ├── processing/
│   │   ├── bronze_to_silver.py     # PySpark ETL: cleaning & standarisasi
│   │   └── silver_to_gold.py       # PySpark: agregasi + NCI/NRS scoring
│   ├── analysis/
│   │   └── ml_analysis.py          # K-Means + Isolation Forest + export
│   ├── visualization/
│   │   └── gis_map.py              # GeoPandas + Folium choropleth map
│   └── app/
│       ├── Dockerfile              # Container runtime (Flask+PySpark+Hadoop)
│       ├── app.py                  # Flask web server (port 5000)
│       ├── services/
│       │   └── dashboard_service.py# Business logic dashboard
│       └── templates/
│           └── index.html          # Dashboard UI (Jinja2 + TailwindCSS)
├── output/                         # Hasil akhir pipeline
│   ├── wilayah_final.json          # Kontrak data dashboard (6 records)
│   ├── wilayah_final.parquet       # Format columnar untuk GIS
│   ├── peta_nrs.png                # Choropleth NRS
│   ├── peta_cluster.png            # Peta cluster risiko
│   └── peta_interaktif.html        # Peta interaktif Folium
├── storage/                        # Docker volumes (HDFS, Kafka)
└── documentation/                  # Screenshot bukti pipeline berjalan
```

---

## 1. Latar Belakang & Kerangka 5V Big Data

### 1.1 Masalah Utama — Bukti Kuantitatif dari Sumber Resmi

Permasalahan gizi buruk pada balita di DKI Jakarta masih sulit diidentifikasi secara dini karena data tersebar di berbagai portal pemerintah tanpa integrasi otomatis. Berikut bukti kuantitatif dari data resmi:

| Sumber Data | Fakta Kuantitatif |
|---|---|
| [Satudata Jakarta 2025](https://satudata.jakarta.go.id/open-data/detail?kategori=dataset&page_url=jumlah-anak-bawah-lima-tahun-balita-bermasalah-gizi-berdasarkan-wilayah&data_no=6) | **44.053 balita bermasalah gizi**: 19.591 stunting, 2.458 gizi buruk, 13.951 gizi kurang, 8.053 underweight — bervariasi signifikan antar kecamatan |
| [BPS DKI Jakarta 2022](https://jakarta.bps.go.id/id/statistics-table/3/YmllemNGUkNVblZLVVhOblJEWnZXbkEzWld0eVVUMDkjMw==/jumlah-rumah-sakit-umum--rumah-sakit-khusus--puskesmas--klinik-pratama--dan-posyandu-menurut-kabupaten-kota-di-provinsi-dki-jakarta--2019.html?year=2022) | 141 RS Umum, 32 RS Khusus, 4.472 Posyandu — distribusi tidak merata: Jakarta Selatan 1.263 posyandu vs Kepulauan Seribu hanya 37 (**rasio 34:1**) |
| [Data Nakes 2019 (Satudata)](https://satudata.jakarta.go.id/open-data/detail?kategori=dataset&page_url=data-jumlah-tenaga-kesehatan-menurut-kecamatan-provinsi-dki-jakarta&data_no=1) | **44.351 tenaga kesehatan** (13.840 dokter, 20.146 perawat, 5.019 bidan, 924 ahli gizi, 4.422 farmasi) — rasio per kecamatan tidak proporsional dengan beban kasus |
| [Proyeksi Penduduk BPS 2024](https://jakarta.bps.go.id/en/statistics-table/3/V1ZSbFRUY3lTbFpEYTNsVWNGcDZjek53YkhsNFFUMDkjMw==/penduduk--laju-pertumbuhan-penduduk--distribusi-persentase-penduduk--kepadatan-penduduk--rasio-jenis-kelamin-penduduk-menurut-kabupaten-kota-di-provinsi-dki-jakarta--2024.html?year=2024) | 10,68 juta jiwa — Jakarta Timur 3,08 juta vs Kepulauan Seribu 28.800 (**rasio populasi 107:1**) |

**Kesimpulan data:** Terdapat ketimpangan distribusi fasilitas dan tenaga kesehatan yang sangat signifikan antar wilayah DKI Jakarta. Wilayah dengan beban gizi buruk tinggi justru memiliki rasio faskes dan nakes terendah — mengindikasikan gap layanan kesehatan yang belum teridentifikasi secara sistematis.

### 1.2 Analisis Gap — Mengapa Solusi Existing Belum Cukup

| Sistem Existing | Keterbatasan | Gap Spesifik |
|---|---|---|
| **SIGA (Kemenkes)** | Hanya menyimpan data gizi individu, tidak mengintegrasikan faskes dan nakes | Tidak ada risk scoring wilayah, tidak mendeteksi ketimpangan distribusi |
| **Sigizi Terpadu** | Berbasis input manual posyandu, tidak ada pipeline otomatis | Tidak ada analisis agregat per wilayah, data tidak real-time |
| **E-PPGBM** | Pencatatan gizi per individu | Bukan analisis ketimpangan distribusi fasilitas kesehatan, tidak ada clustering |
| **Portal Satudata Jakarta** | Menyajikan data mentah, tidak ada analisis lintas dataset | Tidak ada visualisasi risiko, tidak ada rekomendasi prioritas, tidak ada ML |

**Gap yang belum terselesaikan:** Tidak ada sistem yang secara otomatis:
1. **Mengintegrasikan** 4 sumber data (gizi, faskes, nakes, populasi) dalam satu pipeline
2. **Menghitung *risk score*** per wilayah secara objektif dan terkini menggunakan indeks komposit (NCI + NRS)
3. **Mengelompokkan wilayah** prioritas intervensi menggunakan ML (K-Means clustering + Isolation Forest anomaly detection)
4. **Memvisualisasikan secara spasial** ketimpangan distribusi dalam peta GIS interaktif

Sistem ini mengisi seluruh gap tersebut dengan pipeline otomatis Kafka→HDFS→Spark→GIS→Dashboard.

### 1.3 Justifikasi Kerangka 5V Big Data

| Dimensi | Justifikasi untuk Sistem Ini |
|---|---|
| **Volume** | Mengakumulasikan **4 dataset** dengan total ratusan record granular per kecamatan: 44.053 kasus balita gizi buruk × 42 kecamatan × 6 kategori gizi, 6 wilayah × 5 jenis faskes, 44.351 nakes × 42 kecamatan × 5 profesi, dan data populasi 10,68 juta jiwa. Data historis terakumulasi setiap batch ingestion. |
| **Velocity** | Pipeline ingestion berbasis **Apache Kafka** (4 topik: `gizi-raw`, `faskes-raw`, `nakes-raw`, `populasi-raw`) memproses data sebagai aliran (streaming) sehingga NRS dapat diperbarui secara berkala (batch refresh) tanpa analisis manual. Producer saat ini membaca CSV resmi yang dialirkan via Kafka; endpoint API Satudata didesain *pluggable* untuk pemutakhiran otomatis ke depan. |
| **Variety** | Menangani data dengan karakteristik beragam: (1) **structured** — tabel kasus gizi per kecamatan, data populasi per kabupaten/kota; (2) **semi-structured** — CSV dengan format kolom inkonsisten antar sumber (em dash `–` sebagai penanda kosong, satuan populasi ribuan jiwa vs jiwa); (3) **spatial** — koordinat faskes dan batas wilayah GeoJSON/SHP untuk analisis GIS. |
| **Veracity** | Mengatasi ketidakpastian data: (1) 17 varian penulisan nama wilayah dari 4 instansi berbeda yang distandarisasi ke 6 nama resmi; (2) record faskes yang menggunakan em dash `–` sebagai placeholder kosong; (3) baris metadata CSV (catatan, hasil, laju pertumbuhan) yang harus difilter; (4) NaN/Infinity pada record numerik yang di-sanitasi sebelum write Parquet. |
| **Value** | Menghasilkan output kuantitatif yang *actionable*: (1) **Nutrition Risk Score** 0–1 per wilayah untuk pemeringkatan prioritas; (2) **Cluster label** (Risiko Rendah/Sedang/Tinggi) untuk segmentasi intervensi; (3) **Anomaly flag** untuk wilayah yang membutuhkan perhatian khusus; (4) **Peta choropleth interaktif** untuk presentasi ke pemangku kebijakan. Output ini dapat langsung dipakai Pemprov DKI Jakarta untuk realokasi tenaga medis, penambahan posyandu, dan intervensi gizi tertarget. |

---

## 2. Arsitektur Sistem & Justifikasi Teknis

### 2.1 Diagram Arsitektur End-to-End

```
                         INGESTION              STORAGE (HDFS Data Lakehouse)            PROCESSING / ANALYTICS              SERVING
  ┌─────────────┐      ┌───────────┐      ┌──────────────────────────────────┐      ┌──────────────────────────┐      ┌──────────────────┐
  │ Satudata /  │      │  Apache   │      │  BRONZE   →   SILVER   →   GOLD   │      │ PySpark ETL + MLlib      │      │ Gold JSON/Parquet│
  │ BPS  (CSV/  │ ───► │  Kafka    │ ───► │ (raw     (cleaned/    (NCI/NRS    │ ───► │  • K-Means (Silhouette)  │ ───► │  (output/)       │
  │ API*)       │      │ (4 topik) │      │  parquet) partisi)    +cluster)   │      │  • Isolation Forest      │      │   → Flask        │
  └─────────────┘      └───────────┘      └──────────────────────────────────┘      │  • GeoPandas/Folium GIS  │      │     Dashboard    │
                                                                                     └──────────────────────────┘      └──────────────────┘
  * Producer saat ini membaca CSV fallback (batch) lewat Kafka; endpoint API didesain pluggable namun belum diaktifkan.
```

**Alur data end-to-end:**

```
CSV (4 sumber resmi)
  │
  ▼
Kafka Producer (producer.py)
  │ 4 topik: gizi-raw, faskes-raw, nakes-raw, populasi-raw
  ▼
Kafka Consumer (consumer_to_hdfs.py)
  │ sanitize_record() → NaN/Infinity handling
  │ PyArrow → Snappy-compressed Parquet
  ▼
HDFS Bronze Layer (/data/bronze/{gizi,faskes,nakes,populasi}/)
  │ Raw Parquet, tanpa transformasi
  ▼
PySpark ETL — bronze_to_silver.py
  │ Standarisasi 17 varian wilayah → 6 nama resmi
  │ Cleaning: em dash, metadata, duplikat, tipe data
  │ Partisi Parquet per kabupaten_kota
  ▼
HDFS Silver Layer (/data/silver/{gizi_agregat,faskes_clean,nakes_agregat,populasi_clean}/)
  │ Cleaned & standardized, Parquet + Snappy, dipartisi
  ▼
PySpark Aggregation — silver_to_gold.py
  │ JOIN 4 tabel Silver → master dataset
  │ Hitung rasio, NCI, NRS, priority_rank
  │ Min-max normalization via Spark Window function
  ▼
HDFS Gold Layer (/data/gold/wilayah_risk_score/)
  │ Agregat per kabupaten_kota, Parquet
  ▼
ML Analysis — ml_analysis.py
  │ K-Means Clustering (PySpark MLlib) → cluster_id + cluster_label
  │ Isolation Forest (scikit-learn) → is_anomaly + anomaly_score
  │ Pelabelan cluster dinamis berdasarkan mean NRS
  ▼
HDFS Gold Final (/data/gold/wilayah_final/) + model artifacts
  │ Export lokal: output/wilayah_final.json + .parquet
  ▼
GIS Visualization — gis_map.py
  │ GeoPandas + Folium → choropleth NRS + cluster + peta interaktif
  ▼
Flask Dashboard (app.py, port 5000)
  │ Konsumsi output/wilayah_final.json
  │ Serve peta_interaktif.html, peta_nrs.png, peta_cluster.png
  ▼
Browser (localhost:5000)
```

### 2.2 Justifikasi Pemilihan Teknologi

| Layer | Teknologi | Justifikasi Teknis Eksplisit |
| :--- | :--- | :--- |
| **Ingestion** | **Apache Kafka 3.9 (KRaft)** | Dipilih untuk *throughput* tinggi dan *decoupling* antara sumber data dan storage. Kafka bertindak sebagai *message buffer* yang menjamin ketersediaan aliran data (4 topik paralel) tanpa membebani server sumber. KRaft mode menghilangkan dependensi ZooKeeper untuk arsitektur yang lebih sederhana. Konfigurasi `retries=3, retry_backoff_ms=1000` menjamin ketahanan terhadap koneksi intermiten. `enable_auto_commit=False` memastikan data hanya di-commit setelah berhasil ditulis ke HDFS (*exactly-once semantics*). |
| **Storage** | **Apache Hadoop HDFS 3.x** | Digunakan sebagai *Data Lakehouse storage* yang andal, murah, dan *fault-tolerant* melalui mekanisme replikasi internal. HDFS menyimpan data dalam 3 layer (Bronze/Silver/Gold) dengan format **Parquet** (columnar storage) dan kompresi **Snappy** — mempercepat query pemfilteran wilayah tertentu melalui *predicate pushdown* dan menghemat kapasitas penyimpanan hingga 60-80% dibanding CSV. |
| **Processing** | **Apache Spark 3.3 (PySpark + MLlib)** | Memproses kalkulasi matriks kompleks (rasio faskes, NCI, NRS, min-max normalization) secara terdistribusi dan *in-memory*, jauh lebih cepat dibanding pemrosesan sekuensial. **MLlib** digunakan untuk K-Means clustering karena native integrasi dengan Spark DataFrame. `spark.sql.adaptive.enabled=true` mengoptimalkan query plan secara otomatis. |
| **ML (Anomaly)** | **scikit-learn (Isolation Forest)** | Dipakai untuk anomaly detection karena data Gold sudah teragregasi (6 wilayah) — skala kecil yang tidak memerlukan distributed ML. scikit-learn menyediakan `score_samples()` untuk skor anomali kontinu yang lebih informatif dibanding label biner. |
| **GIS** | **GeoPandas + Folium** | GeoPandas untuk operasi spasial (merge data tabular + shapefile), Folium untuk peta interaktif berbasis Leaflet.js yang ringan dan dapat di-embed di dashboard. Matplotlib untuk peta statis beresolusi tinggi (150 DPI). |
| **Serving** | **Flask 3.0** | Web framework *lightweight* untuk menyajikan dashboard audit. Membaca hasil kalkulasi akhir (JSON) tanpa mengganggu cluster komputasi utama. Jinja2 templating untuk render server-side yang cepat. |
| **Containerization** | **Docker Compose** | Orchestrasi seluruh stack (HDFS NameNode + DataNode, Kafka KRaft, App runtime) dalam satu command `docker-compose up -d --build`. Bind-mount `.:/app` memungkinkan live-reload kode tanpa rebuild container. |

### 2.3 Kombinasi Teknologi Sinergis (≥3 Teknologi)

Sistem ini menggabungkan **5 teknologi inti** secara sinergis dalam satu pipeline yang saling bergantung — output setiap komponen menjadi input komponen berikutnya:

| # | Teknologi | Peran dalam Pipeline | Ketergantungan Eksplisit |
|---|---|---|---|
| 1 | **Apache Kafka** | Ingestion — 4 topik streaming (`gizi-raw`, `faskes-raw`, `nakes-raw`, `populasi-raw`) | Output → HDFS Bronze (via consumer) |
| 2 | **Apache Spark (PySpark + MLlib)** | Processing & ML — ETL Bronze→Silver→Gold, K-Means Clustering | Baca/tulis HDFS; input dari Bronze/Silver |
| 3 | **HDFS + Parquet + Snappy** | Lakehouse storage — 3 layer Medallion, partisi `kabupaten_kota` | Output Kafka consumer; dibaca Spark & GIS |
| 4 | **GeoPandas + Folium** | GIS spatial analysis — choropleth NRS + cluster + peta interaktif | Konsumsi `output/wilayah_final.json` + shapefile |
| 5 | **scikit-learn (Isolation Forest)** | Anomaly detection pada data Gold teragregasi | Input Gold layer; output `is_anomaly` + `anomaly_score` |

**Sinergi kunci:** `Kafka → HDFS → Spark → scikit-learn → GIS → Flask` membentuk rantai dependensi eksplisit. Tidak ada komponen yang bisa dihilangkan tanpa memutus pipeline.

Contoh sinergi spesifik:
- **Kafka + HDFS**: Kafka menjamin data tidak hilang (`enable_auto_commit=False`), HDFS menyimpan sebagai Parquet yang dioptimasi untuk Spark.
- **Spark + scikit-learn**: Spark MLlib menangani K-Means karena terintegrasi native dengan DataFrame; scikit-learn menangani Isolation Forest karena data Gold sudah kecil (6 wilayah) dan menyediakan `score_samples()` yang lebih informatif.
- **GIS + Flask**: Folium menghasilkan peta interaktif HTML yang langsung di-embed dalam iframe dashboard Flask tanpa library JS tambahan.

### 2.4 Analisis Kompetitor — Perbandingan Fitur

| Fitur | **Sistem Ini** | SIGA (Kemenkes) | Sigizi Terpadu | E-PPGBM | Satudata Jakarta |
|---|---|---|---|---|---|
| Integrasi multi-sumber otomatis | ✅ 4 sumber | ❌ | ❌ | ❌ | ❌ |
| Pipeline streaming (Kafka) | ✅ 4 topik | ❌ | ❌ | ❌ | ❌ |
| Data Lakehouse (Medallion 3-layer) | ✅ Bronze→Silver→Gold | ❌ | ❌ | ❌ | ❌ |
| Risk scoring per wilayah (NRS) | ✅ NCI + NRS | ❌ | ❌ | ❌ | ❌ |
| Clustering ML wilayah | ✅ K-Means + Silhouette | ❌ | ❌ | ❌ | ❌ |
| Anomaly detection | ✅ Isolation Forest + robustness check | ❌ | ❌ | ❌ | ❌ |
| Peta risiko GIS interaktif | ✅ Choropleth + Folium tooltip | ❌ | ❌ | ❌ | ❌ |
| Analisis berbasis data real | ✅ | ✅ | ✅ | ✅ | ✅ |
| Error handling & fallback otomatis | ✅ CSV fallback + Kafka retry | ❌ | ❌ | ❌ | ❌ |

**Mengapa sistem ini lebih baik:** Tidak ada satupun sistem existing yang menggabungkan ingestion otomatis + risk scoring objektif + ML clustering + anomaly detection + GIS spatial analysis dalam satu pipeline terintegrasi. Sistem ini mengisi gap tersebut secara end-to-end.

---

## 3. Penerapan Arsitektur Medallion & Tata Kelola Data

Sistem menerapkan arsitektur **Medallion (Bronze → Silver → Gold)** secara eksplisit untuk menjamin kualitas data bertingkat:

### 3.1 Bronze Layer — Raw Data (Single Source of Truth)

**Tujuan:** Menyimpan data mentah persis seperti diterima dari sumber, tanpa transformasi apapun.

| Aspek | Detail |
|---|---|
| **Sumber data** | 4 CSV dari Satudata Jakarta (2025) dan BPS DKI Jakarta (2022, 2024) |
| **Path HDFS** | `/data/bronze/{gizi,faskes,nakes,populasi}/` |
| **Format** | Apache Parquet, kompresi Snappy |
| **Skema** | Mengikuti skema asli CSV + metadata ingestion (`ingested_at`, `source`, `ingested_batch`) |
| **Sanitasi** | `sanitize_record()`: NaN/Infinity → `None` sebelum write Parquet |

![Bronze Layer](documentation/bronze.png)
![Bronze Directory](documentation/data_bronze.png)

### 3.2 Silver Layer — Cleaned & Structured

**Tujuan:** Membersihkan, menstandarisasi, dan menormalkan data dari semua sumber agar siap digabungkan.

**Transformasi yang dilakukan:**

| Masalah di Bronze | Solusi di Silver | Implementasi |
|---|---|---|
| Nama wilayah tidak konsisten (17 varian: "KOTA ADM. JAKARTA PUSAT" vs "Jakarta Pusat" vs "KOTA JAKARTA PUSAT") | Standarisasi ke 6 nama uppercase via mapping dictionary | `WILAYAH_MAP` (17 entries) + `F.coalesce()` fallback |
| Granularitas berbeda (gizi & nakes per kecamatan, faskes & populasi per kabupaten/kota) | Agregasi gizi & nakes ke level kabupaten/kota dengan `SUM`/`COUNT` | `groupBy("wilayah_std").pivot("kategori_std").agg(F.sum("jumlah"))` |
| Nilai kosong menggunakan em dash `–` (faskes) | Konversi `–` → `"0"` → cast Integer | `F.when(trim == "–", "0")` |
| Baris agregat "DKI Jakarta" dan metadata CSV | Filter baris non-data | `startswith()` + `isin(["DKI JAKARTA", "CATATAN"])` |
| Satuan populasi (ribuan jiwa) | Konversi ke jiwa absolut | `population_thousand × 1000` → Integer |
| NaN / Infinity di record numerik | Sanitasi → `None` | `sanitize_record()` |
| Nilai null / negatif | Filter dan fill | `isNotNull() & >= 0`, `na.fill(0)` |
| Duplikat record | Deduplicate berdasarkan composite key | `dropDuplicates([wilayah, periode, kategori])` |

| Aspek | Detail |
|---|---|
| **Path HDFS** | `/data/silver/{gizi_agregat, gizi_kecamatan_clean, faskes_clean, nakes_agregat, nakes_kecamatan_clean, populasi_clean}/` |
| **Format** | Apache Parquet, kompresi Snappy |
| **Partisi** | `partitionBy("kabupaten_kota")` — mempercepat query pemfilteran wilayah tertentu melalui *predicate pushdown* |

**Justifikasi format Parquet + partisi:**
- **Parquet (columnar storage):** Mempercepat query analitik yang hanya membaca kolom tertentu (mis. hanya NRS dan cluster) tanpa scan seluruh row. Kompresi Snappy menghemat storage HDFS 60-80% dibanding CSV.
- **Partisi per `kabupaten_kota`:** Spark hanya membaca folder partisi yang relevan saat filter wilayah diterapkan — mengurangi I/O HDFS secara signifikan. Dipilih `kabupaten_kota` (6 nilai unik) karena cardinality rendah = jumlah file terkendali.

![Silver Layer](documentation/silver.png)
![Silver Directory](documentation/data_silver.png)

### 3.3 Gold Layer — Aggregated, Scored & Analysis-Ready

**Tujuan:** Output final yang siap dikonsumsi oleh serving layer (dashboard) dan analisis ML.

**Transformasi yang dilakukan di Gold:**

1. **Penggabungan 4 tabel Silver** berdasarkan kolom `kabupaten_kota`:
   - `populasi_clean` (base table) LEFT JOIN `gizi_agregat` LEFT JOIN `faskes_clean` LEFT JOIN `nakes_agregat`

2. **Perhitungan Rasio Indikator:**
   ```
   rasio_faskes_per_10k_balita    = (RS_umum + RS_khusus) / (balita_gizi_buruk / 10000)
   rasio_posyandu_per_10k_populasi = posyandu / (populasi / 10000)
   rasio_nakes_per_10k_populasi    = nakes / (populasi / 10000)
   prevalensi_stunting_pct         = (stunting / populasi) × 100
   ```

3. **Nutrition Coverage Index (NCI):**
   ```
   NCI = (norm_faskes + norm_posyandu + norm_nakes) / 3
   ```
   Rata-rata tertimbang dari rasio-rasio faskes dan nakes yang sudah dinormalisasi. Range 0–1, semakin tinggi = semakin baik cakupan layanan.

4. **Normalisasi Min-Max (Spark Window function):**
   ```python
   def min_max_norm(col_name):
       w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
       min_val = F.min(col).over(w)
       max_val = F.max(col).over(w)
       return (col - min_val) / (max_val - min_val)
   ```
   Setiap indikator dinormalisasi ke rentang [0, 1] secara global across semua wilayah.

5. **Nutrition Risk Score (NRS):**
   ```
   NRS = (0.5 × norm_stunting) + (0.3 × norm_inverse_faskes) + (0.2 × norm_inverse_nakes)
   ```
   Di mana `norm_inverse_*` = `1 - min_max_norm(rasio_*)` (inverse karena rasio tinggi = risiko rendah).
   NRS range 0–1, **semakin tinggi = semakin berisiko**.
   Bobot `w1=0.5, w2=0.3, w3=0.2` (dapat di-tune sesuai kebijakan).

6. **Priority Ranking:** `RANK()` berdasarkan NRS descending (rank 1 = paling prioritas).

| Aspek | Detail |
|---|---|
| **Path HDFS** | `/data/gold/wilayah_risk_score/` (pre-ML), `/data/gold/wilayah_final/` (post-ML) |
| **Format** | Apache Parquet, kompresi Snappy, `coalesce(1)` untuk single-file output |
| **Export lokal** | `output/wilayah_final.json` + `output/wilayah_final.parquet` |

Data yang tersimpan pada Gold Layer:
![Gold Directory](documentation/data_gold.png)

#### Kontrak Data — `output/wilayah_final.json`

Array of record (1 baris per kabupaten/kota), dikonsumsi langsung oleh dashboard:

| Field | Tipe | Keterangan |
| :--- | :--- | :--- |
| `kabupaten_kota` | string | Nama wilayah standar (UPPER) |
| `populasi` | int | Populasi total wilayah |
| `total_balita_gizi_buruk` | int | Jumlah balita bermasalah gizi (semua kategori) |
| `total_stunting` | int | Jumlah balita stunting |
| `total_gizi_buruk` | int | Breakdown: kategori gizi buruk |
| `total_gizi_kurang` | int | Breakdown: kategori gizi kurang |
| `total_underweight` | int | Breakdown: kategori underweight |
| `jumlah_rs_umum` | int | Jumlah RS Umum |
| `jumlah_rs_khusus` | int | Jumlah RS Khusus |
| `jumlah_posyandu` | int | Jumlah Posyandu |
| `total_nakes` | int | Total tenaga kesehatan |
| `rasio_faskes_per_10k_balita` | float | RS umum+khusus per 10k balita gizi buruk |
| `rasio_posyandu_per_10k_populasi` | float | Posyandu per 10k penduduk |
| `rasio_nakes_per_10k_populasi` | float | Tenaga kesehatan per 10k penduduk |
| `prevalensi_stunting_pct` | float | Stunting / populasi total × 100 (lihat catatan*) |
| `nutrition_coverage_index` | float | NCI (0–1), makin tinggi makin baik cakupan |
| `nutrition_risk_score` | float | NRS (0–1), makin tinggi makin berisiko |
| `priority_rank` | int | Peringkat prioritas (1 = paling prioritas) |
| `cluster_id` | int | ID cluster K-Means |
| `cluster_label` | string | "Risiko Rendah/Sedang/Tinggi" |
| `is_anomaly` | bool | Hasil Isolation Forest |
| `anomaly_score` | float | Skor anomali (makin negatif makin anomali) |
| `last_updated` | string | Timestamp ISO build Gold |

> *Catatan kualitas data:* `prevalensi_stunting_pct` memakai **populasi total** wilayah sebagai penyebut (data populasi hanya tersedia per kabupaten/kota, bukan populasi balita). Karena itu nilainya bukan prevalensi balita sebenarnya, namun **tetap valid untuk pemeringkatan relatif antarwilayah** (transformasi monotonik — urutan tidak berubah).

---

## 4. Analisis Lanjutan (Advanced Analytics) & Evaluasi Model

Sistem menerapkan **3 teknik analisis lanjutan** untuk mengidentifikasi wilayah prioritas secara objektif:

### 4.1 K-Means Clustering — Pengelompokan Zona Risiko Wilayah (PySpark MLlib)

**Deskripsi:** Algoritma K-Means dari **PySpark MLlib** digunakan untuk mengelompokkan 6 kabupaten/kota di DKI Jakarta menjadi 3 cluster tingkat kerentanan: *Risiko Rendah*, *Risiko Sedang*, dan *Risiko Tinggi*.

**Fitur yang digunakan (5 dimensi):**
| Fitur | Peran |
|---|---|
| `prevalensi_stunting_pct` | Beban gizi wilayah |
| `rasio_faskes_per_10k_balita` | Ketersediaan faskes relatif |
| `rasio_posyandu_per_10k_populasi` | Ketersediaan posyandu relatif |
| `rasio_nakes_per_10k_populasi` | Ketersediaan nakes relatif |
| `nutrition_coverage_index` | Indeks komposit cakupan layanan |

**Preprocessing:**
- **Median imputation** untuk null values (data 6 wilayah — drop baris akan membuang informasi berlebihan)
- **StandardScaler** (`withMean=True, withStd=True`) untuk normalisasi fitur agar K-Means tidak bias terhadap skala

**Metode Validasi — Elbow Method + Silhouette Score:**

Jumlah kluster optimal ($K$) ditentukan menggunakan **Elbow Method** pada WSSE (*Within Set Sum of Squared Errors*) dan dievaluasi secara statistik menggunakan **Silhouette Score**:

$$WSSE = \sum_{i=1}^k \sum_{x \in C_i} ||x - \mu_i||^2$$

| K | WSSE | Silhouette Score |
|---|---|---|
| 2 | ~12.5 | ~0.60 |
| **3** | **~4.8** | **~0.72** |
| 4 | ~2.1 | ~0.55 |

**K=3 dipilih** karena:
1. WSSE turun drastis dari K=2 → K=3 (elbow point)
2. Silhouette Score tertinggi di K=3 (>0.5 = clustering quality baik)
3. Interpretasi domain: 3 level risiko (Rendah, Sedang, Tinggi) sesuai konteks kebijakan kesehatan

**Metrik evaluasi:**
- **Silhouette Score** = mengukur seberapa mirip data dengan cluster-nya sendiri vs cluster tetangga. Range [-1, 1], >0.5 = baik, >0.7 = sangat baik.
- **WSSE** = total jarak kuadrat setiap data point ke centroid cluster-nya.

**Pelabelan cluster DINAMIS:**
Label cluster ("Risiko Rendah/Sedang/Tinggi") ditetapkan otomatis dengan mengurutkan rata-rata NRS tiap cluster — **bukan pemetaan manual** — agar konsisten meski ID cluster berubah antar-run:
```python
order = df_clustered.groupBy("cluster_id") \
    .agg(F.mean("nutrition_risk_score").alias("avg_nrs")) \
    .orderBy("avg_nrs")
label_map = {row["cluster_id"]: CLUSTER_LABELS[i] for i, row in enumerate(order)}
```

![K-Means Clustering](documentation/analisis1.png)

**Hasil pemetaan cluster:**

| Cluster | Label | NRS Range | Wilayah |
| --- | --- | --- | --- |
| Cluster 2 | Risiko Rendah | 0.13 – 0.17 | Jakarta Pusat, Jakarta Selatan |
| Cluster 0 | Risiko Sedang | 0.47 – 0.64 | Jakarta Barat, Jakarta Utara, Jakarta Timur |
| Cluster 1 | Risiko Tinggi | 0.9171 | Kepulauan Seribu |

**Output terukur:** `cluster_id` (integer 0/1/2), `cluster_label` (string kategori risiko) per wilayah.

### 4.2 Isolation Forest — Anomaly Detection (scikit-learn)

**Deskripsi:** Mendeteksi wilayah dengan pola tidak wajar — misalnya angka stunting sangat tinggi *sekaligus* rasio faskes juga sangat rendah (*double anomali* yang memerlukan intervensi segera).

**Fitur yang digunakan (5 dimensi):**

| Fitur | Peran |
|---|---|
| `prevalensi_stunting_pct` | Beban gizi wilayah |
| `rasio_faskes_per_10k_balita` | Ketersediaan faskes |
| `rasio_posyandu_per_10k_populasi` | Ketersediaan posyandu |
| `rasio_nakes_per_10k_populasi` | Ketersediaan nakes |
| `nutrition_risk_score` | Sinyal risiko gabungan (NRS) |

**Hyperparameter:**
- `n_estimators=100` — jumlah pohon isolasi
- `contamination=0.15` — proporsi anomali yang diharapkan
- `random_state=42` — reproduksibilitas

**Preprocessing:** `StandardScaler` (sklearn) + median imputation untuk null values.

**Evaluasi (tanpa ground-truth label):**

Karena tidak ada label ground-truth untuk anomali wilayah (unsupervised problem), evaluasi dilakukan melalui 2 pendekatan:

1. **Robustness Check** — menjalankan Isolation Forest dengan beberapa nilai contamination dan memverifikasi konsistensi wilayah anomali:

   | Contamination | Wilayah Anomali |
   |---|---|
   | 0.10 | KEPULAUAN SERIBU |
   | 0.15 | KEPULAUAN SERIBU |
   | 0.20 | KEPULAUAN SERIBU |

   **Kepulauan Seribu konsisten terdeteksi sebagai anomali** di semua nilai contamination.

2. **Domain Validation** — memverifikasi bahwa wilayah anomali memang memiliki kombinasi atypical secara domain:
   - Kepulauan Seribu: NRS tertinggi (0.9171), populasi terendah (28.800), posyandu terendah (37), nakes terendah (172)
   - Profil ini konsisten dengan interpretasi domain: wilayah kepulauan dengan akses layanan kesehatan sangat terbatas

**Output terukur:**
- `is_anomaly` (boolean): flag anomali per wilayah
- `anomaly_score` (float): skor kontinu, semakin negatif = semakin anomali

![Isolation Forest](documentation/analisis2.png)

### 4.3 GIS Spatial Analysis — Clustering Spasial (GeoPandas + Folium)

**Deskripsi:** Menggabungkan hasil NRS, cluster, dan anomaly detection dengan batas wilayah administratif DKI Jakarta untuk menghasilkan analisis spasial dan visualisasi peta.

**Output terukur:**

| Output | Format | Deskripsi |
|---|---|---|
| `output/peta_nrs.png` | PNG 150 DPI | Choropleth Nutrition Risk Score (RdYlGn_r colormap) |
| `output/peta_cluster.png` | PNG 150 DPI | Peta cluster risiko (3 warna: hijau/kuning/merah) |
| `output/peta_interaktif.html` | HTML (Folium) | Peta interaktif dengan tooltip (NRS, kategori, anomali, prevalensi, prioritas) |

**Fitur spasial:**
- **Choropleth mapping:** Warna wilayah proporsional dengan NRS (hijau = aman, merah = kritis)
- **Smart label placement:** `representative_point()` menjamin label wilayah jatuh di dalam poligon (bukan di laut untuk Kepulauan Seribu)
- **Auto-detect nama wilayah:** `detect_name_field()` mencocokkan kolom shapefile dengan 6 wilayah DKI secara otomatis
- **CRS normalisasi:** Otomatis reproject ke EPSG:4326 jika CRS shapefile berbeda
- **Graceful degradation:** Jika shapefile belum tersedia, script mencetak instruksi dan keluar tanpa error (pipeline tetap berjalan)

### Ringkasan Teknik Advanced Analytics

| # | Teknik | Library | Output Terukur | Evaluasi |
|---|---|---|---|---|
| 1 | **K-Means Clustering** | PySpark MLlib | `cluster_id`, `cluster_label` (3 level risiko) | Silhouette Score ~0.72, WSSE ~4.8, Elbow Method |
| 2 | **Isolation Forest (Anomaly Detection)** | scikit-learn | `is_anomaly` (bool), `anomaly_score` (float) | Robustness check 3 contamination + domain validation |
| 3 | **GIS Spatial Analysis** | GeoPandas + Folium | 3 peta (choropleth NRS, cluster, interaktif) | Visual inspection + auto-detect name field scoring |

![Hasil Akhir](documentation/analisis_akhir.png)

---

## 5. Sistem Berjalan End-to-End & Penanganan Error

### 5.1 Bukti Pipeline Aktif (Bukan Mock/Dummy)

Sistem ini berjalan **end-to-end** dengan pipeline data aktif. Berikut bukti dari data output final (`output/wilayah_final.json`) yang dihasilkan oleh pipeline:

| Wilayah | Populasi | Balita Gizi Buruk | Stunting | Posyandu | Nakes | NRS | Cluster | Anomali |
|---|---|---|---|---|---|---|---|---|
| KEPULAUAN SERIBU | 28.800 | 707 | 101 | 37 | 172 | **0.9171** | Risiko Tinggi | ✅ Ya |
| JAKARTA BARAT | 2.479.600 | 13.958 | 5.016 | 855 | 1.258 | **0.6368** | Risiko Sedang | ❌ |
| JAKARTA UTARA | 1.815.600 | 7.974 | 4.080 | 649 | 8.615 | **0.5747** | Risiko Sedang | ❌ |
| JAKARTA TIMUR | 3.086.000 | 11.606 | 5.485 | 1.179 | 8.702 | **0.4788** | Risiko Sedang | ❌ |
| JAKARTA PUSAT | 1.044.300 | 4.827 | 1.876 | 489 | 14.303 | **0.1719** | Risiko Rendah | ❌ |
| JAKARTA SELATAN | 2.230.700 | 4.981 | 3.033 | 1.263 | 11.301 | **0.1309** | Risiko Rendah | ❌ |

- **Data real:** Seluruh angka berasal dari sumber resmi (Satudata Jakarta 2025, BPS 2022/2024)
- **Bukan dummy:** Setiap record memiliki rasio yang dihitung, NCI/NRS yang dinormalisasi, cluster yang di-assign ML, dan anomaly score kontinu
- **Konsisten:** Kepulauan Seribu secara konsisten muncul sebagai wilayah paling berisiko dan satu-satunya anomali — sesuai interpretasi domain (wilayah kepulauan terisolasi)

### 5.2 Error Handling & Fallback di Setiap Layer

#### Layer 1: Ingestion (Kafka Producer + Consumer)

| Mekanisme | Implementasi | File |
|---|---|---|
| **CSV Fallback** | Producer membaca CSV lokal jika API tidak tersedia — pipeline tidak berhenti | `producer.py` |
| **Kafka retry** | `retries=3`, `retry_backoff_ms=1000` — atasi koneksi intermiten | `producer.py` |
| **Offset safety** | `enable_auto_commit=False` — commit HANYA setelah HDFS write sukses. Gagal → offset tidak di-commit → bisa re-run tanpa kehilangan data | `consumer_to_hdfs.py` |
| **HDFS cleanup** | `rm -r -f` sebelum write — cegah duplikat dari re-run consumer | `consumer_to_hdfs.py` |
| **NaN/Infinity sanitasi** | `sanitize_record()` mengkonversi NaN/Infinity → `None` sebelum write Parquet | `consumer_to_hdfs.py` |
| **Empty topic handling** | Skip write jika tidak ada record di topik tertentu | `consumer_to_hdfs.py` |

#### Layer 2: Bronze → Silver (PySpark ETL)

| Masalah | Penanganan | Detail |
|---|---|---|
| Wilayah tidak konsisten (17 varian) | Mapping dictionary → seragam; `coalesce()` fallback untuk varian tak dikenal | `WILAYAH_MAP` (17 entries) |
| `–` (em dash) di kolom faskes | `F.when(trim == "–", "0")` → cast Integer | `clean_faskes()` |
| Baris agregat "DKI Jakarta" | Difilter di faskes & populasi | `isin(["DKI JAKARTA"])` |
| Metadata CSV (Catatan, Hasil, Laju) | Difilter `startswith()` + `isin()` | `clean_populasi()` |
| NaN / Infinity di record | `sanitize_record()` → `None` sebelum Parquet | Shared utility |
| Kolom dengan titik (`.`) di nama | Backtick-quote `` `nama_kolom` `` | PySpark convention |
| NULL / negatif | `isNotNull() & >= 0` | Filter per dataset |
| Duplikat | `dropDuplicates()` composite key | Per dataset |
| Populasi ribuan → jiwa | `× 1000` → Integer | `clean_populasi()` |

#### Layer 3: Silver → Gold (Aggregation + Scoring)

| Masalah | Penanganan |
|---|---|
| **NULL setelah LEFT JOIN** | `na.fill(0)` untuk semua kolom numerik (faskes, nakes, gizi) |
| **Division by zero** (balita = 0, populasi = 0) | `F.when(col > 0, formula).otherwise(None)` — menghasilkan NULL yang aman |
| **Min = Max di normalisasi** (semua wilayah identik) | `F.when(max_val == min_val, 0.0).otherwise(formula)` — mencegah ZeroDivisionError |

#### Layer 4: ML Analysis

| Masalah | Penanganan |
|---|---|
| **Gold kosong** (silver_to_gold belum dijalankan) | Check `df_gold.count() == 0` → print error message + `spark.stop()` + early return |
| **NULL di fitur ML** | `impute_median()` — isi null dengan median Spark-native (`approxQuantile`), bukan drop baris |
| **pandas 2.x + numpy 1.24 breaking changes** | Compatibility shim: `pd.DataFrame.iteritems = pd.DataFrame.items` + restore `np.bool`, `np.object`, dll |
| **Timestamp serialization** | `date_format(col, "yyyy-MM-dd'T'HH:mm:ss")` → string ISO sebelum `toPandas()` |
| **HDFS upload failure (model)** | `upload_to_hdfs()` returns bool, print `[WARN]` jika gagal — pipeline tidak berhenti |

#### Layer 5: GIS Visualization

| Masalah | Penanganan |
|---|---|
| **Shapefile belum tersedia** | `print_shapefile_instructions()` + `sys.exit(0)` — keluar **tanpa error**, pipeline tidak berhenti |
| **Gold final belum ada** | Check `os.path.exists(GOLD_LOCAL)` → print error + `sys.exit(1)` |
| **Fiona version conflict** | Baca GeoJSON via `json.load()` + `GeoDataFrame.from_features()` — bypass fiona dependency |
| **CRS berbeda (bukan EPSG:4326)** | Auto-reproject: `gdf.to_crs(epsg=4326)` |
| **Nama wilayah shapefile tidak cocok** | `detect_name_field()` + `normalize_name()` — auto-detect field terbaik + strip prefix administratif |
| **Label jatuh di luar poligon (Kepulauan Seribu)** | `representative_point()` — dijamin di dalam poligon |
| **numpy types gagal JSON serialize (Folium)** | Konversi eksplisit ke Python native types (`float()`, `int()`, `str()`) sebelum tooltip |

#### Layer 6: Dashboard (Flask)

| Masalah | Penanganan |
|---|---|
| **Output file belum ada** | `load_data()` akan raise FileNotFoundError → Flask mengembalikan 500 (visible error) |
| **X-Frame-Options blocking iframe** | `response.headers["X-Frame-Options"] = "ALLOWALL"` untuk peta interaktif |

---

## 6. Relevansi Smart City

Sistem ini selaras dengan agenda **Smart City** Pemprov DKI Jakarta pada domain *Smart Society/Smart Governance*: pemerataan layanan kesehatan balita berbasis data. Output berupa *priority ranking* dan peta risiko per wilayah dapat langsung dipakai sebagai dasar **kebijakan kota** untuk:

1. **Realokasi tenaga medis** ke wilayah Risiko Tinggi (Kepulauan Seribu: hanya 172 nakes untuk 28.800 jiwa)
2. **Penambahan posyandu** di wilayah dengan rasio rendah (Jakarta Barat: 3.45 per 10k vs Jakarta Selatan: 5.66 per 10k)
3. **Intervensi gizi tertarget** berdasarkan cluster ML dan anomaly flag
4. **Monitoring berkala** — pipeline dapat di-rerun setiap ada update data baru dari Satudata/BPS

Sistem ini berpotensi diadopsi nyata ke dalam ekosistem data terpadu (Satudata) Jakarta.

---

## Command Documentation

### Start & Stop Service
```bash
docker-compose up -d --build
docker-compose down
```

> Nama container aplikasi: `final-project-big-data`. Semua job Spark berjalan
> dalam mode `local[*]` di dalam container ini (bukan cluster Spark terpisah).

### 1. Ingestion — Kafka topics + Producer + Consumer ke Bronze (HDFS)
```bash
docker exec -it final-project-big-data python3 src/ingestion/setup_topics.py
docker exec -it final-project-big-data python3 src/ingestion/producer.py
docker exec -it final-project-big-data python3 src/ingestion/consumer_to_hdfs.py
```

### 2. Bronze → Silver (PySpark ETL)
```bash
docker exec -it final-project-big-data python3 src/processing/bronze_to_silver.py
```

### 3. Silver → Gold (Aggregation + NCI/NRS Scoring)
```bash
docker exec -it final-project-big-data python3 src/processing/silver_to_gold.py
```

### 4. ML Analysis (K-Means + Isolation Forest) → Gold final + export lokal
```bash
docker exec -it final-project-big-data python3 src/analysis/ml_analysis.py
```

### 5. GIS Visualization (butuh shapefile di data/shapefiles/)
```bash
docker exec -it final-project-big-data python3 src/visualization/gis_map.py
```

### 6. Dashboard — otomatis berjalan di port 5000
```
Buka browser → http://localhost:5000
```

### Remove All Data
```bash
docker-compose down -v
docker volume rm (namavolume)
```

---

# Dokumentasi Step-by-Step

## 1. Data Ingestion
Data berasal dari 4 sumber resmi:
- [Data Stunting Berdasarkan Wilayah (Satudata Jakarta 2025)](https://satudata.jakarta.go.id/open-data/detail?kategori=dataset&page_url=jumlah-anak-bawah-lima-tahun-balita-bermasalah-gizi-berdasarkan-wilayah&data_no=6)
- [Jumlah Fasilitas Kesehatan (BPS DKI Jakarta 2022)](https://jakarta.bps.go.id/id/statistics-table/3/YmllemNGUkNVblZLVVhOblJEWnZXbkEzWld0eVVUMDkjMw==/jumlah-rumah-sakit-umum--rumah-sakit-khusus--puskesmas--klinik-pratama--dan-posyandu-menurut-kabupaten-kota-di-provinsi-dki-jakarta--2019.html?year=2022)
- [Jumlah Tenaga Kesehatan DKI Jakarta (Satudata 2019)](https://satudata.jakarta.go.id/open-data/detail?kategori=dataset&page_url=data-jumlah-tenaga-kesehatan-menurut-kecamatan-provinsi-dki-jakarta&data_no=1)
- [Populasi DKI Jakarta (BPS 2024)](https://jakarta.bps.go.id/en/statistics-table/3/V1ZSbFRUY3lTbFpEYTNsVWNGcDZjek53YkhsNFFUMDkjMw==/penduduk--laju-pertumbuhan-penduduk--distribusi-persentase-penduduk--kepadatan-penduduk--rasio-jenis-kelamin-penduduk-menurut-kabupaten-kota-di-provinsi-dki-jakarta--2024.html?year=2024)

Data tersebut dikumpulkan pada [data/fallback](data/fallback) sebagai CSV fallback.

Proses ingestion: CSV → Kafka Producer (4 topik) → Kafka Consumer → HDFS Bronze (Parquet + Snappy).

![Ingest Data](documentation/ingest.png)

## 2. Bronze Layer
Setelah data ingestion, dataset kemudian disimpan ke HDFS (Hadoop Distributed File System) berupa data raw file .parquet dan siap untuk diproses ke layer berikutnya.

**Tujuan:** Menyimpan data mentah persis seperti diterima dari sumber, tanpa transformasi apapun.

![Bronze Layer](documentation/bronze.png)

Berikut adalah data-data yang sudah tersimpan di HDFS:

![Bronze Directory](documentation/data_bronze.png)

## 3. Silver Layer
Dalam layer ini, dilakukan data cleaning dan standarisasi.

**Tujuan:** Membersihkan, menstandarisasi, dan menormalkan data dari semua sumber agar siap digabungkan.

**Transformasi yang dilakukan di Silver:**

| Masalah | Solusi |
|---|---|
| Nama wilayah tidak konsisten ("JAKARTA PUSAT" vs "KOTA ADM. JAKARTA PUSAT" vs "KOTA JAKARTA PUSAT") | Standarisasi ke uppercase + mapping dictionary (17 varian → 6 nama resmi) |
| Granularitas berbeda (gizi & nakes per kecamatan, faskes & populasi per kabupaten/kota) | Agregasi gizi & nakes ke level kabupaten/kota dengan SUM/COUNT |
| Periode data berbeda antar dataset | Tambah kolom `periode_label` yang distandarisasi |
| Nilai null/missing dan em dash `–` | Fill 0 (faskes) atau filter (gizi/nakes); konversi `–` → `"0"` |
| Duplikat record | Deduplicate berdasarkan composite key (wilayah + periode + kategori) |
| Satuan populasi (ribuan jiwa) | Konversi ke jiwa absolut (`population_thousand × 1000`) |

![Silver Layer](documentation/silver.png)

Hasil data yang tersimpan pada silver layer:

![Silver Directory](documentation/data_silver.png)

## 4. Gold Layer
**Tujuan:** Output final yang siap dikonsumsi oleh serving layer dan analisis ML.

**Transformasi yang dilakukan di Gold:**

1. **Penggabungan 4 tabel silver** berdasarkan kolom `kabupaten_kota` - `populasi_clean`, `gizi_agregat`, `faskes_clean`, dan `nakes_agregat`

2. **Perhitungan Rasio Indikator:**
   - `rasio_faskes_per_10k_balita` = (total_rs_umum + total_rs_khusus) / (total_balita_gizi_buruk / 10000)
   - `rasio_posyandu_per_10k_populasi` = total_posyandu / (populasi / 10000)
   - `rasio_nakes_per_10k_populasi` = total_nakes / (populasi / 10000)
   - `prevalensi_stunting_pct` = (total_stunting / populasi) × 100
   - `nutrition_coverage_index (NCI)` = rata-rata tertimbang dari rasio-rasio faskes dan nakes

3. **Normalisasi:**
    - Setiap indikator dinormalisasi ke rentang [0, 1] menggunakan min-max normalisasi via Spark Window function (global across semua wilayah).

4. **Nutrition Risk Score (NRS):**
   ```
   NRS = (w1 × norm_stunting) + (w2 × norm_inverse_faskes) + (w3 × norm_inverse_nakes)
   ```
   Di mana `norm_*` adalah min-max normalization (0–1) dan `w1=0.5, w2=0.3, w3=0.2` (dapat di-tune).
   NRS range 0–1, semakin tinggi = semakin berisiko.

Data yang tersimpan pada Gold Layer:

![Gold Directory](documentation/data_gold.png)

## 5. ML Analysis

### 5.1 K-Means Clustering (PySpark MLlib)

**Tujuan:** Mengelompokkan 6 kabupaten/kota DKI Jakarta ke dalam kategori risiko berdasarkan gabungan fitur gizi dan ketersediaan layanan.

**Hasil K-Means Clustering** (kolom tambahan):
   - `cluster_id` : integer (0, 1, 2)
   - `cluster_label` : string ("Risiko Rendah", "Risiko Sedang", "Risiko Tinggi")

![K-Means Clustering](documentation/analisis1.png)

K=3 dipilih berdasarkan Elbow Method, karena WSSE turun drastis dari K=2 → K=3. **Pelabelan cluster** dilakukan secara dinamis berdasarkan rata-rata NRS tiap cluster (bukan pelabelan manual).

**Hasil pemetaan cluster:**
| Cluster | Label | NRS | Wilayah |
| --- | --- | --- | --- |
| Cluster 2 | Risiko Rendah | 0.13 – 0.17 | Jakarta Pusat, Jakarta Selatan |
| Cluster 0 | Risiko Sedang | 0.47 – 0.64 | Jakarta Barat, Jakarta Utara, Jakarta Timur |
| Cluster 1 | Risiko Tinggi | 0.9171 | Kepulauan Seribu |

### 5.2 Isolation Forest — Anomaly Detection (scikit-learn)

**Tujuan:** Mendeteksi wilayah yang memiliki pola tidak wajar — misalnya angka stunting sangat tinggi tapi rasio faskes juga sangat rendah (double anomali yang memerlukan intervensi segera).

**Hasil Isolation Forest** (kolom tambahan):
   - `is_anomaly` : boolean
   - `anomaly_score` : float (semakin negatif = semakin anomali)

![Isolation Forest](documentation/analisis2.png)
![Hasil Akhir](documentation/analisis_akhir.png)

Kepulauan Seribu konsisten terdeteksi sebagai anomali di semua nilai contamination (0.10, 0.15, 0.20), mengindikasikan profil risiko yang sangat berbeda dari lima wilayah Jakarta lainnya. Kemungkinan besar karena keterbatasan akses layanan kesehatan akibat kondisi wilayah geografis kepulauan.

## 6. Dashboard

Setelah melakukan langkah-langkah pada [Command Documentation](#command-documentation), dashboard akan muncul pada `localhost:5000`.

Dashboard menampilkan:
- **Hero section** dengan summary statistik (jumlah wilayah, rata-rata risk score, total posyandu, total nakes)
- **Peta interaktif Folium** (embedded iframe) dengan tooltip per wilayah
- **Peta statis** choropleth NRS dan cluster distribution
- **Ranking wilayah prioritas** intervensi dengan indikator visual (risk score, coverage %, posyandu count, anomaly flag)
- **Strategic insights** yang di-generate dari data (prioritas intervensi, coverage tertinggi, audit distribusi)