# Implementation Guide: Sistem Audit Ketimpangan Distribusi Fasilitas Kesehatan Balita DKI Jakarta
## Big Data Final Project — Data Lakehouse Architecture

---

## Daftar Isi
1. [Latar Belakang & Justifikasi (Rubrik 1)](#1-latar-belakang--justifikasi)
2. [Arsitektur Sistem & Justifikasi Teknologi (Rubrik 2)](#2-arsitektur-sistem--justifikasi-teknologi)
3. [Medallion Architecture — Layer Detail (Rubrik 3)](#3-medallion-architecture--layer-detail)
4. [Analisis Lanjutan (Rubrik 4)](#4-analisis-lanjutan)
5. [Keunikan & Analisis Kompetitor (Rubrik 5)](#5-keunikan--analisis-kompetitor)
6. [Pipeline Developer Guide — Step by Step (Rubrik 6)](#6-pipeline-developer-guide--step-by-step)
7. [Struktur Direktori Proyek](#7-struktur-direktori-proyek)
8. [Error Handling & Fallback](#8-error-handling--fallback)
9. [Checklist Rubrik Penilaian](#9-checklist-rubrik-penilaian)

---

## 1. Latar Belakang & Justifikasi

### 1.1 Masalah Utama dengan Bukti Kuantitatif

Permasalahan gizi buruk pada balita di DKI Jakarta masih sulit diidentifikasi secara dini karena data tersebar di berbagai portal pemerintah tanpa integrasi otomatis. Berikut bukti kuantitatif pendukung:

| Sumber Data | Fakta Kuantitatif |
|---|---|
| Satudata Jakarta 2025 | Jumlah balita bermasalah gizi bervariasi signifikan antar kabupaten/kota |
| BPS DKI Jakarta 2024 | Persebaran puskesmas, RS, dan posyandu tidak merata antar wilayah |
| Data Nakes 2023–2025 | Rasio tenaga kesehatan per kecamatan tidak proporsional dengan beban kasus |

**Gap yang belum terselesaikan:** Tidak ada sistem yang secara otomatis mengintegrasikan data gizi, faskes, nakes, dan populasi untuk menghasilkan risk score per wilayah secara objektif dan terkini.

### 1.2 Justifikasi Big Data — Framework 5V

| Dimensi | Penjelasan dalam Konteks Proyek |
|---|---|
| **Volume** | Data mencakup ratusan kecamatan × multiple periode × multiple kategori gizi (stunting, wasting, underweight, overweight). Bila digabungkan dengan data nakes, faskes, dan populasi, total record bisa mencapai ratusan ribu baris per snapshot. |
| **Velocity** | Data bersumber dari API publik (Satudata Jakarta) yang diperbarui periodik. Kafka digunakan untuk menangani ingestion secara near-real-time saat ada pembaruan data baru dari endpoint API. |
| **Variety** | Data bersifat heterogen: data gizi per kecamatan (semi-structured JSON/CSV), data faskes per kabupaten/kota (tabular), data nakes per kecamatan (tabular), data populasi (tabular dengan satuan berbeda — ribuan jiwa). Membutuhkan normalisasi lintas sumber. |
| **Veracity** | Data dari portal pemerintah memiliki potensi inkonsistensi: periode data berbeda antar sumber (ada yang 2019, 2022, 2023, 2024, 2025), satuan berbeda, granularitas berbeda (ada per kecamatan, ada per kabupaten/kota). Pipeline Bronze→Silver dirancang untuk menangani masalah ini. |
| **Value** | Output akhir berupa Nutrition Risk Score (NRS) per wilayah dan hasil clustering yang dapat langsung digunakan oleh Dinas Kesehatan DKI Jakarta untuk prioritisasi intervensi. |

### 1.3 Analisis Gap terhadap Solusi yang Ada

| Sistem Existing | Keterbatasan |
|---|---|
| SIGA (Kemenkes) | Hanya menyimpan data gizi individu, tidak mengintegrasikan faskes dan nakes, tidak ada risk scoring wilayah |
| Sigizi Terpadu | Berbasis input manual posyandu, tidak ada pipeline otomatis, tidak ada analisis wilayah agregat |
| E-PPGBM | Pencatatan gizi per individu, bukan analisis ketimpangan distribusi fasilitas |
| Portal Satudata Jakarta | Hanya menyajikan data mentah, tidak ada analisis lintas dataset, tidak ada visualisasi risiko |

**Solusi yang ditawarkan proyek ini:** Pipeline terotomasi yang menghubungkan empat sumber data sekaligus, menghasilkan risk score objektif berbasis indikator kuantitatif, dan menampilkan peta risiko interaktif per wilayah — belum ada sistem publik yang melakukan ini secara terintegrasi.

---

## 2. Arsitektur Sistem & Justifikasi Teknologi

### 2.1 Diagram Arsitektur End-to-End

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                    │
│  [Satudata Jakarta API]  [BPS DKI API]  [CSV/Excel Fallback]            │
└──────────────┬──────────────────┬──────────────┬────────────────────────┘
               │                  │              │
               ▼                  ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                   │
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                  Apache Kafka                                     │  │
│   │  Topic: gizi-raw | Topic: faskes-raw | Topic: nakes-raw          │  │
│   │  Topic: populasi-raw | Topic: fallback-csv                       │  │
│   └──────────────────────────┬───────────────────────────────────────┘  │
└──────────────────────────────┼──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER — HDFS                              │
│                                                                          │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────┐   │
│   │   BRONZE     │   │    SILVER    │   │          GOLD            │   │
│   │  Raw Parquet │──▶│  Cleaned     │──▶│  Aggregated + Scored     │   │
│   │  partitioned │   │  Normalized  │   │  Risk Score + Clusters   │   │
│   └──────────────┘   └──────────────┘   └──────────────────────────┘   │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       PROCESSING LAYER                                   │
│                                                                          │
│   ┌───────────────────────────────────────────────────────────────┐     │
│   │                    Apache Spark (PySpark)                      │     │
│   │   Bronze→Silver ETL | Silver→Gold Aggregation                 │     │
│   │   K-Means Clustering (MLlib) | Isolation Forest (sklearn)     │     │
│   │   GIS / GeoPandas Spatial Join                                │     │
│   └───────────────────────────────────────────────────────────────┘     │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                    │
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────┐      │
│   │         Apache Superset / Custom Dashboard (Flask)            │      │
│   │   Choropleth Map | Risk Score Table | Cluster Visualization  │      │
│   │   Anomaly Alert Panel | Wilayah Comparison Chart             │      │
│   └──────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Justifikasi Teknis Setiap Teknologi

| Teknologi | Peran | Justifikasi Teknis |
|---|---|---|
| **Apache Kafka** | Ingestion Layer | Throughput tinggi (jutaan event/detik), decoupling antara producer (API fetcher) dan consumer (HDFS writer). Memungkinkan fallback CSV masuk melalui topic yang sama tanpa mengubah downstream pipeline. Fault-tolerant dengan replikasi partition. |
| **HDFS** | Storage Layer | Distributed storage yang cocok untuk file Parquet skala besar. Mendukung partisi direktori yang dioptimalkan untuk query Spark. Terintegrasi native dengan ekosistem Hadoop/Spark. |
| **Apache Spark (PySpark)** | Processing Layer | In-memory processing untuk transformasi data lintas sumber yang besar. MLlib menyediakan K-Means yang dapat di-scale. Lazy evaluation memungkinkan optimasi query secara otomatis via Catalyst Optimizer. |
| **GeoPandas** | Spatial Analysis | Library Python untuk join data tabular dengan shapefile wilayah Jakarta. Menghasilkan choropleth map berbasis GeoJSON tanpa perlu infrastruktur GIS server terpisah. |
| **scikit-learn** | Anomaly Detection | Isolation Forest dari scikit-learn lebih matang dan mudah di-tune dibanding implementasi MLlib. Karena data Gold layer sudah kecil (6 wilayah), tidak perlu distributed computing untuk tahap ini. |
| **Apache Superset / Flask** | Serving Layer | Superset mendukung koneksi langsung ke Parquet via SQL Lab. Alternatif Flask+Plotly untuk dashboard custom yang lebih fleksibel dengan kontrol penuh atas layout dan visualisasi peta. |

---

## 3. Medallion Architecture — Layer Detail

### 3.1 Bronze Layer — Raw Ingestion

**Tujuan:** Menyimpan data mentah persis seperti diterima dari sumber, tanpa transformasi apapun.

**Struktur Direktori HDFS:**
```
hdfs://namenode:9000/data/bronze/
├── gizi/
│   └── periode=2025/
│       ├── wilayah=jakarta_pusat/part-0000.parquet
│       ├── wilayah=jakarta_utara/part-0000.parquet
│       ├── wilayah=jakarta_selatan/part-0000.parquet
│       ├── wilayah=jakarta_timur/part-0000.parquet
│       ├── wilayah=jakarta_barat/part-0000.parquet
│       └── wilayah=kepulauan_seribu/part-0000.parquet
├── faskes/
│   └── tahun=2022/
│       └── provinsi=dki_jakarta/part-0000.parquet
├── nakes/
│   └── periode=2023/
│       ├── wilayah=jakarta_pusat/part-0000.parquet
│       └── ... (per wilayah)
└── populasi/
    └── tahun=2024/
        └── provinsi=dki_jakarta/part-0000.parquet
```

**Skema Bronze per Dataset:**

*Dataset 1 — Gizi Balita:*
```
periode_data    : string  (contoh: "2025")
wilayah         : string  (contoh: "JAKARTA PUSAT")
kecamatan       : string  (contoh: "GAMBIR")
kategori_masalah_gizi : string  (contoh: "STUNTING", "WASTING", "UNDERWEIGHT", "OVERWEIGHT")
jumlah          : integer
ingested_at     : timestamp  (ditambahkan saat ingestion)
source          : string  ("api" atau "csv_fallback")
```

*Dataset 2 — Fasilitas Kesehatan:*
```
kabupaten_kota      : string
jumlah_rs_umum      : integer
jumlah_rs_khusus    : integer
jumlah_posyandu     : integer
tahun               : string
ingested_at         : timestamp
source              : string
```

*Dataset 3 — Tenaga Kesehatan:*
```
periode_data        : string
wilayah             : string
kecamatan           : string
tenaga_kesehatan    : string  (jenis nakes)
jumlah              : integer
ingested_at         : timestamp
source              : string
```

*Dataset 4 — Populasi:*
```
kabupaten_kota              : string
population_thousand         : float
population_growth_rate      : float
percentage_of_total         : float
density_per_sqkm            : float
sex_ratio                   : float
tahun                       : string
ingested_at                 : timestamp
source                      : string
```

**Format:** Parquet (columnar, compressed snappy)
**Partisi:** per `periode_data` atau `tahun` + per `wilayah` atau `kabupaten_kota`
**Transformasi:** NONE — data ditulis apa adanya dari Kafka consumer

### 3.2 Silver Layer — Cleaned & Normalized

**Tujuan:** Membersihkan, menstandarisasi, dan menormalkan data dari semua sumber agar siap digabungkan.

**Transformasi yang dilakukan di Silver:**

| Masalah | Solusi |
|---|---|
| Nama wilayah tidak konsisten ("JAKARTA PUSAT" vs "Jakarta Pusat" vs "Jak-Pus") | Standarisasi ke uppercase + mapping dictionary |
| Granularitas berbeda (gizi & nakes per kecamatan, faskes & populasi per kabupaten/kota) | Agregasi gizi & nakes ke level kabupaten/kota dengan SUM/COUNT |
| Periode data berbeda antar dataset | Tambah kolom `periode_label` yang distandarisasi, flag data dengan `data_vintage` |
| Nilai null/missing | Impute dengan median per wilayah atau flag sebagai `NULL_FLAG=True` |
| Duplikat record | Deduplicate berdasarkan composite key (wilayah + periode + kategori) |
| Satuan populasi (ribuan jiwa) | Konversi ke jiwa absolut (`population_thousand × 1000`) |

**Skema Silver — Master Wilayah (hasil agregasi):**
```
kabupaten_kota          : string  (key utama, 6 nilai)
total_balita_gizi_buruk : integer (SUM dari semua kategori)
total_stunting          : integer
total_wasting           : integer
total_underweight       : integer
total_overweight        : integer
total_rs_umum           : integer
total_rs_khusus         : integer
total_posyandu          : integer
total_nakes             : integer
populasi                : integer (jiwa absolut)
periode_gizi            : string
periode_nakes           : string
tahun_faskes            : string
tahun_populasi          : string
data_completeness_score : float  (% kolom yang tidak null, untuk tracking kualitas data)
```

**Struktur HDFS Silver:**
```
hdfs://namenode:9000/data/silver/
├── master_wilayah/
│   └── wilayah=jakarta_pusat/part-0000.parquet
│   └── ... (per wilayah)
├── gizi_kecamatan_cleaned/  (tetap simpan level kecamatan untuk drill-down)
│   └── periode=2025/wilayah=jakarta_pusat/part-0000.parquet
└── nakes_kecamatan_cleaned/
    └── periode=2023/wilayah=jakarta_pusat/part-0000.parquet
```

### 3.3 Gold Layer — Aggregated, Scored & Analysis-Ready

**Tujuan:** Output final yang siap dikonsumsi oleh serving layer dan analisis ML.

**Transformasi yang dilakukan di Gold:**

1. **Perhitungan Rasio Indikator:**
   - `rasio_faskes_per_10k_balita` = (total_rs_umum + total_rs_khusus) / (total_balita_gizi_buruk / 10000)
   - `rasio_posyandu_per_10k_populasi` = total_posyandu / (populasi / 10000)
   - `rasio_nakes_per_10k_populasi` = total_nakes / (populasi / 10000)
   - `prevalensi_stunting_pct` = (total_stunting / populasi) × 100
   - `nutrition_coverage_index (NCI)` = rata-rata tertimbang dari rasio-rasio faskes dan nakes

2. **Nutrition Risk Score (NRS):**
   ```
   NRS = (w1 × norm_stunting) + (w2 × norm_inverse_faskes) + (w3 × norm_inverse_nakes)
   ```
   Di mana `norm_*` adalah min-max normalization (0–1) dan `w1=0.5, w2=0.3, w3=0.2` (dapat di-tune).
   NRS range 0–1, semakin tinggi = semakin berisiko.

3. **Hasil K-Means Clustering** (kolom tambahan):
   - `cluster_id` : integer (0, 1, 2)
   - `cluster_label` : string ("Risiko Rendah", "Risiko Sedang", "Risiko Tinggi")

4. **Hasil Isolation Forest** (kolom tambahan):
   - `is_anomaly` : boolean
   - `anomaly_score` : float (semakin negatif = semakin anomali)

**Skema Gold — Final Wilayah:**
```
kabupaten_kota                  : string
populasi                        : integer
total_balita_gizi_buruk         : integer
total_stunting                  : integer
rasio_faskes_per_10k_balita     : float
rasio_posyandu_per_10k_populasi : float
rasio_nakes_per_10k_populasi    : float
prevalensi_stunting_pct         : float
nutrition_coverage_index        : float
nutrition_risk_score            : float
cluster_id                      : integer
cluster_label                   : string
is_anomaly                      : boolean
anomaly_score                   : float
priority_rank                   : integer  (1 = paling prioritas)
last_updated                    : timestamp
```

**Struktur HDFS Gold:**
```
hdfs://namenode:9000/data/gold/
├── wilayah_risk_score/
│   └── part-0000.parquet  (flat, tidak perlu partisi — hanya 6 baris)
├── kecamatan_drill_down/
│   └── wilayah=jakarta_pusat/part-0000.parquet
└── model_artifacts/
    ├── kmeans_model/
    └── isolation_forest_model.pkl
```

---

## 4. Analisis Lanjutan

### 4.1 K-Means Clustering (PySpark MLlib)

**Tujuan:** Mengelompokkan 6 kabupaten/kota DKI Jakarta ke dalam kategori risiko berdasarkan gabungan fitur gizi dan ketersediaan layanan.

**Fitur Input:**
```python
feature_cols = [
    "prevalensi_stunting_pct",
    "rasio_faskes_per_10k_balita",
    "rasio_posyandu_per_10k_populasi",
    "rasio_nakes_per_10k_populasi",
    "nutrition_coverage_index"
]
```

**Implementasi:**
```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler

# Assembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_assembled = assembler.transform(df_gold)

# Scaling (wajib untuk K-Means)
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

# K-Means
# Karena hanya 6 wilayah, k=3 adalah pilihan paling masuk akal
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster_id")
kmeans_model = kmeans.fit(df_scaled)
df_clustered = kmeans_model.transform(df_scaled)

# Evaluasi — Silhouette Score
evaluator = ClusteringEvaluator(
    featuresCol="features",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)
silhouette = evaluator.evaluate(df_clustered)
print(f"Silhouette Score: {silhouette:.4f}")
# Target: Silhouette Score > 0.5 dianggap clustering yang baik

# Simpan model
kmeans_model.save("hdfs://namenode:9000/data/gold/model_artifacts/kmeans_model")
```

**Evaluasi Model:**
- **Metrik:** Silhouette Score (range -1 sampai 1, target > 0.5)
- **Validasi:** Karena data hanya 6 poin, gunakan Leave-One-Out validation secara manual — uji apakah penghapusan satu wilayah mengubah cluster assignment secara drastis
- **Interpretasi Cluster:** Setelah clustering, sort cluster berdasarkan rata-rata NRS untuk memberi label "Risiko Rendah/Sedang/Tinggi"

### 4.2 Isolation Forest — Anomaly Detection (scikit-learn)

**Tujuan:** Mendeteksi wilayah yang memiliki pola tidak wajar — misalnya angka stunting sangat tinggi tapi rasio faskes juga sangat rendah (double anomali yang memerlukan intervensi segera).

**Catatan:** Isolation Forest dijalankan pada data Gold yang sudah diagregasi (6 baris per snapshot). scikit-learn digunakan karena data sudah kecil dan tidak perlu distributed computation.

**Implementasi:**
```python
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib

# Load dari Gold layer
df_gold_pd = df_gold.toPandas()  # convert dari Spark ke Pandas

feature_cols = [
    "prevalensi_stunting_pct",
    "rasio_faskes_per_10k_balita",
    "rasio_posyandu_per_10k_populasi",
    "rasio_nakes_per_10k_populasi",
    "nutrition_risk_score"
]

X = df_gold_pd[feature_cols].fillna(df_gold_pd[feature_cols].median())

# Scaling
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Isolation Forest
# contamination=0.15 artinya kita ekspektasi ~15% data bisa jadi anomali
# (dari 6 wilayah, ~1 wilayah)
iso_forest = IsolationForest(
    n_estimators=100,
    contamination=0.15,
    random_state=42
)
iso_forest.fit(X_scaled)

# Prediksi: -1 = anomali, 1 = normal
df_gold_pd["anomaly_raw"] = iso_forest.predict(X_scaled)
df_gold_pd["anomaly_score"] = iso_forest.score_samples(X_scaled)
df_gold_pd["is_anomaly"] = df_gold_pd["anomaly_raw"] == -1

# Simpan model
joblib.dump(iso_forest, "/tmp/isolation_forest_model.pkl")
# Upload ke HDFS
# hadoop fs -put /tmp/isolation_forest_model.pkl hdfs://namenode:9000/data/gold/model_artifacts/

print(df_gold_pd[["kabupaten_kota", "anomaly_score", "is_anomaly"]].sort_values("anomaly_score"))
```

**Evaluasi Model:**
- **Metrik:** Precision dan Recall tidak bisa dihitung karena tidak ada ground truth label. Sebagai gantinya, gunakan **domain validation** — apakah wilayah yang terdeteksi anomali secara kualitatif memang memiliki kombinasi stunting tinggi + faskes rendah?
- **Supplementary:** Visualisasi dengan scatter plot `prevalensi_stunting_pct` vs `rasio_faskes_per_10k_balita`, tandai anomali dengan warna berbeda
- **Robustness check:** Jalankan dengan `contamination` berbeda (0.1, 0.15, 0.2) dan pastikan wilayah anomali konsisten

### 4.3 GIS — Choropleth Map (GeoPandas)

**Tujuan:** Menggabungkan hasil analisis (NRS, cluster, anomali) dengan shapefile wilayah Jakarta untuk menghasilkan peta risiko visual.

**Setup:**
```bash
pip install geopandas folium matplotlib contextily
# Download shapefile DKI Jakarta
# Tersedia di: https://data.jakarta.go.id atau gunakan shapefile Indonesia umum
```

**Implementasi:**
```python
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import folium

# Load shapefile DKI Jakarta (level kabupaten/kota)
gdf_jakarta = gpd.read_file("data/shapefiles/dki_jakarta_kabupaten.shp")

# Load Gold layer hasil analisis
df_gold_pd = pd.read_parquet("gold_wilayah_risk_score.parquet")

# Mapping nama wilayah agar konsisten dengan shapefile
nama_mapping = {
    "JAKARTA PUSAT": "Jakarta Pusat",
    "JAKARTA UTARA": "Jakarta Utara",
    "JAKARTA SELATAN": "Jakarta Selatan",
    "JAKARTA TIMUR": "Jakarta Timur",
    "JAKARTA BARAT": "Jakarta Barat",
    "KEPULAUAN SERIBU": "Kepulauan Seribu"
}
df_gold_pd["kabupaten_kota_mapped"] = df_gold_pd["kabupaten_kota"].map(nama_mapping)

# Join
gdf_merged = gdf_jakarta.merge(
    df_gold_pd,
    left_on="NAMOBJ",  # sesuaikan dengan field di shapefile
    right_on="kabupaten_kota_mapped",
    how="left"
)

# Plot Choropleth — NRS
fig, axes = plt.subplots(1, 2, figsize=(16, 8))

# Map 1: Nutrition Risk Score
gdf_merged.plot(
    column="nutrition_risk_score",
    cmap="RdYlGn_r",  # merah = berisiko tinggi
    legend=True,
    ax=axes[0],
    edgecolor="black",
    linewidth=0.5,
    missing_kwds={"color": "lightgrey", "label": "No Data"}
)
axes[0].set_title("Nutrition Risk Score per Wilayah", fontsize=14, fontweight="bold")
axes[0].axis("off")

# Map 2: Cluster Label
cluster_colors = {"Risiko Rendah": "green", "Risiko Sedang": "yellow", "Risiko Tinggi": "red"}
gdf_merged["cluster_color"] = gdf_merged["cluster_label"].map(cluster_colors)
gdf_merged.plot(
    color=gdf_merged["cluster_color"].fillna("lightgrey"),
    ax=axes[1],
    edgecolor="black",
    linewidth=0.5
)
axes[1].set_title("Cluster Risiko per Wilayah", fontsize=14, fontweight="bold")
axes[1].axis("off")

plt.tight_layout()
plt.savefig("output/peta_risiko_dki_jakarta.png", dpi=150, bbox_inches="tight")
plt.show()

# Opsional: Peta interaktif dengan Folium
m = folium.Map(location=[-6.2, 106.8], zoom_start=11)
folium.Choropleth(
    geo_data=gdf_merged,
    name="choropleth",
    data=df_gold_pd,
    columns=["kabupaten_kota_mapped", "nutrition_risk_score"],
    key_on="feature.properties.NAMOBJ",
    fill_color="RdYlGn_r",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Nutrition Risk Score"
).add_to(m)
m.save("output/peta_interaktif_jakarta.html")
```

---

## 5. Keunikan & Analisis Kompetitor

### 5.1 Tabel Perbandingan Kompetitor

| Fitur | Proyek Ini | SIGA (Kemenkes) | Sigizi Terpadu | E-PPGBM | Satudata Jakarta |
|---|---|---|---|---|---|
| Integrasi multi-sumber otomatis | ✅ | ❌ | ❌ | ❌ | ❌ |
| Risk scoring per wilayah | ✅ | ❌ | ❌ | ❌ | ❌ |
| Clustering ML wilayah | ✅ | ❌ | ❌ | ❌ | ❌ |
| Anomaly detection | ✅ | ❌ | ❌ | ❌ | ❌ |
| Peta risiko GIS | ✅ | ❌ | ❌ | ❌ | ❌ |
| Pipeline otomatis (Kafka) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Analisis berbasis data real | ✅ | ✅ | ✅ | ✅ | ✅ |

### 5.2 Kombinasi Teknologi Sinergis (≥3)

1. **Apache Kafka** → unified ingestion dari API dan CSV fallback
2. **Apache Spark (PySpark + MLlib)** → ETL medallion + K-Means clustering distributed
3. **GeoPandas + Folium** → spatial analysis dan visualisasi peta interaktif
4. **HDFS + Parquet** → lakehouse storage dengan partisi optimal
5. **scikit-learn (Isolation Forest)** → anomaly detection pada output Gold layer

Kombinasi Kafka + Spark + GIS adalah sinergi yang eksplisit karena data dari Kafka → diproses Spark → hasilnya divisualisasi secara spasial oleh GIS — tiap layer bergantung pada output layer sebelumnya.

---

## 6. Pipeline Developer Guide — Step by Step

### 6.1 Prerequisites & Environment Setup

**Software yang dibutuhkan:**
```
- Docker & Docker Compose (recommended untuk local dev)
- Python 3.10+
- Java 11 (untuk Spark & Kafka)
- Apache Kafka 3.x
- Apache Spark 3.4+
- Hadoop 3.3+ (HDFS)
- Node.js (opsional, untuk Superset frontend)
```

**Python Dependencies:**
```bash
pip install pyspark==3.4.0 \
            kafka-python==2.0.2 \
            pandas==2.0.0 \
            pyarrow==12.0.0 \
            geopandas==0.13.0 \
            folium==0.14.0 \
            scikit-learn==1.3.0 \
            joblib==1.3.0 \
            matplotlib==3.7.0 \
            requests==2.31.0 \
            python-dotenv==1.0.0
```

**Docker Compose untuk local environment (docker-compose.yml):**
```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    ports:
      - "9870:9870"
      - "9000:9000"
    environment:
      CLUSTER_NAME: test
    volumes:
      - hadoop_namenode:/hadoop/dfs/name

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    depends_on:
      - namenode
    volumes:
      - hadoop_datanode:/hadoop/dfs/data

volumes:
  hadoop_namenode:
  hadoop_datanode:
```

**Start environment:**
```bash
docker-compose up -d
```

### 6.2 Step 1 — Kafka Topic Setup

```bash
# Buat topics
kafka-topics.sh --create --topic gizi-raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic faskes-raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic nakes-raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic populasi-raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Verifikasi
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 6.3 Step 2 — Data Producer (Ingestion)

**File: `src/ingestion/producer.py`**
```python
import json
import time
import requests
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SATUDATA_BASE_URL = "https://satudata.jakarta.go.id/api"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    retries=3,
    retry_backoff_ms=1000
)

def fetch_from_api(endpoint: str, params: dict = None) -> list:
    """Fetch data dari Satudata Jakarta API dengan retry logic."""
    try:
        url = f"{SATUDATA_BASE_URL}/{endpoint}"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("data", data) if isinstance(data, dict) else data
    except requests.exceptions.RequestException as e:
        print(f"[WARNING] API fetch failed for {endpoint}: {e}")
        return None  # Trigger fallback

def fetch_from_csv_fallback(filepath: str) -> list:
    """Fallback: baca dari file CSV/Excel lokal."""
    try:
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filepath.endswith((".xlsx", ".xls")):
            df = pd.read_excel(filepath)
        else:
            raise ValueError(f"Unsupported format: {filepath}")
        print(f"[FALLBACK] Loaded {len(df)} rows from {filepath}")
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"[ERROR] Fallback also failed for {filepath}: {e}")
        return []

def produce_to_kafka(topic: str, records: list, source: str):
    """Kirim records ke Kafka topic."""
    ingested_at = datetime.utcnow().isoformat()
    for i, record in enumerate(records):
        record["ingested_at"] = ingested_at
        record["source"] = source
        future = producer.send(
            topic,
            key=str(i),
            value=record
        )
        future.get(timeout=10)  # Block sampai acknowledge
    producer.flush()
    print(f"[INFO] Produced {len(records)} records to topic '{topic}' (source: {source})")

def ingest_gizi():
    """Ingest dataset 1 — Gizi Balita."""
    # Coba API dulu
    data = fetch_from_api("v1/datasets/gizi-balita", params={"periode": "2025"})
    source = "api"

    # Fallback ke CSV jika API gagal
    if not data:
        data = fetch_from_csv_fallback("data/fallback/gizi_balita_2025.csv")
        source = "csv_fallback"

    if data:
        produce_to_kafka("gizi-raw", data, source)

def ingest_faskes():
    """Ingest dataset 2 — Fasilitas Kesehatan."""
    data = fetch_from_api("v1/datasets/fasilitas-kesehatan")
    source = "api"
    if not data:
        data = fetch_from_csv_fallback("data/fallback/faskes_dki_2022.csv")
        source = "csv_fallback"
    if data:
        produce_to_kafka("faskes-raw", data, source)

def ingest_nakes():
    """Ingest dataset 3 — Tenaga Kesehatan."""
    data = fetch_from_api("v1/datasets/tenaga-kesehatan")
    source = "api"
    if not data:
        data = fetch_from_csv_fallback("data/fallback/nakes_dki_2023.csv")
        source = "csv_fallback"
    if data:
        produce_to_kafka("nakes-raw", data, source)

def ingest_populasi():
    """Ingest dataset 4 — Populasi."""
    data = fetch_from_api("v1/datasets/populasi-dki")
    source = "api"
    if not data:
        data = fetch_from_csv_fallback("data/fallback/populasi_dki_2024.csv")
        source = "csv_fallback"
    if data:
        produce_to_kafka("populasi-raw", data, source)

if __name__ == "__main__":
    print("[INFO] Starting ingestion pipeline...")
    ingest_gizi()
    ingest_faskes()
    ingest_nakes()
    ingest_populasi()
    print("[INFO] Ingestion complete.")
```

### 6.4 Step 3 — HDFS Consumer (Kafka → Bronze)

**File: `src/ingestion/consumer_to_hdfs.py`**
```python
import json
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
import subprocess
import tempfile
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

TOPIC_CONFIG = {
    "gizi-raw": {
        "hdfs_path": f"{HDFS_NAMENODE}/data/bronze/gizi",
        "partition_cols": ["periode_data", "wilayah"]
    },
    "faskes-raw": {
        "hdfs_path": f"{HDFS_NAMENODE}/data/bronze/faskes",
        "partition_cols": ["tahun"]
    },
    "nakes-raw": {
        "hdfs_path": f"{HDFS_NAMENODE}/data/bronze/nakes",
        "partition_cols": ["periode_data", "wilayah"]
    },
    "populasi-raw": {
        "hdfs_path": f"{HDFS_NAMENODE}/data/bronze/populasi",
        "partition_cols": ["tahun"]
    }
}

def write_to_hdfs(records: list, hdfs_path: str):
    """Tulis records sebagai Parquet ke HDFS."""
    if not records:
        return
    table = pa.Table.from_pylist(records)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        pq.write_table(table, tmp.name, compression="snappy")
        tmp_path = tmp.name
    # Upload ke HDFS
    subprocess.run(
        ["hadoop", "fs", "-put", "-f", tmp_path, hdfs_path],
        check=True
    )
    os.unlink(tmp_path)
    print(f"[INFO] Written {len(records)} records to HDFS: {hdfs_path}")

def consume_all():
    consumer = KafkaConsumer(
        *TOPIC_CONFIG.keys(),
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        group_id="hdfs-writer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000  # Stop setelah 10 detik tidak ada pesan baru
    )

    buffer = {topic: [] for topic in TOPIC_CONFIG.keys()}

    for message in consumer:
        topic = message.topic
        buffer[topic].append(message.value)

    # Flush buffer ke HDFS
    for topic, records in buffer.items():
        if records:
            config = TOPIC_CONFIG[topic]
            write_to_hdfs(records, config["hdfs_path"])

    consumer.close()

if __name__ == "__main__":
    consume_all()
```

### 6.5 Step 4 — Bronze to Silver (PySpark ETL)

**File: `src/processing/bronze_to_silver.py`**
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

HDFS_BASE = "hdfs://namenode:9000/data"

# Dictionary untuk standarisasi nama wilayah
WILAYAH_MAPPING = {
    "JAKARTA PUSAT": "JAKARTA PUSAT",
    "JAKARTA UTARA": "JAKARTA UTARA",
    "JAKARTA SELATAN": "JAKARTA SELATAN",
    "JAKARTA TIMUR": "JAKARTA TIMUR",
    "JAKARTA BARAT": "JAKARTA BARAT",
    "KEPULAUAN SERIBU": "KEPULAUAN SERIBU",
    # Tambahkan variasi nama yang mungkin muncul di data
    "KAB. KEPULAUAN SERIBU": "KEPULAUAN SERIBU",
    "JAKPUS": "JAKARTA PUSAT",
}

mapping_expr = F.create_map([F.lit(k) for pair in WILAYAH_MAPPING.items() for k in pair])

def clean_gizi():
    df = spark.read.parquet(f"{HDFS_BASE}/bronze/gizi/")

    df_clean = df \
        .withColumn("wilayah_std", mapping_expr[F.upper(F.col("wilayah"))]) \
        .withColumn("wilayah_std", F.coalesce(F.col("wilayah_std"), F.upper(F.col("wilayah")))) \
        .withColumn("jumlah", F.col("jumlah").cast(IntegerType())) \
        .filter(F.col("jumlah").isNotNull() & (F.col("jumlah") >= 0)) \
        .dropDuplicates(["periode_data", "wilayah_std", "kecamatan", "kategori_masalah_gizi"]) \
        .withColumn("kategori_masalah_gizi", F.upper(F.trim(F.col("kategori_masalah_gizi"))))

    # Agregasi ke level kabupaten/kota
    df_agg = df_clean.groupBy("wilayah_std", "periode_data") \
        .pivot("kategori_masalah_gizi", ["STUNTING", "WASTING", "UNDERWEIGHT", "OVERWEIGHT"]) \
        .agg(F.sum("jumlah")) \
        .withColumnRenamed("wilayah_std", "kabupaten_kota")

    df_agg.write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{HDFS_BASE}/silver/gizi_agregat/")

    print("[Silver] Gizi cleaning done.")

def clean_faskes():
    df = spark.read.parquet(f"{HDFS_BASE}/bronze/faskes/")

    df_clean = df \
        .withColumn("kabupaten_kota", mapping_expr[F.upper(F.col("kabupaten_kota"))]) \
        .withColumn("jumlah_rs_umum", F.col("jumlah_rs_umum").cast(IntegerType())) \
        .withColumn("jumlah_rs_khusus", F.col("jumlah_rs_khusus").cast(IntegerType())) \
        .withColumn("jumlah_posyandu", F.col("jumlah_posyandu").cast(IntegerType())) \
        .na.fill(0, subset=["jumlah_rs_umum", "jumlah_rs_khusus", "jumlah_posyandu"])

    df_clean.write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{HDFS_BASE}/silver/faskes_clean/")

    print("[Silver] Faskes cleaning done.")

def clean_nakes():
    df = spark.read.parquet(f"{HDFS_BASE}/bronze/nakes/")

    df_clean = df \
        .withColumn("wilayah_std", mapping_expr[F.upper(F.col("wilayah"))]) \
        .withColumn("wilayah_std", F.coalesce(F.col("wilayah_std"), F.upper(F.col("wilayah")))) \
        .withColumn("jumlah", F.col("jumlah").cast(IntegerType())) \
        .filter(F.col("jumlah").isNotNull() & (F.col("jumlah") >= 0)) \
        .dropDuplicates(["periode_data", "wilayah_std", "kecamatan", "tenaga_kesehatan"])

    # Agregasi total nakes per wilayah
    df_agg = df_clean.groupBy("wilayah_std", "periode_data") \
        .agg(F.sum("jumlah").alias("total_nakes")) \
        .withColumnRenamed("wilayah_std", "kabupaten_kota")

    df_agg.write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{HDFS_BASE}/silver/nakes_agregat/")

    print("[Silver] Nakes cleaning done.")

def clean_populasi():
    df = spark.read.parquet(f"{HDFS_BASE}/bronze/populasi/")

    df_clean = df \
        .withColumn("kabupaten_kota", mapping_expr[F.upper(F.col("kabupaten_kota"))]) \
        .withColumn("populasi", (F.col("population_thousand") * 1000).cast(IntegerType())) \
        .withColumn("density_per_sqkm", F.col("population_density_per_sqkm").cast(FloatType()))

    df_clean.write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{HDFS_BASE}/silver/populasi_clean/")

    print("[Silver] Populasi cleaning done.")

if __name__ == "__main__":
    clean_gizi()
    clean_faskes()
    clean_nakes()
    clean_populasi()
    spark.stop()
```

### 6.6 Step 5 — Silver to Gold (Aggregation + Scoring)

**File: `src/processing/silver_to_gold.py`**
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

HDFS_BASE = "hdfs://namenode:9000/data"

def build_gold():
    # Load semua silver datasets
    df_gizi = spark.read.parquet(f"{HDFS_BASE}/silver/gizi_agregat/")
    df_faskes = spark.read.parquet(f"{HDFS_BASE}/silver/faskes_clean/")
    df_nakes = spark.read.parquet(f"{HDFS_BASE}/silver/nakes_agregat/")
    df_pop = spark.read.parquet(f"{HDFS_BASE}/silver/populasi_clean/")

    # Join semua ke satu tabel master
    df_master = df_pop.select("kabupaten_kota", "populasi") \
        .join(df_gizi.select(
            "kabupaten_kota",
            F.coalesce(F.col("STUNTING"), F.lit(0)).alias("total_stunting"),
            F.coalesce(F.col("WASTING"), F.lit(0)).alias("total_wasting"),
            F.coalesce(F.col("UNDERWEIGHT"), F.lit(0)).alias("total_underweight"),
            F.coalesce(F.col("OVERWEIGHT"), F.lit(0)).alias("total_overweight")
        ), on="kabupaten_kota", how="left") \
        .join(df_faskes.select(
            "kabupaten_kota",
            "jumlah_rs_umum", "jumlah_rs_khusus", "jumlah_posyandu"
        ), on="kabupaten_kota", how="left") \
        .join(df_nakes.select(
            "kabupaten_kota", "total_nakes"
        ), on="kabupaten_kota", how="left")

    # Hitung total balita gizi buruk
    df_master = df_master.withColumn(
        "total_balita_gizi_buruk",
        F.col("total_stunting") + F.col("total_wasting") +
        F.col("total_underweight") + F.col("total_overweight")
    )

    # Hitung rasio indikator
    df_indicators = df_master \
        .withColumn(
            "rasio_faskes_per_10k_balita",
            F.when(F.col("total_balita_gizi_buruk") > 0,
                (F.col("jumlah_rs_umum") + F.col("jumlah_rs_khusus")) /
                (F.col("total_balita_gizi_buruk") / 10000)
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "rasio_posyandu_per_10k_populasi",
            F.when(F.col("populasi") > 0,
                F.col("jumlah_posyandu") / (F.col("populasi") / 10000)
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "rasio_nakes_per_10k_populasi",
            F.when(F.col("populasi") > 0,
                F.col("total_nakes") / (F.col("populasi") / 10000)
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "prevalensi_stunting_pct",
            F.when(F.col("populasi") > 0,
                (F.col("total_stunting") / F.col("populasi")) * 100
            ).otherwise(F.lit(None))
        )

    # Hitung Nutrition Coverage Index (NCI) — rata-rata ternormalisasi
    # Min-max normalization menggunakan Window function
    def min_max_norm(col_name):
        w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        min_val = F.min(F.col(col_name)).over(w)
        max_val = F.max(F.col(col_name)).over(w)
        return (F.col(col_name) - min_val) / (max_val - min_val)

    df_scored = df_indicators \
        .withColumn("norm_stunting", min_max_norm("prevalensi_stunting_pct")) \
        .withColumn("norm_faskes_inv", F.lit(1) - min_max_norm("rasio_faskes_per_10k_balita")) \
        .withColumn("norm_nakes_inv", F.lit(1) - min_max_norm("rasio_nakes_per_10k_populasi")) \
        .withColumn(
            "nutrition_risk_score",
            (F.col("norm_stunting") * 0.5) +
            (F.col("norm_faskes_inv") * 0.3) +
            (F.col("norm_nakes_inv") * 0.2)
        ) \
        .withColumn(
            "nutrition_coverage_index",
            (min_max_norm("rasio_faskes_per_10k_balita") +
             min_max_norm("rasio_posyandu_per_10k_populasi") +
             min_max_norm("rasio_nakes_per_10k_populasi")) / 3
        ) \
        .withColumn("last_updated", F.current_timestamp())

    # Ranking wilayah (1 = paling prioritas = NRS tertinggi)
    window_rank = Window.orderBy(F.col("nutrition_risk_score").desc())
    df_final = df_scored.withColumn("priority_rank", F.rank().over(window_rank))

    df_final.write.mode("overwrite") \
        .parquet(f"{HDFS_BASE}/gold/wilayah_risk_score/")

    print("[Gold] Risk scoring complete.")
    df_final.show(truncate=False)

if __name__ == "__main__":
    build_gold()
    spark.stop()
```

### 6.7 Step 6 — ML Analysis (K-Means + Isolation Forest)

**File: `src/analysis/ml_analysis.py`**
```python
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler as SklearnScaler
import joblib
import pandas as pd

spark = SparkSession.builder.appName("MLAnalysis").getOrCreate()
HDFS_BASE = "hdfs://namenode:9000/data"

feature_cols = [
    "prevalensi_stunting_pct",
    "rasio_faskes_per_10k_balita",
    "rasio_posyandu_per_10k_populasi",
    "rasio_nakes_per_10k_populasi",
    "nutrition_coverage_index"
]

def run_kmeans(df_gold):
    df_clean = df_gold.dropna(subset=feature_cols)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_assembled = assembler.transform(df_clean)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                             withMean=True, withStd=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)

    # K=3 untuk 3 level risiko (Rendah/Sedang/Tinggi)
    kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster_id",
                    maxIter=20, tol=1e-4)
    kmeans_model = kmeans.fit(df_scaled)
    df_clustered = kmeans_model.transform(df_scaled)

    # Evaluasi
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    silhouette = evaluator.evaluate(df_clustered)
    print(f"[K-Means] Silhouette Score: {silhouette:.4f}")

    # Assign label berdasarkan rata-rata NRS per cluster
    cluster_nrs = df_clustered.groupBy("cluster_id") \
        .agg({"nutrition_risk_score": "mean"}) \
        .orderBy("avg(nutrition_risk_score)", ascending=True)
    cluster_nrs.show()

    # Map cluster_id ke label (manual setelah lihat output)
    # Ganti mapping ini setelah melihat hasil cluster_nrs.show()
    cluster_label_map = {0: "Risiko Rendah", 1: "Risiko Sedang", 2: "Risiko Tinggi"}

    kmeans_model.save(f"{HDFS_BASE}/gold/model_artifacts/kmeans_model")
    return df_clustered, cluster_label_map

def run_isolation_forest(df_gold_pd):
    X = df_gold_pd[feature_cols].fillna(df_gold_pd[feature_cols].median())

    scaler = SklearnScaler()
    X_scaled = scaler.fit_transform(X)

    iso_forest = IsolationForest(
        n_estimators=100,
        contamination=0.15,
        random_state=42
    )
    iso_forest.fit(X_scaled)

    df_gold_pd = df_gold_pd.copy()
    df_gold_pd["anomaly_raw"] = iso_forest.predict(X_scaled)
    df_gold_pd["anomaly_score"] = iso_forest.score_samples(X_scaled)
    df_gold_pd["is_anomaly"] = df_gold_pd["anomaly_raw"] == -1

    joblib.dump(iso_forest, "/tmp/isolation_forest_model.pkl")

    print("[Isolation Forest] Results:")
    print(df_gold_pd[["kabupaten_kota", "anomaly_score", "is_anomaly"]].sort_values("anomaly_score"))

    return df_gold_pd

if __name__ == "__main__":
    df_gold = spark.read.parquet(f"{HDFS_BASE}/gold/wilayah_risk_score/")

    # Run K-Means
    df_clustered, cluster_label_map = run_kmeans(df_gold)

    # Run Isolation Forest (convert ke Pandas karena data kecil)
    df_gold_pd = df_gold.toPandas()
    df_result = run_isolation_forest(df_gold_pd)

    # Simpan hasil gabungan ke Gold final
    df_gold_pd["cluster_id"] = df_clustered.select("cluster_id").toPandas()["cluster_id"]
    df_gold_pd["cluster_label"] = df_gold_pd["cluster_id"].map(cluster_label_map)
    df_gold_pd["is_anomaly"] = df_result["is_anomaly"]
    df_gold_pd["anomaly_score"] = df_result["anomaly_score"]

    # Convert kembali ke Spark dan simpan
    df_final = spark.createDataFrame(df_gold_pd)
    df_final.write.mode("overwrite") \
        .parquet(f"{HDFS_BASE}/gold/wilayah_final/")

    spark.stop()
```

### 6.8 Step 7 — GIS Visualization

**File: `src/visualization/gis_map.py`**
```python
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import folium
import pyarrow.parquet as pq

# Load data Gold final
df = pd.read_parquet("output/wilayah_final.parquet")

# Load shapefile Jakarta (download dari data.jakarta.go.id)
gdf = gpd.read_file("data/shapefiles/dki_jakarta_kabupaten.shp")
gdf["NAMOBJ"] = gdf["NAMOBJ"].str.upper().str.strip()

# Merge
gdf_merged = gdf.merge(df, left_on="NAMOBJ", right_on="kabupaten_kota", how="left")

# ─── Plot 1: Choropleth NRS ───────────────────────────────
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
gdf_merged.plot(
    column="nutrition_risk_score",
    cmap="RdYlGn_r",
    legend=True,
    legend_kwds={"label": "Nutrition Risk Score (0=Aman, 1=Kritis)"},
    ax=ax,
    edgecolor="black",
    linewidth=0.8,
    missing_kwds={"color": "lightgrey"}
)

# Tambah label nama wilayah
for idx, row in gdf_merged.iterrows():
    if row.geometry:
        centroid = row.geometry.centroid
        ax.annotate(
            row["NAMOBJ"].replace("JAKARTA ", "Jak. "),
            xy=(centroid.x, centroid.y),
            ha="center", fontsize=7
        )

ax.set_title("Peta Nutrition Risk Score\nDKI Jakarta per Kabupaten/Kota",
             fontsize=14, fontweight="bold")
ax.axis("off")
plt.tight_layout()
plt.savefig("output/peta_nrs.png", dpi=150, bbox_inches="tight")

# ─── Plot 2: Cluster Risiko ───────────────────────────────
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
color_map = {"Risiko Rendah": "#2ecc71", "Risiko Sedang": "#f39c12", "Risiko Tinggi": "#e74c3c"}
gdf_merged["color"] = gdf_merged["cluster_label"].map(color_map).fillna("lightgrey")
gdf_merged.plot(color=gdf_merged["color"], ax=ax, edgecolor="black", linewidth=0.8)

legend_patches = [mpatches.Patch(color=v, label=k) for k, v in color_map.items()]
ax.legend(handles=legend_patches, loc="lower right", fontsize=10)
ax.set_title("Cluster Risiko Gizi Balita\nDKI Jakarta per Kabupaten/Kota",
             fontsize=14, fontweight="bold")
ax.axis("off")
plt.tight_layout()
plt.savefig("output/peta_cluster.png", dpi=150, bbox_inches="tight")

# ─── Peta Interaktif Folium ───────────────────────────────
m = folium.Map(location=[-6.2, 106.8], zoom_start=11, tiles="CartoDB positron")

folium.Choropleth(
    geo_data=gdf_merged.__geo_interface__,
    data=df,
    columns=["kabupaten_kota", "nutrition_risk_score"],
    key_on="feature.properties.NAMOBJ",
    fill_color="RdYlGn_r",
    fill_opacity=0.75,
    line_opacity=0.3,
    legend_name="Nutrition Risk Score",
    nan_fill_color="lightgrey"
).add_to(m)

# Tooltip
folium.GeoJson(
    gdf_merged,
    tooltip=folium.GeoJsonTooltip(
        fields=["NAMOBJ", "nutrition_risk_score", "cluster_label",
                "prevalensi_stunting_pct", "priority_rank"],
        aliases=["Wilayah", "NRS", "Kategori Risiko", "Prevalensi Stunting (%)", "Prioritas"],
        localize=True
    )
).add_to(m)

m.save("output/peta_interaktif.html")
print("[GIS] Maps exported to output/")
```

### 6.9 Step 8 — Run Pipeline (Urutan Eksekusi)

```bash
# 1. Start infrastruktur
docker-compose up -d
sleep 30  # Tunggu services ready

# 2. Buat Kafka topics
python src/ingestion/setup_topics.py

# 3. Jalankan producer (ingestion dari API atau fallback CSV)
python src/ingestion/producer.py

# 4. Jalankan consumer (Kafka → Bronze HDFS)
python src/ingestion/consumer_to_hdfs.py

# 5. Bronze → Silver (PySpark)
spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  src/processing/bronze_to_silver.py

# 6. Silver → Gold (PySpark)
spark-submit \
  --master spark://localhost:7077 \
  src/processing/silver_to_gold.py

# 7. ML Analysis (K-Means + Isolation Forest)
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-mllib_2.12:3.4.0 \
  src/analysis/ml_analysis.py

# 8. Export Gold ke local untuk GIS + Dashboard
hadoop fs -get hdfs://namenode:9000/data/gold/wilayah_final/ output/

# 9. GIS Visualization
python src/visualization/gis_map.py

# 10. Launch Dashboard (Superset atau Flask)
python src/dashboard/app.py
```

---

## 7. Struktur Direktori Proyek

```
big-data-final-project/
├── docker-compose.yml
├── .env                          # KAFKA_BOOTSTRAP, HDFS_NAMENODE, dll
├── requirements.txt
├── README.md
│
├── data/
│   ├── fallback/                 # CSV/Excel fallback data
│   │   ├── gizi_balita_2025.csv
│   │   ├── faskes_dki_2022.csv
│   │   ├── nakes_dki_2023.csv
│   │   └── populasi_dki_2024.csv
│   └── shapefiles/               # Shapefile DKI Jakarta
│       ├── dki_jakarta_kabupaten.shp
│       ├── dki_jakarta_kabupaten.dbf
│       └── dki_jakarta_kabupaten.prj
│
├── src/
│   ├── ingestion/
│   │   ├── producer.py           # API fetcher + Kafka producer
│   │   ├── consumer_to_hdfs.py   # Kafka consumer → HDFS Bronze
│   │   └── setup_topics.py       # Buat Kafka topics
│   │
│   ├── processing/
│   │   ├── bronze_to_silver.py   # ETL Bronze → Silver (PySpark)
│   │   └── silver_to_gold.py     # Aggregation + Scoring (PySpark)
│   │
│   ├── analysis/
│   │   └── ml_analysis.py        # K-Means + Isolation Forest
│   │
│   └── visualization/
│       ├── gis_map.py            # Choropleth + Folium map
│       └── dashboard/
│           └── app.py            # Flask dashboard (opsional)
│
└── output/                       # Output files (peta, parquet hasil, model)
    ├── peta_nrs.png
    ├── peta_cluster.png
    ├── peta_interaktif.html
    └── wilayah_final.parquet
```

---

## 8. Error Handling & Fallback

### 8.1 Strategi Fallback Data

| Skenario | Handling |
|---|---|
| API Satudata Jakarta tidak merespons (timeout 30 detik) | Otomatis fallback ke CSV lokal di `data/fallback/` |
| CSV fallback tidak ada / corrupt | Log error, skip dataset tersebut, flag `source=NULL` di Bronze |
| Kafka tidak tersedia | Producer menggunakan retry (3x) dengan backoff 1 detik; jika gagal semua, tulis langsung ke HDFS via batch mode |
| Data null di Silver | Impute dengan median per wilayah; kolom `data_completeness_score` mencatat % data valid |
| Data null di Gold untuk perhitungan rasio | Gunakan `F.when(...).otherwise(F.lit(None))` — wilayah dengan data tidak lengkap tetap muncul di tabel tapi NRS-nya NULL dan dikecualikan dari ranking |
| K-Means gagal konvergen | Naikkan `maxIter` ke 50; jika data terlalu sedikit, fallback ke manual kategorisasi berdasarkan NRS quartile |
| Shapefile tidak match nama wilayah | `NAMA_MAPPING` dictionary di `gis_map.py` sebagai reconciliation layer |

### 8.2 Data Completeness Check

```python
# Setelah Silver selesai, jalankan ini untuk cek kualitas data
def check_completeness(df_silver):
    total = df_silver.count()
    for col in df_silver.columns:
        null_count = df_silver.filter(F.col(col).isNull()).count()
        pct = (null_count / total) * 100
        if pct > 0:
            print(f"  {col}: {null_count} nulls ({pct:.1f}%)")
```

---

## 9. Checklist Rubrik Penilaian

### Rubrik 1 — Problem Statement
- [x] Masalah dibuktikan dengan data kuantitatif (Satudata Jakarta 2025, BPS 2024)
- [x] Justifikasi 5V (Volume, Velocity, Variety, Veracity, Value) — lihat Bagian 1.2
- [x] Analisis gap menunjukkan solusi existing belum menyelesaikan masalah — lihat Bagian 1.3

### Rubrik 2 — Pipeline & Arsitektur
- [x] Pipeline lengkap: Kafka (ingestion) → HDFS (storage) → Spark (processing) → Dashboard (serving)
- [x] Justifikasi teknis setiap teknologi — lihat Bagian 2.2
- [x] Diagram arsitektur end-to-end — lihat Bagian 2.1

### Rubrik 3 — Medallion Architecture
- [x] Bronze → Silver → Gold eksplisit dengan transformasi tiap layer
- [x] Format Parquet + Snappy compression
- [x] Partisi: Bronze per wilayah/periode, Silver per kabupaten_kota, Gold flat

### Rubrik 4 — Analisis Lanjutan
- [x] K-Means Clustering (teknik 1) — evaluasi: Silhouette Score
- [x] Isolation Forest / Anomaly Detection (teknik 2) — evaluasi: domain validation + anomaly score
- [x] GIS Spatial Analysis (memperkuat rubrik "clustering spasial")
- [x] Output terukur: NRS (0–1), cluster label, anomaly score, priority rank

### Rubrik 5 — Keunikan & Kompetitor
- [x] Analisis kompetitor: SIGA, Sigizi Terpadu, E-PPGBM, Satudata Jakarta
- [x] Kombinasi ≥3 teknologi sinergis: Kafka + Spark + GIS + HDFS + scikit-learn

### Rubrik 6 — Implementasi End-to-End
- [x] Pipeline aktif dari ingestion hingga serving (bukan mock)
- [x] Data real via API + simulasi realistis via CSV fallback
- [x] Error handling: API timeout → CSV fallback, null handling di setiap layer, Kafka retry

---

*Guide ini dibuat berdasarkan diskusi desain sistem dan disesuaikan dengan rubrik penilaian final project Big Data. Setiap kode di atas adalah template starter — sesuaikan nama endpoint API, field name, dan path HDFS dengan environment aktual kelompok.*
