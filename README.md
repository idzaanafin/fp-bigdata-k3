# FINAL PROJECT - BIG DATA K3

### Kelas A
| Nama | NRP |
| --- | --- |
| Ahmad Idza Anafin | 5027241017 |
| Aditya Reza Daffansyah | 50272410 |
| Reza Aziz Simatupang | 50272410 |
| Ahmad Rafi Fadhillah Dwiputra | 50272410 |
| Muhammad Rafi' Adly | 50272410 |



## Command Documentation
### Start & Stop Service
```
docker-compose up -d --build
docker-compose down
```

### Ingestion (Raw Data to Kafka)
```
docker exec -it (namacontainer) python3 src/ingestion/ingestion.py
```

### Bronze Layer (Kafka to HDFS)
```
docker exec -it (namacontainer) python3 src/pipeline/bronze_layer.py

```

### Silver Layer (HDFS to Spark)
```
docker exec -it (namacontainer) python3 src/pipeline/silver_layer.py
```

### Gold Layer (Spark to JSON)
```
docker exec -it (namacontainer) python3 src/pipeline/gold_layer.py
```

### Remove All Data
```
docker-compose down -v
docker volume rm (namavolume)
```


# Sistem Audit Ketimpangan Distribusi Fasilitas Kesehatan Balita DKI Jakarta Berbasis Arsitektur Data Lakehouse

Sistem ini adalah platform *Data Lakehouse* end-to-end yang mengintegrasikan data prevalensi gizi buruk balita dengan data sebaran fasilitas kesehatan (Puskesmas, Rumah Sakit, Posyandu) di DKI Jakarta. Sistem ini secara otomatis menghitung *Nutrition Risk Score* dan mengelompokkan wilayah prioritas intervensi menggunakan Machine Learning secara objektif.

---

## 1. Latar Belakang & Kerangka 5V Big Data

### Analisis Masalah & Gap Analysis
Permasalahan gizi buruk dan stunting pada balita di DKI Jakarta masih menjadi tantangan yang sulit teridentifikasi secara dini. Saat ini, data terkait status gizi, fasilitas kesehatan, dan posyandu tersimpan secara terfragmentasi pada berbagai portal berbeda (Satudata Jakarta dan BPS). Belum adanya mekanisme integrasi otomatis menyebabkan proses analisis masih manual, memakan waktu lama, dan bersifat reaktif. 

Sistem ini menyelesaikan *gap* tersebut dengan mengintegrasikan data lintas sektoral secara otomatis untuk menghasilkan pemetaan intervensi yang proaktif dan berbasis data kuantitatif terukur.

### Justifikasi Kerangka 5V Big Data
* **Volume:** Mengakumulasikan data granular seluruh balita berisiko gizi, log kunjungan posyandu, dan koordinat faskes di seluruh kelurahan/kecamatan DKI Jakarta secara kumulatif.
* **Velocity:** Mengotomatiskan penyerapan data berkala dari API portal pemerintah daerah untuk memperbarui *Nutrition Risk Score* secara dinamis tanpa jeda analisis manual.
* **Variety:** Menangani karakteristik data yang beragam (*semi-structured* dan *structured*) seperti file tabular kasus gizi BPS, data spasial lokasi faskes, hingga metadata posyandu.
* **Veracity:** Mengatasi ketidakpastian data (*data noise*) seperti format penulisan nama wilayah yang inkonsisten antar instansi dan menangani record faskes yang kosong.
* **Value:** Memberikan rekomendasi wilayah prioritas intervensi secara presisi bagi Pemerintah Daerah untuk pemerataan tenaga medis dan posyandu.

---

## 2. Arsitektur Sistem & Justifikasi Teknis

Sistem ini menggabungkan komponen teknologi modern untuk membentuk pipeline data aktif yang sinergis:

[ Raw Data ] -> [ Apache Kafka ] -> [ Apache Hadoop HDFS ] -> (Medallion Architecture) -> [ PySpark (MLlib K-Means) ] -> [ Local Gold JSON ] -> [ Flask Dashboard UI ]




### Justifikasi Pemilihan Teknologi
| Layer | Teknologi | Justifikasi Teknis Eksplisit |
| :--- | :--- | :--- |
| **Ingestion** | Apache Kafka | Dipilih untuk menangani *ingestion* data dari berbagai *endpoint* portal data secara aman. Kafka bertindak sebagai *decoupling layer* yang menjamin ketersediaan aliran data tanpa membebani server sumber daya primer. |
| **Storage** | Apache Hadoop HDFS | Digunakan sebagai *Data Lakehouse storage* untuk menyimpan snapshot data kesehatan historis Jakarta yang andal, murah, dan *fault-tolerant* melalui mekanisme replikasi internal. |
| **Processing** | Apache Spark (PySpark) | Memproses kalkulasi matriks kompleks (Rasio faskes dan indeks cakupan gizi) secara terdistribusi di dalam memori (*in-memory*), yang jauh lebih cepat dibandingkan pemrosesan sekuensial tradisional. |
| **Serving** | Flask Web Framework | Menyajikan visualisasi peta choropleth tingkat risiko wilayah secara ringan (*lightweight*) dengan membaca hasil kalkulasi akhir tanpa mengganggu kluster komputasi utama. |

---

## 3. Penerapan Arsitektur Medallion & Tata Kelola Data

Sistem menerapkan alur pengolahan data bertingkat untuk menjamin kualitas data:

1.  **Bronze Layer (Raw Data):** Mengumpulkan data mentah dari Satudata Jakarta (2025) dan BPS (2024) dalam format asli (JSON/CSV) ke dalam HDFS sebagai *single source of truth*.
2.  **Silver Layer (Cleaned & Structured):** PySpark melakukan pembersihan data: standardisasi penulisan nama kabupaten/kota, penanganan data faskes yang kosong (*missing values*), dan transformasi tipe data. Data disimpan kembali dalam format **Parquet** yang dipartisi berdasarkan `kabupaten_kota`.
    * *Justifikasi Parquet & Partisi:* Format *columnar storage* ini mempercepat query pemfilteran wilayah tertentu melalui fitur *predicate pushdown* dan menghemat kapasitas penyimpanan HDFS.
3.  **Gold Layer (Aggregated & Insights):** Menyimpan hasil kalkulasi indikator kuantitatif seperti rasio faskes per 10.000 balita dan *Nutrition Coverage Index* (NCI). Data akhir ini diekspor ke folder lokal `./storage/gold_output/` dalam format JSON ringkas untuk langsung dikonsumsi oleh Flask Dashboard.

---

## 4. Analisis Lanjutan (Advanced Analytics) & Evaluasi

Untuk mengidentifikasi wilayah prioritas secara objektif, sistem menerapkan dua teknik analitik tingkat lanjut:

### A. Komputasi Kuantitatif Nutrition Coverage Index (NCI)
Sistem menghitung rasio matematis ketersediaan faskes penunjang terhadap beban jumlah kasus balita gizi buruk di wilayah tersebut secara berkala untuk menentukan bobot ketimpangan distribusi.

### B. K-Means Clustering (Pengelompokkan Zona Risiko Wilayah)
* **Deskripsi:** Algoritma K-Means dari **Spark MLlib** digunakan untuk mengelompokkan kabupaten/kota di DKI Jakarta menjadi 3 kluster tingkat kerentanan: *High Risk*, *Medium Risk*, dan *Low Risk* berdasarkan fitur prevalensi gizi dan rasio faskes.
* **Metode Validasi:** Menentukan jumlah kluster optimal ($K$) menggunakan *Elbow Method* dan dievaluasi secara statistik menggunakan **Silhouette Score**.
* **Metrik Evaluasi:** Mengukur jarak kedekatan antar data dalam kluster menggunakan rumus *Within Set Sum of Squared Errors* (WSSE):
    $$WSSE = \sum_{i=1}^k \sum_{x \in C_i} ||x - \mu_i||^2$$