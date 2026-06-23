# ============================================
# ml_analysis.py — K-Means + Isolation Forest
# ============================================
# Menjalankan analisis lanjutan pada Gold layer (level wilayah):
#   1. K-Means Clustering (PySpark MLlib) — 3 cluster risiko
#        - Validasi: Elbow Method (WSSE) + Silhouette Score
#        - Pelabelan cluster dinamis berdasarkan rata-rata NRS
#   2. Isolation Forest (scikit-learn) — anomaly detection
#        - Robustness check pada beberapa nilai contamination
#
# Output:
#   - HDFS  : gold/wilayah_final/            (parquet, gold + cluster + anomaly)
#   - HDFS  : gold/model_artifacts/kmeans_model/
#   - HDFS  : gold/model_artifacts/isolation_forest_model.pkl
#   - Lokal : output/wilayah_final.parquet   (untuk gis_map.py)
#   - Lokal : output/wilayah_final.json      (kontrak data untuk dashboard)
#
# Sesuai Implementation Guide Section 6.7 (dengan perbaikan:
#   pelabelan cluster dinamis, join via kabupaten_kota, port dari env).
# ============================================
import os
import subprocess
import tempfile

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler as SklearnScaler

load_dotenv()

# Kompatibilitas PySpark 3.3 ↔ pandas 2.x / numpy 1.2x:
#   • createDataFrame() memanggil DataFrame.iteritems() (dihapus pandas 2.0).
#   • toPandas() memakai np.bool / np.object dll (dihapus numpy 1.24).
# Shim ini mengembalikan alias lama tanpa mengubah perilaku.
if not hasattr(pd.DataFrame, "iteritems"):
    pd.DataFrame.iteritems = pd.DataFrame.items
for _np_alias, _py_type in (("bool", bool), ("object", object),
                            ("int", int), ("float", float), ("str", str)):
    if not hasattr(np, _np_alias):
        setattr(np, _np_alias, _py_type)

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")
GOLD_BASE = f"{HDFS_NAMENODE}/data/gold"
LOCAL_OUTPUT = os.getenv("LOCAL_OUTPUT_DIR", "output")

# Fitur untuk K-Means (gabungan beban gizi + ketersediaan layanan)
FEATURE_COLS = [
    "prevalensi_stunting_pct",
    "rasio_faskes_per_10k_balita",
    "rasio_posyandu_per_10k_populasi",
    "rasio_nakes_per_10k_populasi",
    "nutrition_coverage_index",
]

# Fitur untuk Isolation Forest (sertakan NRS sebagai sinyal risiko gabungan)
IFOREST_FEATURES = [
    "prevalensi_stunting_pct",
    "rasio_faskes_per_10k_balita",
    "rasio_posyandu_per_10k_populasi",
    "rasio_nakes_per_10k_populasi",
    "nutrition_risk_score",
]

CLUSTER_LABELS = ["Risiko Rendah", "Risiko Sedang", "Risiko Tinggi"]

spark = SparkSession.builder \
    .appName("MLAnalysis") \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Helpers
# ============================================================
def impute_median(df, cols):
    """Isi null pada kolom fitur dengan median (Spark-native approxQuantile).

    Dipakai karena data sangat kecil (6 wilayah) — drop baris akan
    membuang informasi; rasio faskes bisa null bila kasus balita = 0.
    """
    fill_values = {}
    for c in cols:
        q = df.approxQuantile(c, [0.5], 0.0)
        median = q[0] if q and q[0] is not None else 0.0
        fill_values[c] = float(median)
    return df.fillna(fill_values), fill_values


def upload_to_hdfs(local_path: str, hdfs_target: str) -> bool:
    """Upload file lokal ke HDFS (pola sama seperti consumer_to_hdfs.py)."""
    hdfs_dir = hdfs_target.rsplit("/", 1)[0]
    subprocess.run(
        ["hadoop", "fs", "-mkdir", "-p", hdfs_dir],
        check=False, capture_output=True,
    )
    result = subprocess.run(
        ["hadoop", "fs", "-put", "-f", local_path, hdfs_target],
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  [WARN] HDFS put gagal ({hdfs_target}): "
              f"{result.stderr.strip()}")
        return False
    return True


# ============================================================
# 1. K-Means Clustering (PySpark MLlib)
# ============================================================
def run_kmeans(df_gold):
    print("\n" + "=" * 60)
    print("K-MEANS CLUSTERING (PySpark MLlib)")
    print("=" * 60)

    df_imp, medians = impute_median(df_gold, FEATURE_COLS)
    print(f"[K-Means] Median imputasi: "
          f"{ {k: round(v, 4) for k, v in medians.items()} }")

    assembler = VectorAssembler(
        inputCols=FEATURE_COLS, outputCol="features_raw"
    )
    df_assembled = assembler.transform(df_imp)

    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True,
    )
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled).cache()

    n_rows = df_scaled.count()

    # ─── Elbow Method + Silhouette per k (validasi pilihan k) ───
    print("\n[K-Means] Elbow Method & Silhouette (validasi k):")
    print(f"  {'k':>3} | {'WSSE':>12} | {'Silhouette':>11}")
    print("  " + "-" * 34)
    k_max = min(5, n_rows - 1)
    for k in range(2, k_max + 1):
        km = KMeans(k=k, seed=42, featuresCol="features",
                    predictionCol="cluster_id", maxIter=20, tol=1e-4)
        model_k = km.fit(df_scaled)
        pred_k = model_k.transform(df_scaled)
        wsse = model_k.summary.trainingCost
        sil_k = ClusteringEvaluator(
            featuresCol="features", predictionCol="cluster_id",
            metricName="silhouette", distanceMeasure="squaredEuclidean",
        ).evaluate(pred_k)
        print(f"  {k:>3} | {wsse:>12.4f} | {sil_k:>11.4f}")

    # ─── Model final: k=3 (3 level risiko) ─────────────────────
    K = min(3, n_rows)
    kmeans = KMeans(k=K, seed=42, featuresCol="features",
                    predictionCol="cluster_id", maxIter=20, tol=1e-4)
    kmeans_model = kmeans.fit(df_scaled)
    df_clustered = kmeans_model.transform(df_scaled)

    silhouette = ClusteringEvaluator(
        featuresCol="features", predictionCol="cluster_id",
        metricName="silhouette", distanceMeasure="squaredEuclidean",
    ).evaluate(df_clustered)
    print(f"\n[K-Means] Model final k={K} → "
          f"Silhouette Score: {silhouette:.4f} "
          f"(>0.5 = clustering baik)")

    # ─── Pelabelan cluster DINAMIS berdasar rata-rata NRS ──────
    # (perbaikan atas template guide yang memakai map statis)
    order = df_clustered.groupBy("cluster_id") \
        .agg(F.mean("nutrition_risk_score").alias("avg_nrs")) \
        .orderBy("avg_nrs") \
        .collect()

    label_map = {}
    for i, row in enumerate(order):
        label_map[row["cluster_id"]] = CLUSTER_LABELS[
            min(i, len(CLUSTER_LABELS) - 1)
        ]
    print(f"[K-Means] Pemetaan cluster→label (by mean NRS): {label_map}")

    map_expr = F.create_map(
        [F.lit(x) for kv in label_map.items() for x in kv]
    )
    df_out = df_clustered.withColumn(
        "cluster_label", map_expr[F.col("cluster_id")]
    )

    # ─── Simpan model (overwrite agar re-run aman) ─────────────
    model_path = f"{GOLD_BASE}/model_artifacts/kmeans_model"
    kmeans_model.write().overwrite().save(model_path)
    print(f"[K-Means] Model tersimpan → {model_path}")

    df_scaled.unpersist()
    return df_out.select("kabupaten_kota", "cluster_id", "cluster_label"), \
        silhouette


# ============================================================
# 2. Isolation Forest (scikit-learn) — Anomaly Detection
# ============================================================
def run_isolation_forest(pdf_gold):
    print("\n" + "=" * 60)
    print("ISOLATION FOREST (scikit-learn) — Anomaly Detection")
    print("=" * 60)

    X = pdf_gold[IFOREST_FEATURES].copy()
    X = X.fillna(X.median(numeric_only=True))

    scaler = SklearnScaler()
    X_scaled = scaler.fit_transform(X)

    iso = IsolationForest(
        n_estimators=100, contamination=0.15, random_state=42
    )
    iso.fit(X_scaled)

    pdf = pdf_gold.copy()
    pdf["is_anomaly"] = (iso.predict(X_scaled) == -1)
    pdf["anomaly_score"] = iso.score_samples(X_scaled)

    # ─── Robustness check (tanpa ground-truth label) ───────────
    print("\n[IsoForest] Robustness check (konsistensi wilayah anomali):")
    for c in (0.10, 0.15, 0.20):
        iso_c = IsolationForest(
            n_estimators=100, contamination=c, random_state=42
        ).fit(X_scaled)
        anomalies = pdf_gold.loc[
            iso_c.predict(X_scaled) == -1, "kabupaten_kota"
        ].tolist()
        print(f"  contamination={c:.2f} → anomali: {anomalies}")

    # ─── Simpan model ke /tmp lalu upload ke HDFS ──────────────
    with tempfile.NamedTemporaryFile(
        suffix=".pkl", delete=False
    ) as tmp:
        local_model = tmp.name
    joblib.dump(iso, local_model)
    upload_to_hdfs(
        local_model,
        f"{GOLD_BASE}/model_artifacts/isolation_forest_model.pkl",
    )
    os.remove(local_model)

    print("\n[IsoForest] Skor anomali (urut paling anomali → normal):")
    show = pdf[["kabupaten_kota", "anomaly_score", "is_anomaly"]] \
        .sort_values("anomaly_score")
    print(show.to_string(index=False))

    return pdf[["kabupaten_kota", "is_anomaly", "anomaly_score"]]


# ============================================================
# 3. Ekspor Gold final ke lokal (kontrak data untuk serving)
# ============================================================
def export_local(df_final):
    os.makedirs(LOCAL_OUTPUT, exist_ok=True)
    pdf = df_final.toPandas()
    if "last_updated" in pdf.columns:
        pdf["last_updated"] = pdf["last_updated"].astype(str)

    parquet_path = os.path.join(LOCAL_OUTPUT, "wilayah_final.parquet")
    json_path = os.path.join(LOCAL_OUTPUT, "wilayah_final.json")

    pdf.to_parquet(parquet_path, index=False)
    pdf.to_json(json_path, orient="records", indent=2, force_ascii=False)

    print(f"\n[Export] {parquet_path}")
    print(f"[Export] {json_path}  ({len(pdf)} records)")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("ML ANALYSIS — Gold Layer (level wilayah)")
    print(f"Gold: {GOLD_BASE}")
    print("=" * 60)

    df_gold = spark.read.parquet(f"{GOLD_BASE}/wilayah_risk_score/")
    n = df_gold.count()
    print(f"[Load] Gold wilayah_risk_score: {n} baris")
    if n == 0:
        print("[ERROR] Gold kosong. Jalankan silver_to_gold.py dulu.")
        spark.stop()
        return

    # Cast timestamp → string ISO agar toPandas() aman di pandas 2.x
    # (pandas menolak cast ke 'datetime64' tanpa unit) sekaligus konsisten
    # dengan kontrak JSON (last_updated = string ISO).
    df_gold = df_gold.withColumn(
        "last_updated",
        F.date_format(F.col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss"),
    )

    # 1. K-Means (Spark)
    df_clusters, silhouette = run_kmeans(df_gold)

    # 2. Isolation Forest (Pandas — data kecil)
    pdf_gold = df_gold.toPandas()
    pdf_iso = run_isolation_forest(pdf_gold)
    df_iso = spark.createDataFrame(pdf_iso)

    # 3. Gabung via kabupaten_kota (bukan positional — robust)
    df_final = df_gold \
        .join(df_clusters, on="kabupaten_kota", how="left") \
        .join(df_iso, on="kabupaten_kota", how="left") \
        .orderBy("priority_rank")

    # 4. Tulis Gold final ke HDFS
    df_final.coalesce(1).write.mode("overwrite") \
        .parquet(f"{GOLD_BASE}/wilayah_final/")

    # 5. Ekspor lokal (untuk GIS + dashboard teman)
    export_local(df_final)

    print("\n" + "=" * 60)
    print("HASIL AKHIR — Wilayah + Cluster + Anomaly (urut prioritas)")
    print("=" * 60)
    df_final.select(
        "priority_rank", "kabupaten_kota",
        F.round("nutrition_risk_score", 4).alias("NRS"),
        "cluster_label",
        F.round("anomaly_score", 4).alias("anomaly_score"),
        "is_anomaly",
    ).orderBy("priority_rank").show(truncate=False, n=10)

    print(f"[Done] Silhouette={silhouette:.4f} | "
          f"Gold final → {GOLD_BASE}/wilayah_final/")
    spark.stop()


if __name__ == "__main__":
    main()
