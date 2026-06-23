# ============================================
# silver_to_gold.py — Aggregation + Scoring (PySpark)
# ============================================
# Menggabungkan semua Silver datasets, menghitung:
# - Rasio indikator (faskes, posyandu, nakes per 10k)
# - Prevalensi stunting
# - Nutrition Coverage Index (NCI)
# - Nutrition Risk Score (NRS)
# - Priority ranking
#
# Sesuai Implementation Guide Section 6.6
# ============================================
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType, StringType

load_dotenv()

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")

SILVER_BASE = f"{HDFS_NAMENODE}/data/silver"
GOLD_BASE = f"{HDFS_NAMENODE}/data/gold"

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def min_max_norm(col_name):
    w = Window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    min_val = F.min(F.col(col_name)).over(w)
    max_val = F.max(F.col(col_name)).over(w)
    return F.when(
        max_val == min_val,
        F.lit(0.0)
    ).otherwise(
        (F.col(col_name) - min_val) / (max_val - min_val)
    )


def build_gold():
    print("=" * 60)
    print("SILVER → GOLD: Aggregation & Risk Scoring")
    print("=" * 60)

    print("\n[Gold] Loading Silver datasets...")

    df_gizi = spark.read.parquet(f"{SILVER_BASE}/gizi_agregat/")
    df_faskes = spark.read.parquet(f"{SILVER_BASE}/faskes_clean/")
    df_nakes = spark.read.parquet(f"{SILVER_BASE}/nakes_agregat/")
    df_pop = spark.read.parquet(f"{SILVER_BASE}/populasi_clean/")

    print(f"  Gizi agregat  : {df_gizi.count()} rows")
    print(f"  Faskes clean   : {df_faskes.count()} rows")
    print(f"  Nakes agregat  : {df_nakes.count()} rows")
    print(f"  Populasi clean : {df_pop.count()} rows")

    # ─── JOIN ─────────────────────────────────────────────────
    df_master = df_pop.select("kabupaten_kota", "populasi") \
        .join(
            df_gizi.select(
                "kabupaten_kota",
                F.coalesce(F.col("STUNTING"), F.lit(0))
                    .cast(IntegerType()).alias("total_stunting"),
                F.coalesce(F.col("GIZI BURUK"), F.lit(0))
                    .cast(IntegerType()).alias("total_gizi_buruk"),
                F.coalesce(F.col("GIZI KURANG"), F.lit(0))
                    .cast(IntegerType()).alias("total_gizi_kurang"),
                F.coalesce(F.col("UNDERWEIGHT"), F.lit(0))
                    .cast(IntegerType()).alias("total_underweight"),
            ),
            on="kabupaten_kota",
            how="left"
        ) \
        .join(
            df_faskes.select(
                "kabupaten_kota",
                "jumlah_rs_umum",
                "jumlah_rs_khusus",
                "jumlah_posyandu"
            ),
            on="kabupaten_kota",
            how="left"
        ) \
        .join(
            df_nakes.select("kabupaten_kota", "total_nakes"),
            on="kabupaten_kota",
            how="left"
        )

    # ─── FILL NULLS ───────────────────────────────────────────
    df_master = df_master \
        .na.fill(0, subset=[
            "total_stunting", "total_gizi_buruk",
            "total_gizi_kurang", "total_underweight",
            "jumlah_rs_umum", "jumlah_rs_khusus",
            "jumlah_posyandu", "total_nakes"
        ])

    # ─── TOTAL BALITA GIZI BURUK ──────────────────────────────
    df_master = df_master.withColumn(
        "total_balita_gizi_buruk",
        F.col("total_stunting") +
        F.col("total_gizi_buruk") +
        F.col("total_gizi_kurang") +
        F.col("total_underweight")
    )

    # ─── RASIO INDIKATOR ──────────────────────────────────────
    df_indicators = df_master \
        .withColumn(
            "rasio_faskes_per_10k_balita",
            F.when(
                F.col("total_balita_gizi_buruk") > 0,
                (F.col("jumlah_rs_umum") + F.col("jumlah_rs_khusus"))
                / (F.col("total_balita_gizi_buruk") / 10000)
            ).otherwise(F.lit(None).cast(FloatType()))
        ) \
        .withColumn(
            "rasio_posyandu_per_10k_populasi",
            F.when(
                F.col("populasi") > 0,
                F.col("jumlah_posyandu") / (F.col("populasi") / 10000)
            ).otherwise(F.lit(None).cast(FloatType()))
        ) \
        .withColumn(
            "rasio_nakes_per_10k_populasi",
            F.when(
                F.col("populasi") > 0,
                F.col("total_nakes") / (F.col("populasi") / 10000)
            ).otherwise(F.lit(None).cast(FloatType()))
        ) \
        .withColumn(
            # CAVEAT: penyebut = populasi TOTAL wilayah (data balita per
            # wilayah tidak tersedia). Jadi ini BUKAN prevalensi balita
            # sebenarnya, tetapi tetap valid untuk pemeringkatan relatif
            # antarwilayah (transformasi monotonik). Lihat README §3.
            "prevalensi_stunting_pct",
            F.when(
                F.col("populasi") > 0,
                (F.col("total_stunting") / F.col("populasi")) * 100
            ).otherwise(F.lit(None).cast(FloatType()))
        )

    # ─── NCI & NRS ────────────────────────────────────────────
    df_scored = df_indicators \
        .withColumn(
            "norm_stunting",
            min_max_norm("prevalensi_stunting_pct")
        ) \
        .withColumn(
            "norm_faskes_inv",
            F.lit(1.0) - min_max_norm("rasio_faskes_per_10k_balita")
        ) \
        .withColumn(
            "norm_nakes_inv",
            F.lit(1.0) - min_max_norm("rasio_nakes_per_10k_populasi")
        ) \
        .withColumn(
            "nutrition_risk_score",
            (F.col("norm_stunting") * 0.5) +
            (F.col("norm_faskes_inv") * 0.3) +
            (F.col("norm_nakes_inv") * 0.2)
        ) \
        .withColumn(
            "nutrition_coverage_index",
            (
                min_max_norm("rasio_faskes_per_10k_balita") +
                min_max_norm("rasio_posyandu_per_10k_populasi") +
                min_max_norm("rasio_nakes_per_10k_populasi")
            ) / 3.0
        ) \
        .withColumn("last_updated", F.current_timestamp())

    # ─── PRIORITY RANKING ─────────────────────────────────────
    window_rank = Window.orderBy(
        F.col("nutrition_risk_score").desc()
    )
    df_final = df_scored \
        .withColumn("priority_rank", F.rank().over(window_rank))

    # ─── SELECT FINAL COLUMNS ─────────────────────────────────
    df_final = df_final.select(
        "kabupaten_kota",
        "populasi",
        "total_balita_gizi_buruk",
        "total_stunting",
        "total_gizi_buruk",
        "total_gizi_kurang",
        "total_underweight",
        "jumlah_rs_umum",
        "jumlah_rs_khusus",
        "jumlah_posyandu",
        "total_nakes",
        F.round("rasio_faskes_per_10k_balita", 4)
            .alias("rasio_faskes_per_10k_balita"),
        F.round("rasio_posyandu_per_10k_populasi", 4)
            .alias("rasio_posyandu_per_10k_populasi"),
        F.round("rasio_nakes_per_10k_populasi", 4)
            .alias("rasio_nakes_per_10k_populasi"),
        F.round("prevalensi_stunting_pct", 2)
            .alias("prevalensi_stunting_pct"),
        F.round("nutrition_coverage_index", 4)
            .alias("nutrition_coverage_index"),
        F.round("nutrition_risk_score", 4)
            .alias("nutrition_risk_score"),
        "priority_rank",
        "last_updated"
    )

    # ─── WRITE ────────────────────────────────────────────────
    df_final.coalesce(1).write.mode("overwrite") \
        .parquet(f"{GOLD_BASE}/wilayah_risk_score/")

    print("\n[Gold] Risk scoring complete.")
    print("=" * 60)
    print("NUTRITION RISK SCORE — DKI Jakarta per Kabupaten/Kota")
    print("=" * 60)

    df_final.orderBy("priority_rank").show(
        truncate=False, n=10
    )

    return df_final


if __name__ == "__main__":
    build_gold()
    spark.stop()
