# ============================================
# bronze_to_silver.py — ETL Bronze → Silver (PySpark)
# ============================================
# Membersihkan, menstandarisasi, dan menormalkan data
# dari Bronze layer untuk semua dataset:
# - Gizi Balita (aggregated + detail)
# - Fasilitas Kesehatan
# - Tenaga Kesehatan (aggregated)
# - Populasi
#
# Sesuai Implementation Guide Section 6.5
# ============================================
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

load_dotenv()

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")

BRONZE_BASE = f"{HDFS_NAMENODE}/data/bronze"
SILVER_BASE = f"{HDFS_NAMENODE}/data/silver"

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Wilayah mapping — semua varian → standar "JAKARTA PUSAT" dst
# ============================================================
WILAYAH_MAP = {
    # Gizi / Nakes style (KOTA ADM. / KAB. ADM.)
    "KOTA ADM. JAKARTA PUSAT": "JAKARTA PUSAT",
    "KOTA ADM. JAKARTA SELATAN": "JAKARTA SELATAN",
    "KOTA ADM. JAKARTA TIMUR": "JAKARTA TIMUR",
    "KOTA ADM. JAKARTA UTARA": "JAKARTA UTARA",
    "KOTA ADM. JAKARTA BARAT": "JAKARTA BARAT",
    "KAB. ADM. KEP. SERIBU": "KEPULAUAN SERIBU",
    # Nakes style (without prefix)
    "JAKARTA PUSAT": "JAKARTA PUSAT",
    "JAKARTA SELATAN": "JAKARTA SELATAN",
    "JAKARTA TIMUR": "JAKARTA TIMUR",
    "JAKARTA UTARA": "JAKARTA UTARA",
    "JAKARTA BARAT": "JAKARTA BARAT",
    "KEPULAUAN SERIBU": "KEPULAUAN SERIBU",
    # Faskes / Populasi style (Kota ... / Kab. ...)
    "KEPULAUAN SERIBU": "KEPULAUAN SERIBU",
    "KOTA JAKARTA SELATAN": "JAKARTA SELATAN",
    "KOTA JAKARTA TIMUR": "JAKARTA TIMUR",
    "KOTA JAKARTA PUSAT": "JAKARTA PUSAT",
    "KOTA JAKARTA BARAT": "JAKARTA BARAT",
    "KOTA JAKARTA UTARA": "JAKARTA UTARA",
}

mapping_expr = F.create_map(
    [F.lit(x) for pair in WILAYAH_MAP.items() for x in pair]
)


def standardize_wilayah(col):
    raw = F.upper(F.trim(col))
    mapped = mapping_expr[raw]
    return F.coalesce(mapped, raw)


# ============================================================
# 1. GIZI BALITA
# ============================================================
def clean_gizi():
    print("\n[Silver] Processing Gizi Balita...")

    df = spark.read.parquet(f"{BRONZE_BASE}/gizi/")

    df_clean = df \
        .withColumn("wilayah_std", standardize_wilayah(F.col("wilayah"))) \
        .withColumn(
            "kategori_std",
            F.when(
                F.upper(F.trim(F.col("kategori_masalah_gizi"))) == "BALITA STUNTING",
                F.lit("STUNTING")
            ).otherwise(F.upper(F.trim(F.col("kategori_masalah_gizi"))))
        ) \
        .withColumn("jumlah", F.col("jumlah").cast(IntegerType())) \
        .filter(F.col("jumlah").isNotNull() & (F.col("jumlah") >= 0)) \
        .dropDuplicates([
            "periode_data", "wilayah_std", "kecamatan",
            "kategori_std"
        ])

    valid_kategori = ["STUNTING", "GIZI BURUK", "GIZI KURANG",
                      "UNDERWEIGHT", "WASTING", "OVERWEIGHT"]
    df_clean = df_clean.filter(F.col("kategori_std").isin(valid_kategori))

    df_detail = df_clean.select(
        "periode_data",
        F.col("wilayah_std").alias("kabupaten_kota"),
        "kecamatan",
        F.col("kategori_std").alias("kategori_masalah_gizi"),
        "jumlah",
        "ingested_at",
        "source",
        "ingested_batch"
    )

    df_detail.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/gizi_kecamatan_clean/")

    df_agg = df_clean.groupBy("wilayah_std", "periode_data") \
        .pivot("kategori_std", valid_kategori) \
        .agg(F.sum("jumlah")) \
        .fillna(0) \
        .withColumnRenamed("wilayah_std", "kabupaten_kota")

    df_agg.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/gizi_agregat/")

    row_count = df_clean.count()
    print(f"  → Gizi: {row_count} rows cleaned, "
          f"{df_agg.count()} kabupaten aggregated")


# ============================================================
# 2. FASILITAS KESEHATAN
# ============================================================
def clean_faskes():
    print("\n[Silver] Processing Fasilitas Kesehatan...")

    df = spark.read.parquet(f"{BRONZE_BASE}/faskes/")

    df_clean = df \
        .withColumn(
            "kabupaten_kota_raw",
            F.upper(F.trim(F.col("`Kabupaten/Kota`")))
        ) \
        .filter(~F.col("kabupaten_kota_raw").isin(
            ["DKI JAKARTA", ""]
        )) \
        .filter(F.col("kabupaten_kota_raw").isNotNull()) \
        .withColumn(
            "kabupaten_kota",
            standardize_wilayah(F.col("`Kabupaten/Kota`"))
        )

    for col_name in [
        "`Jumlah Rumah Sakit Umum`",
        "`Jumlah Rumah Sakit Khusus`",
        "`Jumlah Posyandu`"
    ]:
        df_clean = df_clean \
            .withColumn(
                col_name,
                F.when(
                    F.trim(F.col(col_name)) == "–", F.lit("0")
                ).otherwise(F.col(col_name))
            )

    df_clean = df_clean \
        .withColumn("jumlah_rs_umum",
                    F.col("`Jumlah Rumah Sakit Umum`").cast(IntegerType())) \
        .withColumn("jumlah_rs_khusus",
                    F.col("`Jumlah Rumah Sakit Khusus`").cast(IntegerType())) \
        .withColumn("jumlah_posyandu",
                    F.col("`Jumlah Posyandu`").cast(IntegerType())) \
        .na.fill(0, subset=[
            "jumlah_rs_umum", "jumlah_rs_khusus", "jumlah_posyandu"
        ])

    df_out = df_clean.select(
        "kabupaten_kota",
        "jumlah_rs_umum",
        "jumlah_rs_khusus",
        "jumlah_posyandu",
        "ingested_at",
        "source",
        "ingested_batch"
    )

    df_out.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/faskes_clean/")

    row_count = df_out.count()
    print(f"  → Faskes: {row_count} rows cleaned")


# ============================================================
# 3. TENAGA KESEHATAN
# ============================================================
def clean_nakes():
    print("\n[Silver] Processing Tenaga Kesehatan...")

    df = spark.read.parquet(f"{BRONZE_BASE}/nakes/")

    df_clean = df \
        .withColumn("wilayah_std", standardize_wilayah(F.col("wilayah"))) \
        .withColumn("jumlah", F.col("jumlah").cast(IntegerType())) \
        .filter(F.col("jumlah").isNotNull() & (F.col("jumlah") >= 0)) \
        .dropDuplicates([
            "periode_data", "wilayah_std", "kecamatan",
            "tenaga_kesehatan"
        ])

    df_detail = df_clean.select(
        "periode_data",
        F.col("wilayah_std").alias("kabupaten_kota"),
        "kecamatan",
        "tenaga_kesehatan",
        "jumlah",
        "ingested_at",
        "source",
        "ingested_batch"
    )

    df_detail.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/nakes_kecamatan_clean/")

    df_agg = df_clean.groupBy("wilayah_std", "periode_data") \
        .agg(F.sum("jumlah").alias("total_nakes")) \
        .withColumnRenamed("wilayah_std", "kabupaten_kota")

    df_agg.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/nakes_agregat/")

    row_count = df_clean.count()
    print(f"  → Nakes: {row_count} rows cleaned, "
          f"{df_agg.count()} kabupaten aggregated")


# ============================================================
# 4. POPULASI
# ============================================================
def clean_populasi():
    print("\n[Silver] Processing Populasi...")

    df = spark.read.parquet(f"{BRONZE_BASE}/populasi/")

    df_clean = df \
        .withColumn(
            "kabupaten_kota_raw",
            F.upper(F.trim(F.col("`Kabupaten/Kota`")))
        ) \
        .filter(~F.col("kabupaten_kota_raw").isin(
            ["DKI JAKARTA", "", "CATATAN"]
        )) \
        .filter(~F.col("kabupaten_kota_raw").startswith("HASIL")) \
        .filter(~F.col("kabupaten_kota_raw").startswith("LAJU")) \
        .filter(F.col("kabupaten_kota_raw").isNotNull()) \
        .withColumn(
            "kabupaten_kota",
            standardize_wilayah(F.col("`Kabupaten/Kota`"))
        ) \
        .withColumn(
            "population_thousand",
            F.col("`Population (Thousand)`").cast(FloatType())
        ) \
        .filter(F.col("population_thousand").isNotNull()) \
        .filter(F.col("population_thousand") > 0)

    df_out = df_clean \
        .withColumn(
            "populasi",
            (F.col("population_thousand") * 1000).cast(IntegerType())
        ) \
        .withColumn(
            "population_growth_rate",
            F.col("`Population Growth Rate`").cast(FloatType())
        ) \
        .withColumn(
            "percentage_of_total",
            F.col("`Percentage of Total Population`").cast(FloatType())
        ) \
        .withColumn(
            "density_per_sqkm",
            F.col("`Population Density per sq.km (Km2)`").cast(FloatType())
        ) \
        .withColumn(
            "sex_ratio",
            F.col("`Population Sex Ratio`").cast(FloatType())
        ) \
        .select(
            "kabupaten_kota",
            "populasi",
            "population_growth_rate",
            "percentage_of_total",
            "density_per_sqkm",
            "sex_ratio",
            "ingested_at",
            "source",
            "ingested_batch"
        )

    df_out.coalesce(1).write.mode("overwrite") \
        .partitionBy("kabupaten_kota") \
        .parquet(f"{SILVER_BASE}/populasi_clean/")

    row_count = df_out.count()
    print(f"  → Populasi: {row_count} rows cleaned")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("BRONZE → SILVER ETL")
    print(f"Bronze: {BRONZE_BASE}")
    print(f"Silver: {SILVER_BASE}")
    print("=" * 60)

    clean_gizi()
    clean_faskes()
    clean_nakes()
    clean_populasi()

    print("\n[Silver] All datasets processed. Spark stopped.")
    spark.stop()
