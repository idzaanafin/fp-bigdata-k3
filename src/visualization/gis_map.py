# ============================================
# gis_map.py — Choropleth + Folium map
# ============================================
# Visualisasi spasial hasil Gold final (NRS, cluster, anomaly):
#   1. Choropleth Nutrition Risk Score   → output/peta_nrs.png
#   2. Peta cluster risiko               → output/peta_cluster.png
#   3. Peta interaktif (Folium + tooltip)→ output/peta_interaktif.html
#
# DESAIN GRACEFUL: jika shapefile DKI Jakarta belum tersedia di
# data/shapefiles/, script mencetak instruksi & keluar tanpa error
# (tidak menghentikan pipeline). Begitu shapefile ada, jalankan ulang.
#
# Sesuai Implementation Guide Section 6.8.
# ============================================
import glob
import os
import sys

import pandas as pd

LOCAL_OUTPUT = os.getenv("LOCAL_OUTPUT_DIR", "output")
SHAPEFILE_DIR = os.getenv("SHAPEFILE_DIR", "data/shapefiles")
GOLD_LOCAL = os.path.join(LOCAL_OUTPUT, "wilayah_final.parquet")

# 6 wilayah DKI Jakarta (bentuk standar di Gold layer)
EXPECTED_WILAYAH = {
    "JAKARTA PUSAT", "JAKARTA UTARA", "JAKARTA SELATAN",
    "JAKARTA TIMUR", "JAKARTA BARAT", "KEPULAUAN SERIBU",
}

CLUSTER_COLORS = {
    "Risiko Rendah": "#2ecc71",
    "Risiko Sedang": "#f39c12",
    "Risiko Tinggi": "#e74c3c",
}


def find_shapefile():
    """Cari .geojson / .shp di SHAPEFILE_DIR. Return path atau None."""
    if not os.path.isdir(SHAPEFILE_DIR):
        return None
    for ext in ("*.geojson", "*.json", "*.shp"):
        hits = sorted(glob.glob(os.path.join(SHAPEFILE_DIR, ext)))
        if hits:
            return hits[0]
    return None


def print_shapefile_instructions():
    print("=" * 64)
    print("SHAPEFILE BELUM ADA — peta GIS dilewati (pipeline tetap OK)")
    print("=" * 64)
    print(f"Taruh file batas wilayah di: {SHAPEFILE_DIR}/")
    print("Kriteria yang dibutuhkan:")
    print("  • Level Kabupaten/Kota DKI Jakarta (6 wilayah: 5 Kota Adm.")
    print("    + Kepulauan Seribu). BUKAN provinsi, BUKAN kecamatan.")
    print("  • Format GeoJSON (1 file) ATAU Shapefile (.shp+.shx+.dbf+.prj).")
    print("  • Ada kolom nama wilayah (mis. NAMOBJ / WADMKK / NAME_2).")
    print("  • CRS idealnya EPSG:4326 (WGS84).")
    print("  • Sumber: data.jakarta.go.id, GADM level-2 (filter DKI),")
    print("    Ina-Geoportal/BIG, atau repo GeoJSON wilayah Indonesia.")
    print("Disarankan nama file: dki_jakarta_kabupaten.geojson")
    print("=" * 64)


def normalize_name(name) -> str:
    """Normalisasi nama wilayah agar cocok dengan EXPECTED_WILAYAH."""
    if name is None:
        return ""
    s = str(name).upper().strip()
    for token in ("KOTA ADM. ", "KAB. ADM. ", "KOTA ADMINISTRASI ",
                  "KABUPATEN ADMINISTRASI ", "KOTA ", "KABUPATEN ",
                  "KAB. ", "ADM. "):
        if s.startswith(token):
            s = s[len(token):]
    s = s.replace("KEP. ", "KEPULAUAN ")
    if "SERIBU" in s:
        s = "KEPULAUAN SERIBU"
    return s.strip()


def detect_name_field(gdf):
    """Pilih kolom shapefile yang nilainya paling cocok 6 wilayah DKI."""
    best_field, best_score = None, -1
    for col in gdf.columns:
        if col == "geometry":
            continue
        try:
            vals = {normalize_name(v) for v in gdf[col].unique()}
        except TypeError:
            continue
        score = len(vals & EXPECTED_WILAYAH)
        if score > best_score:
            best_field, best_score = col, score
    return best_field, best_score


def main():
    # ─── Pastikan Gold final lokal tersedia ────────────────────
    if not os.path.exists(GOLD_LOCAL):
        print(f"[ERROR] {GOLD_LOCAL} tidak ada. "
              f"Jalankan src/analysis/ml_analysis.py dulu.")
        sys.exit(1)

    df = pd.read_parquet(GOLD_LOCAL)
    df["wilayah_key"] = df["kabupaten_kota"].map(normalize_name)
    print(f"[GIS] Gold final dimuat: {len(df)} wilayah")

    # ─── Cek shapefile (graceful jika belum ada) ───────────────
    shp = find_shapefile()
    if shp is None:
        print_shapefile_instructions()
        sys.exit(0)

    # Import GIS libs hanya jika shapefile ada
    import geopandas as gpd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import folium

    print(f"[GIS] Memuat shapefile: {shp}")
    if shp.lower().endswith((".geojson", ".json")):
        # Baca GeoJSON via shapely/from_features — hindari dependensi fiona
        # yang sering bentrok versi ('module fiona has no attribute path').
        import json as _json
        with open(shp, encoding="utf-8") as _fh:
            _gj = _json.load(_fh)
        gdf = gpd.GeoDataFrame.from_features(_gj["features"])
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)  # GeoJSON default = WGS84
    else:
        gdf = gpd.read_file(shp)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    name_field, score = detect_name_field(gdf)
    print(f"[GIS] Field nama wilayah terdeteksi: '{name_field}' "
          f"({score}/6 cocok)")
    if score < 3:
        print("[WARN] Kecocokan nama rendah. Periksa field/isi shapefile "
              "atau set SHAPEFILE_DIR ke file yang benar.")

    gdf["wilayah_key"] = gdf[name_field].map(normalize_name)
    gdf_merged = gdf.merge(df, on="wilayah_key", how="left")

    os.makedirs(LOCAL_OUTPUT, exist_ok=True)

    # ─── Plot 1: Choropleth NRS ────────────────────────────────
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    gdf_merged.plot(
        column="nutrition_risk_score", cmap="RdYlGn_r", legend=True,
        legend_kwds={"label": "Nutrition Risk Score (0=Aman, 1=Kritis)"},
        ax=ax, edgecolor="black", linewidth=0.8,
        missing_kwds={"color": "lightgrey", "label": "No Data"},
    )
    for _, row in gdf_merged.iterrows():
        if row.geometry is not None and not row.geometry.is_empty:
            # representative_point() dijamin di dalam poligon (label Kepulauan
            # Seribu tidak jatuh di laut) + tanpa warning centroid CRS geografis
            pt = row.geometry.representative_point()
            ax.annotate(str(row[name_field]).title(),
                        xy=(pt.x, pt.y), ha="center", fontsize=7)
    ax.set_title("Peta Nutrition Risk Score\nDKI Jakarta per Kabupaten/Kota",
                 fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()
    p1 = os.path.join(LOCAL_OUTPUT, "peta_nrs.png")
    plt.savefig(p1, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[GIS] {p1}")

    # ─── Plot 2: Cluster risiko ────────────────────────────────
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    gdf_merged["_color"] = gdf_merged["cluster_label"].map(
        CLUSTER_COLORS
    ).fillna("lightgrey")
    gdf_merged.plot(color=gdf_merged["_color"], ax=ax,
                    edgecolor="black", linewidth=0.8)
    patches = [mpatches.Patch(color=v, label=k)
               for k, v in CLUSTER_COLORS.items()]
    ax.legend(handles=patches, loc="lower right", fontsize=10)
    ax.set_title("Cluster Risiko Gizi Balita\nDKI Jakarta per Kabupaten/Kota",
                 fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()
    p2 = os.path.join(LOCAL_OUTPUT, "peta_cluster.png")
    plt.savefig(p2, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[GIS] {p2}")

    # ─── Peta interaktif Folium ────────────────────────────────
    m = folium.Map(location=[-6.2, 106.8], zoom_start=10,
                   tiles="CartoDB positron")

    # Layer choropleth (warna NRS). geo_data minimal (hanya kunci + geometri)
    # agar serialisasi JSON tidak tersandung tipe numpy.
    folium.Choropleth(
        geo_data=gdf_merged[["wilayah_key", "geometry"]],
        data=df, columns=["wilayah_key", "nutrition_risk_score"],
        key_on="feature.properties.wilayah_key",
        fill_color="RdYlGn_r", fill_opacity=0.75, line_opacity=0.3,
        legend_name="Nutrition Risk Score", nan_fill_color="lightgrey",
    ).add_to(m)

    # Layer tooltip — TRANSPARAN supaya warna choropleth tidak tertutup fill
    # biru default folium. Kolom display dibuat bertipe Python native agar
    # aman di-serialize (hindari numpy float/bool yang gagal json.dumps).
    gdf_tip = gdf_merged[[name_field, "geometry"]].copy()
    gdf_tip["NRS"] = gdf_merged["nutrition_risk_score"].map(
        lambda v: round(float(v), 4) if pd.notnull(v) else None)
    gdf_tip["Kategori"] = gdf_merged["cluster_label"].astype("object")
    gdf_tip["Anomali"] = gdf_merged["is_anomaly"].map(
        {True: "Ya", False: "Tidak"})
    gdf_tip["Prevalensi"] = gdf_merged["prevalensi_stunting_pct"].map(
        lambda v: round(float(v), 2) if pd.notnull(v) else None)
    gdf_tip["Prioritas"] = gdf_merged["priority_rank"].map(
        lambda v: int(v) if pd.notnull(v) else None)

    folium.GeoJson(
        gdf_tip,
        style_function=lambda _: {
            "fillColor": "#00000000", "color": "#444444",
            "weight": 0.5, "fillOpacity": 0,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=[name_field, "NRS", "Kategori", "Anomali",
                    "Prevalensi", "Prioritas"],
            aliases=["Wilayah", "NRS", "Kategori Risiko", "Anomali?",
                     "Prevalensi Stunting (%)", "Prioritas"],
            localize=True,
        ),
    ).add_to(m)

    # Pastikan ke-6 wilayah (termasuk Kepulauan Seribu di utara) terlihat
    minx, miny, maxx, maxy = gdf_merged.total_bounds
    m.fit_bounds([[float(miny), float(minx)], [float(maxy), float(maxx)]])

    p3 = os.path.join(LOCAL_OUTPUT, "peta_interaktif.html")
    m.save(p3)
    print(f"[GIS] {p3}")
    print("[GIS] Selesai — peta diekspor ke output/")


if __name__ == "__main__":
    main()
