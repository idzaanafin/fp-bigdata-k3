import json
from pathlib import Path
from statistics import mean


ROOT = Path("/app")
OUTPUT = ROOT / "output"


def load_data():
    with open(OUTPUT / "wilayah_final.json", "r") as f:
        return json.load(f)


def summary(data):

    avg_risk = round(
        mean(x["nutrition_risk_score"] for x in data),
        3
    )

    total_posyandu = sum(
        x["jumlah_posyandu"]
        for x in data
    )

    total_nakes = sum(
        x["total_nakes"]
        for x in data
    )

    anomaly = sum(
        1 for x in data
        if x["is_anomaly"]
    )

    highest = max(
        data,
        key=lambda x:
        x["nutrition_risk_score"]
    )

    coverage = max(
        data,
        key=lambda x:
        x["nutrition_coverage_index"]
    )

    return {
        "wilayah": len(data),
        "avg_risk": avg_risk,
        "posyandu": total_posyandu,
        "nakes": total_nakes,
        "anomaly": anomaly,
        "highest": highest,
        "coverage": coverage
    }


def insights(data):

    sorted_risk = sorted(
        data,
        key=lambda x:
        x["nutrition_risk_score"],
        reverse=True
    )

    return [
        {
            "title": "Prioritas Intervensi",
            "body":
            f'{sorted_risk[0]["kabupaten_kota"]} '
            f'memiliki Nutrition Risk '
            f'Score tertinggi '
            f'({sorted_risk[0]["nutrition_risk_score"]})'
        },

        {
            "title": "Coverage Tertinggi",
            "body":
            f'{max(data,key=lambda x:x["nutrition_coverage_index"])["kabupaten_kota"]}'
        },

        {
            "title": "Audit Distribusi",
            "body":
            "Perbedaan rasio fasilitas "
            "dan tenaga kesehatan "
            "menunjukkan ketimpangan."
        }

    ]


def load_dashboard():

    data = load_data()

    return {
        "summary": summary(data),
        "regions":
        sorted(
            data,
            key=lambda x:
            x["priority_rank"]
        ),
        "insights":
        insights(data)
    }
