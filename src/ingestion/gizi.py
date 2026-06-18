import requests

'''
curl -X POST "https://satudata.jakarta.go.id/backend/api/v2/satudata/get-table-data"   -H "Content-Type: application/json"   -d '{
    "page_url": "jumlah-anak-bawah-lima-tahun-balita-bermasalah-gizi-berdasarkan-wilayah",
    "kategori": "dataset",
    "page": 1,
    "per_page": 200,
    "sort_field": null,
    "sort_order": "asc",
    "filters": {}
  }
'''

def get_gizi_data():
    url = "https://satudata.jakarta.go.id/backend/api/v2/satudata/get-table-data"
    payload = {
        "page_url": "jumlah-anak-bawah-lima-tahun-balita-bermasalah-gizi-berdasarkan-wilayah",
        "kategori": "dataset",
        "page": 1,
        "per_page": 200,
        "sort_field": None,
        "sort_order": "asc",
        "filters": {}
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()


