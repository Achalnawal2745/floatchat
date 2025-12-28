import requests

print("Testing backend download endpoint with HTTP...")

# Test download
response = requests.post(
    "http://127.0.0.1:8000/admin/download-float",
    json={"float_id": "1902669"}
)

print("Download response:", response.json())

# Test ingest
response2 = requests.post(
    "http://127.0.0.1:8000/admin/ingest-float",
    json={"float_id": "1902669"}
)

print("Ingest response:", response2.json())
