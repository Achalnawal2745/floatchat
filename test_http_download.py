import requests

# Try with a float we know exists (1902669)
response = requests.post(
    "http://127.0.0.1:8000/admin/download-float",
    json={"float_id": "1902669"}
)

print("Response:", response.json())
