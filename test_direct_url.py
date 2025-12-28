import requests

# Try float 1902669 (we know this exists)
url = "http://data-argo.ifremer.fr/dac/incois/1902669/1902669_meta.nc"
print(f"Testing URL: {url}")

try:
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print(f"[OK] File accessible! Size: {len(response.content)} bytes")
    elif response.status_code == 403:
        print("[FAIL] 403 Forbidden - Server blocking programmatic access")
        print("This means HTTP downloads won't work either.")
        print("The server requires browser-like requests or authentication.")
    else:
        print(f"[FAIL] HTTP Error {response.status_code}")
except Exception as e:
    print(f"[ERROR] {e}")
