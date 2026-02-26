import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

def get_access_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    if not r.ok:
        print("Token request failed:", r.status_code)
        print(r.text)
        r.raise_for_status()
    return r.json()["access_token"]

if __name__ == "__main__":
    user = os.environ["CDSE_USERNAME"]
    pw = os.environ["CDSE_PASSWORD"]
    token = get_access_token(user, pw)
    print("✅ Token OK")
    print("len =", len(token))
    print("head =", token[:20], "...")