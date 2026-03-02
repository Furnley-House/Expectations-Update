import os
from dotenv import load_dotenv
import requests


def auth(scope_usage):
    load_dotenv()
    if scope_usage == "ZOHO_REFRESH_TOKEN":
        clientScope = "ZOHO_CLIENT_ID"
        secretScope = "ZOHO_CLIENT_SECRET"
        checkAuthUrl = "https://www.zohoapis.eu/crm/v2/Plans"
    else:
        clientScope = "ZOHO_CATALYST_CLIENT_ID"
        secretScope = "ZOHO_CATALYST_CLIENT_SECRET"
        project_id = "2382000000022533"
        table_name = "Historical_Valuations"
        checkAuthUrl = f"https://api.catalyst.zoho.eu/baas/v1/project/{project_id}/table/{table_name}/row"
    refresh_token = os.getenv(scope_usage)
    client_id = os.getenv(clientScope)
    client_secret = os.getenv(secretScope)
    zohoAuthToken = os.getenv("ZOHO_OAUTH_TOKEN")
    headers = {
        "Authorization": f"Zoho-oauthtoken {zohoAuthToken}",
        "Content-Type": "application/json"
    }
    print(checkAuthUrl)
    authCheck = requests.get(checkAuthUrl,headers=headers)
    if authCheck.status_code == 401:
        print("Zoho OAuth token expired or invalid, refreshing...")
        token_url = (
            "https://accounts.zoho.eu/oauth/v2/token"
            f"?refresh_token={refresh_token}"
            f"&client_id={client_id}"
            f"&client_secret={client_secret}"
            "&grant_type=refresh_token"
        )
        token_resp = requests.post(token_url, timeout=100)
        access_token = token_resp.json().get("access_token")
        if not access_token:
            return "Failed to obtain access token"
        env_path = ".env"
        if not os.path.exists(env_path):
            env_path = "../.env"
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            for line in lines:
                if line.startswith("ZOHO_OAUTH_TOKEN="):
                    f.write(f"ZOHO_OAUTH_TOKEN={access_token}")
                else:
                    f.write(line)
        return access_token
    print("Existing Zoho OAuth token is valid.")
    return zohoAuthToken