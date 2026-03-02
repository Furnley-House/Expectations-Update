import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from authorizeZoho import auth

PROJECT_ID = "2382000000022533"
TABLE_NAME = "Historical_Valuations"
API_URL = f"https://api.catalyst.zoho.eu/baas/v1/project/{PROJECT_ID}/query"
MONTH_START = "2026-01-02"
MONTH_END_EXCLUSIVE = "2026-15-02"


def run_query(query: str, access_token: str, timeout: int = 100) -> dict:
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(API_URL, headers=headers, json={"query": query}, timeout=timeout)
    except requests.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
        return {"data": []}
    except Exception as e:
        print(f"[ERROR] Unexpected error during request: {e}")
        return {"data": []}

    if resp.status_code != 200:
        print(f"[ERROR] ZCQL error {resp.status_code}: {(resp.text or '')[:1000]}")
        return {"data": []}

    try:
        return resp.json()
    except ValueError as e:
        print(f"[ERROR] Failed to parse JSON response: {e}")
        print(f"[ERROR] Body preview: {(resp.text or '')[:1000]}")
        return {"data": []}
    except Exception as e:
        print(f"[ERROR] Unexpected error parsing response JSON: {e}")
        return {"data": []}


def extract_total_valuation(zcql_response: dict):
    try:
        data = zcql_response.get("data") or []
        if not data:
            return None
        row = data[0].get("Historical_Valuations") or {}
        return row.get("Total_Valuation")
    except Exception as e:
        print(f"[ERROR] Failed to extract Total_Valuation: {e}")
        return None


def get_first_and_last_valuation(policy_reference: str, access_token: str):
    base_query = f"""
        SELECT Total_Valuation
        FROM {TABLE_NAME}
        WHERE Policy_Reference = '{policy_reference}'
          AND Date_Of_Valuation >= '{MONTH_START}'
          AND Date_Of_Valuation < '{MONTH_END_EXCLUSIVE}'
        ORDER BY Date_Of_Valuation {{direction}}
        LIMIT 1
    """

    query_first = base_query.format(direction="ASC")
    query_last = base_query.format(direction="DESC")

    print(f"[INFO] Fetching first/last valuations for Policy_Reference={policy_reference}")

    results: dict[str, dict] = {"ASC": {"data": []}, "DESC": {"data": []}}

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_map = {
            executor.submit(run_query, query_first, access_token): "ASC",
            executor.submit(run_query, query_last, access_token): "DESC",
        }

        for future in as_completed(future_map):
            direction = future_map[future]
            try:
                results[direction] = future.result()
            except Exception as e:
                print(f"[ERROR] Unexpected future error for {direction}: {e}")
                results[direction] = {"data": []}

    first_val = extract_total_valuation(results["ASC"])
    last_val = extract_total_valuation(results["DESC"])

    if first_val is None and last_val is None:
        print(f"[WARN] No valuations found for Policy_Reference={policy_reference} in range {MONTH_START}..{MONTH_END_EXCLUSIVE}")
    else:
        print(f"[INFO] Done. first={first_val} last={last_val}")

    return first_val, last_val


if __name__ == "__main__":
    policy_ref = "PF20000141F5"

    try:
        access_token = auth("ZOHO_CATALYST_REFRESH_TOKEN")
    except Exception as e:
        print(f"[ERROR] Failed to get access token: {e}")
        raise SystemExit(1)

    first, last = get_first_and_last_valuation(policy_ref, access_token)
    print("First Total_Valuation in Jan 2026:", first)
    print("Last Total_Valuation in Jan 2026:", last)
