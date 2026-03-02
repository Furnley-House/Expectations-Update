import json
from pathlib import Path
from datetime import datetime, timezone

import requests

from authorizeZoho import auth


def fetch_all_fees() -> list[dict]:
    print("[INFO] Starting fetch_all_fees()")

    try:
        access_token = auth("ZOHO_REFRESH_TOKEN")
    except Exception as e:
        print(f"[ERROR] Failed to get access token: {e}")
        return []

    session = requests.Session()
    session.headers.update({"Authorization": f"Zoho-oauthtoken {access_token}"})

    fields = [
        "Client_1",
        "Client_2",
        "Account_Owner",
        "Deal_For_Initial_Fee",
        "Owner",
        "Total_Fee",
        "Expected_Fees_History",
        "Expected_Fees_Received",
        "Expected_Payment_Date",
        "id",
        "Fee_Type",
        "Other_Plan",
        "Paid_By",
        "Plan",
        "Deal_Stage",
        "Percentage",
        "Created_Time"
    ]
    fields_param = ",".join(fields)

    created_after = datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat()
    provider_id = "382102000003529852"
    criteria = (
        f"(Fee_Type:equals:Initial)"
        f"and(Provider:equals:{provider_id})"
        f"and(Created_Time:greater_than:{created_after})"
    )

    all_fees: list[dict] = []
    page = 1

    while True:
        print(f"[INFO] Fetching page {page}...")

        try:
            resp = session.get(
                "https://www.zohoapis.eu/crm/v8/Fees/search",
                params={
                    "criteria": criteria,
                    "fields": fields_param,
                    "page": page,
                    "per_page": 200,
                },
                timeout=100,
            )
        except requests.RequestException as e:
            print(f"[ERROR] Request failed on page {page}: {e}")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error on page {page}: {e}")
            break

        # Zoho sometimes returns 204 when there are no results.
        if resp.status_code == 204:
            print(f"[INFO] Page {page} returned 204 (no content). Stopping.")
            break

        if resp.status_code != 200:
            body_preview = (resp.text or "")[:2000]
            print(f"[ERROR] HTTP {resp.status_code} on page {page}: {body_preview}")
            break

        try:
            payload = resp.json()
        except ValueError as e:
            print(f"[ERROR] Failed to parse JSON on page {page}: {e}")
            print(f"[ERROR] Body preview: {(resp.text or '')[:2000]}")
            break

        data = payload.get("data") or []
        if not data:
            print(f"[INFO] No more data returned on page {page}. Stopping.")
            break

        all_fees.extend(data)
        print(f"[INFO] Page {page} returned {len(data)} records. Total so far: {len(all_fees)}")

        page += 1

    print(f"[INFO] Finished fetching. Found {len(all_fees)} fees.")

    try:
        path = Path(__file__).resolve().parent / "AllInitialFeesGoodForExpectationUpload.json"
        path.write_text(json.dumps(all_fees, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Wrote output to: {path}")
    except OSError as e:
        print(f"[ERROR] Failed to write output file: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error while writing output: {e}")

    return all_fees
fetch_all_fees()