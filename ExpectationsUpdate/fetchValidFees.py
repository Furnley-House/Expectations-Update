import json
import math
from pathlib import Path

import requests

from authorizeZoho import auth


class NoContent204Error(Exception):
    """Raised when Zoho returns HTTP 204 for the first page of a requested chunk."""
    pass


def fetch_all_fees(chunk: int = 1, chunk_size: int = 2000, per_page: int = 200):
    """
    Fetch one chunk of ongoing percentage fees.

    chunk=1 -> rows 1..2000
    chunk=2 -> rows 2001..4000
    chunk=3 -> rows 4001..6000
    etc.

    Returns up to `chunk_size` rows.
    Raises NoContent204Error if there are no rows in that chunk (HTTP 204 on first page).
    """
    if chunk < 1:
        raise ValueError("chunk must be >= 1")
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    if per_page < 1:
        raise ValueError("per_page must be >= 1")

    print(f"[INFO] Starting fetch_all_fees(chunk={chunk}, chunk_size={chunk_size}, per_page={per_page})")

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
    ]
    fields_param = ",".join(fields)

    criteria = (
        "(Fee_Type:equals:Ongoing)"
        "and(Provider:equals:382102000003529852)"
        "and(Charging_type:equals:Percentage of FUM)"
        "and(Owner:not_equal:382102000067457001)"
        "and(Owner:not_equal:382102000067458001)"
        "and(Owner:not_equal:382102000046272395)"
        "and((Plan.id:starts_with:1)"
        "or(Plan.id:starts_with:2)"
        "or(Plan.id:starts_with:3)"
        "or(Plan.id:starts_with:4)"
        "or(Plan.id:starts_with:5)"
        "or(Plan.id:starts_with:6)"
        "or(Plan.id:starts_with:7)"
        "or(Plan.id:starts_with:8)"
        "or(Plan.id:starts_with:9))"
    )

    # Map chunk -> page window
    # chunk=1 => pages 1..10 (for per_page=200 and chunk_size=2000)
    first_page = ((chunk - 1) * chunk_size) // per_page + 1
    pages_to_fetch = math.ceil(chunk_size / per_page)

    start_row = (chunk - 1) * chunk_size + 1
    end_row = chunk * chunk_size

    print(f"[INFO] Requested chunk rows: {start_row}-{end_row}")
    print(f"[INFO] Page window: {first_page}-{first_page + pages_to_fetch - 1}")

    all_fees: list[dict] = []
    base_url = "https://www.zohoapis.eu/crm/v8/Fees/search"

    for offset in range(pages_to_fetch):
        page = first_page + offset
        params = {
            "criteria": criteria,
            "fields": fields_param,
            "page": page,
            "per_page": per_page,
        }

        print(f"[INFO] Fetching page {page}...")

        try:
            resp = session.get(base_url, params=params, timeout=100)
        except requests.RequestException as e:
            print(f"[ERROR] Request failed on page {page}: {e}")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected request error on page {page}: {e}")
            break

        # Zoho returns 204 when there is no content for this page range
        if resp.status_code == 204:
            if not all_fees:
                # No rows at all for requested chunk
                raise NoContent204Error(
                    f"204 No Content: no rows in chunk {chunk} (rows {start_row}-{end_row})."
                )
            print(f"[INFO] HTTP 204 on page {page}. Reached end of data for this chunk.")
            break

        if resp.status_code != 200:
            body_preview = (resp.text or "")[:1000]
            print(f"[ERROR] HTTP {resp.status_code} on page {page}: {body_preview}")
            break

        try:
            payload = resp.json()
        except ValueError as e:
            print(f"[ERROR] Failed to parse JSON on page {page}: {e}")
            print(f"[ERROR] Body preview: {(resp.text or '')[:1000]}")
            break

        data = payload.get("data", [])
        if not data:
            print(f"[INFO] Empty data on page {page}. Stopping.")
            break

        all_fees.extend(data)
        print(f"[INFO] Page {page} returned {len(data)} records. Chunk total: {len(all_fees)}")

        # Hard cap to chunk_size in case API gives more than expected
        if len(all_fees) >= chunk_size:
            all_fees = all_fees[:chunk_size]
            break

    print(f"[INFO] Finished chunk {chunk}. Fetched {len(all_fees)} rows.")

    try:
        out_name = f"AllFeesGoodForExpectationUpload.json"
        path = Path(__file__).resolve().parent / out_name
        path.write_text(json.dumps(all_fees, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Wrote output to: {path}")
    except OSError as e:
        print(f"[ERROR] Failed to write output file: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error while writing output: {e}")

    return None


if __name__ == "__main__":
    try:
        fetch_all_fees(1)
    except NoContent204Error as e:
        print(f"[ERROR] {e}")
