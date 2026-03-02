import json

import requests

from authorizeZoho import auth

EXPECTED_FEES_MODULE = "Expectations"
UPLOAD_URL = f"https://www.zohoapis.eu/crm/v2/{EXPECTED_FEES_MODULE}"


def upload_expectations(batch_size: int = 100) -> None:
    print(f"[INFO] Starting upload_expectations(batch_size={batch_size})")

    try:
        access_token = auth("ZOHO_REFRESH_TOKEN")
    except Exception as e:
        print(f"[ERROR] Failed to get access token: {e}")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Zoho-oauthtoken {access_token}"})

    file_path = "InitialExpectationsToUpload.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            expectations = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {file_path!r}")
        return
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in {file_path!r}: {e}")
        return
    except OSError as e:
        print(f"[ERROR] Failed to read {file_path!r}: {e}")
        return
    except Exception as e:
        print(f"[ERROR] Unexpected error while reading {file_path!r}: {e}")
        return

    if not isinstance(expectations, list):
        print(f"[ERROR] JSON root must be a list, got {type(expectations).__name__}")
        return

    total = len(expectations)
    print(f"[INFO] Uploading {total} expectations to {EXPECTED_FEES_MODULE}...")

    if total == 0:
        print("[INFO] Nothing to upload.")
        return

    batch_num = 0
    uploaded_records = 0

    for start in range(0, total, batch_size):
        batch_num += 1
        batch = expectations[start : start + batch_size]
        body = {"data": batch}

        print(f"[INFO] Uploading batch {batch_num} ({start + 1}-{min(start + batch_size, total)} of {total})")

        try:
            resp = session.post(UPLOAD_URL, json=body, timeout=100)
        except requests.RequestException as e:
            print(f"[ERROR] Request failed for batch {batch_num}: {e}")
            print("[INFO] Stopping due to request error.")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error for batch {batch_num}: {e}")
            print("[INFO] Stopping due to unexpected error.")
            break

        try:
            resp_json = resp.json()
        except ValueError as e:
            print(f"[WARN] Response not JSON for batch {batch_num}: {e}")
            resp_json = {"raw": (resp.text or "")[:4000]}
        except Exception as e:
            print(f"[WARN] Unexpected error parsing JSON for batch {batch_num}: {e}")
            resp_json = {"raw": (resp.text or "")[:4000]}

        print(f"[INFO] Batch {batch_num}: HTTP {resp.status_code}")
        print(resp_json)

        if resp.status_code not in (200, 201, 202):
            print("[ERROR] Stopping due to HTTP error above.")
            break

        try:
            data = resp_json.get("data") or []
            success = sum(1 for r in data if (r.get("status") == "success"))
            failures = sum(1 for r in data if (r.get("status") != "success"))
            uploaded_records += success
            print(f"[INFO] Batch {batch_num} result: success={success}, failed={failures}, uploaded_total={uploaded_records}")
        except Exception as e:
            print(f"[WARN] Could not summarize batch {batch_num} response: {e}")

    print(f"[INFO] Finished. Uploaded (reported successes): {uploaded_records} / {total}")


if __name__ == "__main__":
    upload_expectations()
