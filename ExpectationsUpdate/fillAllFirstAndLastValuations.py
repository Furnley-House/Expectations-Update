import json

from authorizeZoho import auth
from ExpectationsUpdate.getFirstAndLastValuation import get_first_and_last_valuation


def fill_all_first_and_last_valuations() -> str:
    file_path = "ExpectationsUpdate/ExpectationsToUpload.json"
    print(f"[INFO] Loading expectations from {file_path!r}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            expectations = json.load(f)
    except FileNotFoundError:
        msg = f"[ERROR] File not found: {file_path!r}"
        print(msg)
        return msg
    except json.JSONDecodeError as e:
        msg = f"[ERROR] Invalid JSON in {file_path!r}: {e}"
        print(msg)
        return msg
    except OSError as e:
        msg = f"[ERROR] Failed to read {file_path!r}: {e}"
        print(msg)
        return msg
    except Exception as e:
        msg = f"[ERROR] Unexpected error while reading {file_path!r}: {e}"
        print(msg)
        return msg

    if not isinstance(expectations, list):
        msg = "[ERROR] JSON root must be a list of expectations."
        print(msg)
        return msg

    try:
        access_token = auth("ZOHO_CATALYST_REFRESH_TOKEN")
    except Exception as e:
        msg = f"[ERROR] Failed to get access token: {e}"
        print(msg)
        return msg

    total = len(expectations)
    print(f"[INFO] There are {total} expectations to process.")

    processed = 0
    skipped = 0
    failed = 0
    refreshed = 0

    for i, expectation in enumerate(expectations):
        exp_id = None
        try:
            if not isinstance(expectation, dict):
                skipped += 1
                print(f"[WARN] Skipping index {i}: expected dict, got {type(expectation).__name__}")
                continue

            policy_ref = expectation.get("Plan_Policy_Reference")
            exp_id = expectation.get("id", f"index_{i}")

            if not policy_ref:
                skipped += 1
                print(f"[WARN] Skipping expectation {exp_id}: missing Plan_Policy_Reference")
                continue

            print(f"[INFO] Processing expectation {exp_id} | Policy Ref: {policy_ref}")

            first = last = None
            try:
                first, last = get_first_and_last_valuation(policy_ref, access_token)
            except Exception as e:
                print(f"[WARN] Error fetching valuations for {exp_id}: {e}")
                print("[INFO] Refreshing token and retrying once...")
                try:
                    access_token = auth("ZOHO_CATALYST_REFRESH_TOKEN")
                    refreshed += 1
                except Exception as e2:
                    failed += 1
                    print(f"[ERROR] Token refresh failed for {exp_id}: {e2}")
                    continue

                try:
                    first, last = get_first_and_last_valuation(policy_ref, access_token)
                except Exception as e3:
                    failed += 1
                    print(f"[ERROR] Retry failed for {exp_id}: {e3}")
                    continue

            expectation["First_Day_Valuation"] = first
            expectation["Last_Day_Valuation"] = last
            processed += 1

            if (i + 1) % 100 == 0:
                print(
                    "[INFO] Progress "
                    f"{i + 1}/{total} | processed={processed}, skipped={skipped}, failed={failed}, token_refreshes={refreshed}"
                )

        except Exception as e:
            failed += 1
            print(f"[ERROR] Unexpected error at index {i} (exp_id={exp_id}): {e}")
            continue

    print(
        "[INFO] Completed processing. "
        f"processed={processed}, skipped={skipped}, failed={failed}, token_refreshes={refreshed}"
    )

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(expectations, f, indent=4)
        print(f"[INFO] Wrote updated expectations to {file_path!r}")
    except OSError as e:
        msg = f"[ERROR] Failed to write {file_path!r}: {e}"
        print(msg)
        return msg
    except Exception as e:
        msg = f"[ERROR] Unexpected error while writing {file_path!r}: {e}"
        print(msg)
        return msg

    return f"uploaded valuations in-place. Total items in file: {len(expectations)}"


if __name__ == "__main__":
    print(fill_all_first_and_last_valuations())
