import json
import os
from typing import Any


def fee_waterfall(value: float) -> float:
    fee = 0.0

    tier1 = min(value, 1_000_000)
    fee += tier1 * 0.0075

    if value > 1_000_000:
        tier2 = min(value - 1_000_000, 1_000_000)
        fee += tier2 * 0.005

    if value > 2_000_000:
        tier3 = value - 2_000_000
        fee += tier3 * 0.002

    return fee/12


def update_expectations_file(path: str = "ExpectationsUpdate/ExpectationsToUpload.json") -> list[dict[str, Any]]:
    print(f"[INFO] Starting update_expectations_file(path={path!r})")

    if not os.path.exists(path):
        print(f"[ERROR] File not found: {path!r}")
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in {path!r}: {e}")
        return []
    except OSError as e:
        print(f"[ERROR] Failed to read {path!r}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error while reading {path!r}: {e}")
        return []

    if not isinstance(data, list):
        print(f"[ERROR] Expected a JSON array (list) at root, got: {type(data).__name__}")
        return []

    cleaned: list[dict[str, Any]] = []
    skipped_null = 0
    skipped_invalid = 0

    for idx, obj in enumerate(data):
        try:
            if not isinstance(obj, dict):
                skipped_invalid += 1
                print(f"[WARN] Skipping index {idx}: expected object/dict, got {type(obj).__name__}")
                continue

            first_val = obj.get("First_Day_Valuation")
            last_val = obj.get("Last_Day_Valuation")

            if first_val is None or last_val is None:
                skipped_null += 1
                continue

            first_val_f = float(first_val)
            last_val_f = float(last_val)

            mean_val = (first_val_f + last_val_f) / 2.0
            mean_val = round(mean_val, 2)

            fee = fee_waterfall(mean_val)
            fee = round(fee, 2)

            obj["Mean_Valuation"] = mean_val
            obj["Expected_Fee_Amount"] = fee

            cleaned.append(obj)

            if (idx + 1) % 1000 == 0:
                print(f"[INFO] Processed {idx + 1} records. Cleaned so far: {len(cleaned)}")
        except (ValueError, TypeError) as e:
            skipped_invalid += 1
            print(f"[WARN] Skipping index {idx}: invalid valuation types/values ({e})")
            continue
        except Exception as e:
            skipped_invalid += 1
            print(f"[WARN] Skipping index {idx}: unexpected error ({e})")
            continue

    try:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=4)
        os.replace(tmp_path, path)
    except OSError as e:
        print(f"[ERROR] Failed to write output to {path!r}: {e}")
        return cleaned
    except Exception as e:
        print(f"[ERROR] Unexpected error while writing {path!r}: {e}")
        return cleaned

    print(
        "[INFO] Finished. "
        f"Input records: {len(data)}; Output records: {len(cleaned)}; "
        f"Skipped nulls: {skipped_null}; Skipped invalid: {skipped_invalid}"
    )
    return cleaned


if __name__ == "__main__":
    update_expectations_file()
