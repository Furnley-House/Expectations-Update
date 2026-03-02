import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import requests
from datetime import datetime
from authorizeZoho import auth


PLANS_API_BASE = "https://www.zohoapis.eu/crm/v2/Plans"

def to_yyyy_mm_dd(dt_value: Optional[str]) -> Optional[str]:
    if not dt_value or not isinstance(dt_value, str):
        return None
    try:
        return datetime.fromisoformat(dt_value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        # Fallback: take first 10 chars if it looks like YYYY-MM-DD...
        return dt_value[:10] if len(dt_value) >= 10 else None


def prepare_basic_expectations_fields() -> None:
    print("[INFO] Starting prepare_basic_expectations_fields()")

    try:
        access_token = auth("ZOHO_REFRESH_TOKEN")
    except Exception as e:
        print(f"[ERROR] Failed to get access token: {e}")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Zoho-oauthtoken {access_token}"})

    plans_cache: Dict[str, Dict[str, Any]] = {}

    def get_plan(plan_id: Optional[str]) -> Optional[Dict[str, Any]]:
        if not plan_id:
            print("[PLAN] Missing plan_id -> skip fetch")
            return None

        if plan_id in plans_cache:
            return plans_cache[plan_id]

        url = f"{PLANS_API_BASE}/{plan_id}"
        print(f"[PLAN] Fetching plan: {plan_id}")

        try:
            resp = session.get(url, timeout=100)
        except requests.RequestException as e:
            print(f"[PLAN] ERROR {plan_id}: request failed: {e}")
            return None
        except Exception as e:
            print(f"[PLAN] ERROR {plan_id}: unexpected error: {e}")
            return None

        if resp.status_code != 200:
            print(f"[PLAN] ERROR {plan_id}: HTTP {resp.status_code} | {(resp.text or '')[:1000]}")
            return None

        try:
            payload = resp.json()
        except ValueError as e:
            print(f"[PLAN] ERROR {plan_id}: invalid JSON response: {e}")
            print(f"[PLAN] Body preview: {(resp.text or '')[:1000]}")
            return None
        except Exception as e:
            print(f"[PLAN] ERROR {plan_id}: unexpected error parsing JSON: {e}")
            return None

        data = payload.get("data", [])
        if not data:
            print(f"[PLAN] No data returned for plan: {plan_id}")
            return None

        plan = data[0]
        if isinstance(plan, dict):
            plans_cache[plan_id] = plan
        return plan if isinstance(plan, dict) else None

    def build_expectation_from_fee(
        fee: dict,
        plan: dict,
        other_plan: Optional[Dict[str, Any]],
        expected_fee_amount: float,
        index: int,
    ) -> dict:
        expectation_to_upload = {
            "Index": index,
            "Client_1": ({"id": fee["Client_1"]["id"]} if fee.get("Client_1") else None),
            "Client_2": ({"id": fee["Client_2"]["id"]} if fee.get("Client_2") else None),
            "Client_Owner": (
                {"id": fee["Account_Owner"]["id"], "name": fee["Account_Owner"]["name"]}
                if fee.get("Account_Owner")
                else None
            ),
            "Deal_Name": (fee["Deal_For_Initial_Fee"]["name"] if fee.get("Deal_For_Initial_Fee") else None),
            "Owner": ({"id": fee["Owner"]["id"], "name": fee["Owner"]["name"]} if fee.get("Owner") else None),
            "Expected_Fee_Amount": round(expected_fee_amount, 2),
            "Expected_Fee_History": fee.get("Expected_Fees_History"),
            "Expected_Fees_Received": fee.get("Expected_Fees_Received"),
            "Expected_Payment_Date": to_yyyy_mm_dd(fee.get("Created_Time")),
            "Fee": fee.get("id"),
            "Fee_Type": fee.get("Fee_Type"),
            "Other_Plan_Policy_Reference": (other_plan.get("Policy_Ref") if other_plan else None),
            "Paid_By": fee.get("Paid_By"),
            "Plan_Policy_Reference": (plan.get("Policy_Ref") if plan else None),
            "Provider": (
                {"id": plan["Provider"]["id"], "name": plan["Provider"]["name"]}
                if plan and isinstance(plan.get("Provider"), dict)
                else None
            ),
            "Deal_Stage": fee.get("Deal_Stage"),
        }
        return {k: v for k, v in expectation_to_upload.items() if v is not None}

    input_path = Path(__file__).resolve().parent / "AllInitialFeesGoodForExpectationUpload.json"
    try:
        if input_path.exists():
            all_fees = json.loads(input_path.read_text(encoding="utf-8"))
        else:
            print(f"[WARN] Input file not found: {input_path}. Using empty list.")
            all_fees = []
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in {input_path}: {e}")
        return
    except OSError as e:
        print(f"[ERROR] Failed to read {input_path}: {e}")
        return
    except Exception as e:
        print(f"[ERROR] Unexpected error reading {input_path}: {e}")
        return

    if not isinstance(all_fees, list):
        print(f"[ERROR] Input JSON root must be a list, got {type(all_fees).__name__}")
        return

    expectations_payload: List[Dict[str, Any]] = []

    skipped_missing_plan = 0
    skipped_not_in_force = 0
    skipped_zero_valuation = 0
    skipped_zero_expected_fee = 0
    skipped_bad_record = 0

    total = len(all_fees)
    print(f"[INFO] Processing {total} fees...")

    for idx, fee in enumerate(all_fees, start=0):
        try:
            if not isinstance(fee, dict):
                skipped_bad_record += 1
                print(f"[SKIP] Index {idx}: expected dict, got {type(fee).__name__}")
                continue

            fee_id = fee.get("id", "UNKNOWN_FEE_ID")
            plan_ref = fee.get("Plan", {})
            plan_id = plan_ref.get("id") if isinstance(plan_ref, dict) else None

            if idx == 0 or (idx + 1) % 50 == 0 or (idx + 1) == total:
                print(
                    f"[PROGRESS] {idx + 1}/{total} fees processed | "
                    f"expectations prepared: {len(expectations_payload)}"
                )

            if not plan_id:
                skipped_missing_plan += 1
                print(f"[SKIP] Fee {fee_id}: missing Plan.id")
                continue

            plan = get_plan(plan_id)
            if plan is None:
                skipped_missing_plan += 1
                print(f"[SKIP] Fee {fee_id}: could not fetch plan {plan_id}")
                continue

            if plan.get("Plan_Status") != "In Force":
                skipped_not_in_force += 1
                print(f"[SKIP] Fee {fee_id}: plan {plan_id} status = {plan.get('Plan_Status')}")
                continue

            expected_fee_amount = fee.get("Total_Fee")
            if round(expected_fee_amount, 2) == 0:
                skipped_zero_expected_fee += 1
                print(f"[SKIP] Fee {fee_id}: expected fee is 0")
                continue

            other_plan_id = None
            other_plan_ref = fee.get("Other_Plan")
            if isinstance(other_plan_ref, dict):
                other_plan_id = other_plan_ref.get("id")

            other_plan = get_plan(other_plan_id) if other_plan_id else None

            expectations_payload.append(
                build_expectation_from_fee(fee, plan, other_plan, expected_fee_amount, idx)
            )

        except Exception as e:
            skipped_bad_record += 1
            print(f"[ERROR] Unexpected error processing index {idx}: {e}")
            continue

    print(f"[INFO] Prepared {len(expectations_payload)} expectations for upload.")
    print(
        "[SUMMARY] "
        f"total={total}, "
        f"missing_plan={skipped_missing_plan}, "
        f"not_in_force={skipped_not_in_force}, "
        f"zero_valuation={skipped_zero_valuation}, "
        f"zero_expected_fee={skipped_zero_expected_fee}, "
        f"bad_record={skipped_bad_record}"
    )
    print(f"[CACHE] Plans fetched: {len(plans_cache)}")

    output_path = Path(__file__).resolve().parent / "InitialExpectationsToUpload.json"
    try:
        output_path.write_text(json.dumps(expectations_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Wrote output to: {output_path}")
    except OSError as e:
        print(f"[ERROR] Failed to write output file {output_path}: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error writing output file {output_path}: {e}")


if __name__ == "__main__":
    prepare_basic_expectations_fields()
