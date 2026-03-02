import json
from pathlib import Path
from typing import Any, Dict, List, Set, Optional

import requests
from authorizeZoho import auth


PLANS_BASE = "https://www.zohoapis.eu/crm/v8/Plans"
FEES_SEARCH = "https://www.zohoapis.eu/crm/v8/Fees/search"


def fetch_in_force_plans_without_fees() -> List[Dict[str, Any]]:
    access_token = auth("ZOHO_REFRESH_TOKEN")

    session = requests.Session()
    session.headers.update({"Authorization": f"Zoho-oauthtoken {access_token}"})

    # 1) All In Force plans
    in_force_plans: List[Dict[str, Any]] = []
    in_force_plan_ids: Set[str] = set()

    page = 1
    per_page = 200
    while True:
        resp = session.get(
            PLANS_BASE,
            params={
                "criteria": "(Plan_Status:equals:In Force)",
                "fields": "id,Policy_Ref,Plan_Status,Provider,Owner,Created_Time,Modified_Time",
                "page": page,
                "per_page": per_page,
            },
            timeout=100,
        )

        if resp.status_code == 204:
            break
        if resp.status_code != 200:
            raise RuntimeError(f"Plans HTTP {resp.status_code}: {(resp.text or '')[:2000]}")

        payload = resp.json()
        data = payload.get("data") or []
        if not data:
            break

        in_force_plans.extend(data)
        for p in data:
            pid = p.get("id")
            if pid:
                in_force_plan_ids.add(str(pid))

        page += 1

    # 2) All plan IDs that appear in any Fee
    fee_plan_ids: Set[str] = set()

    page = 1
    while True:
        resp = session.get(
            FEES_SEARCH,
            params={
                "criteria": "(Plan:not_equal:null)",
                "fields": "id,Plan",
                "page": page,
                "per_page": 200,
            },
            timeout=100,
        )

        if resp.status_code == 204:
            break
        if resp.status_code != 200:
            raise RuntimeError(f"Fees HTTP {resp.status_code}: {(resp.text or '')[:2000]}")

        payload = resp.json()
        data = payload.get("data") or []
        if not data:
            break

        for fee in data:
            plan = fee.get("Plan")
            if isinstance(plan, dict) and plan.get("id"):
                fee_plan_ids.add(str(plan["id"]))

        page += 1

    # 3) Set difference
    missing_fee_ids = in_force_plan_ids - fee_plan_ids

    # 4) Return full plan records for those IDs
    plans_without_fees = [p for p in in_force_plans if str(p.get("id")) in missing_fee_ids]

    out_path = Path(__file__).resolve().parent / "Plans_InForce_WithoutFees.json"
    out_path.write_text(json.dumps(plans_without_fees, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[INFO] In Force plans: {len(in_force_plans)}")
    print(f"[INFO] Plans referenced by any fee: {len(fee_plan_ids)}")
    print(f"[INFO] In Force plans without any fee: {len(plans_without_fees)}")
    print(f"[INFO] Wrote: {out_path}")

    return plans_without_fees
fetch_in_force_plans_without_fees()