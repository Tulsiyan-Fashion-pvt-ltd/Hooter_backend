"""Smoke test script for Shopify catalogue API.

Usage:
  python Hooter_backend/tests/smoke_shopify_catalogue.py \
    --base-url http://127.0.0.1:5000 \
    --email user@example.com \
    --password MyPass123 \
    --store-id 1

Notes:
- Assumes your backend is running locally.
- Uses session cookies, so keep the same process for login + API calls.
"""

import argparse
import json
import time
import requests


def login(session: requests.Session, base_url: str, email: str, password: str):
    response = session.post(
        f"{base_url}/login",
        json={"email": email, "password": password},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "ok":
        raise RuntimeError(f"Login failed: {payload}")


def create_catalogue(session: requests.Session, base_url: str, store_id: int):
    payload = {
        "title": f"Smoke Test Product {int(time.time())}",
        "description": "<p>Smoke test product</p>",
        "vendor": "SmokeVendor",
        "product_type": "SmokeType",
        "tags": "smoke,test",
        "store_id": store_id,
        "variants": [
            {
                "sku": f"SMOKE-{int(time.time())}",
                "price": "10.00",
                "compare_at_price": "12.00",
                "title": "Default",
                "weight": 0.5,
                "weight_unit": "KG"
            }
        ],
        "images": []
    }

    response = session.post(
        f"{base_url}/products",
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def get_catalogue(session: requests.Session, base_url: str, catalogue_id: str):
    response = session.get(
        f"{base_url}/products/{catalogue_id}",
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def delete_catalogue(session: requests.Session, base_url: str, catalogue_id: str):
    response = session.delete(
        f"{base_url}/products/{catalogue_id}",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--store-id", type=int, required=True)
    args = parser.parse_args()

    session = requests.Session()
    print("[1/4] Logging in...")
    login(session, args.base_url, args.email, args.password)

    print("[2/4] Creating catalogue...")
    create_response = create_catalogue(session, args.base_url, args.store_id)
    print(json.dumps(create_response, indent=2))
    catalogue_id = create_response["data"]["catalogue_id"]

    print("[3/4] Fetching catalogue...")
    get_response = get_catalogue(session, args.base_url, catalogue_id)
    print(json.dumps(get_response, indent=2))

    print("[4/4] Deleting catalogue...")
    delete_response = delete_catalogue(session, args.base_url, catalogue_id)
    print(json.dumps(delete_response, indent=2))

    print("Smoke test completed successfully.")


if __name__ == "__main__":
    main()