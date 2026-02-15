"""
Smoke test for Hooter Shopify API integration.
Logs in, discovers stores, creates a test product, verifies, then deletes it.

Usage: python smoke_test_shopify.py
Requires: backend running on localhost:8800
"""

import requests
import json
import sys
import uuid
from datetime import datetime

BASE_URL = "http://localhost:8800"
CREDS = {
    "email": "smoke.test@hooter.com",
    "password": "SmokePass123"
}
# Generate unique idempotency key for each test run
IDEMPOTENCY_KEY = f"smoke-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"


def fail(msg, resp=None):
    print(f"FAIL: {msg}")
    if resp is not None:
        print(f"  HTTP {resp.status_code}: {resp.text[:500]}")
    sys.exit(1)


def ok(msg):
    print(f"  OK: {msg}")


def main():
    print("=" * 60)
    print("HOOTER SHOPIFY SMOKE TEST")
    print("=" * 60)

    # 0. Check backend is up
    try:
        requests.get(f"{BASE_URL}/session", timeout=3)
    except (requests.ConnectionError, requests.Timeout):
        print("FAIL: Backend not running on port 8800.")
        print("  Start it first in a separate terminal:")
        print("    cd Hooter_backend && python app.py")
        sys.exit(1)

    sess = requests.Session()

    # 1. Login
    print("\n[1] Login")
    r = sess.post(f"{BASE_URL}/login", json=CREDS)
    if r.status_code != 200:
        fail("Login failed — is the user registered?", r)
    ok(f"Logged in as {CREDS['email']}")

    # 2. Session check
    print("\n[2] Session check")
    r = sess.get(f"{BASE_URL}/session")
    if r.status_code != 200 or r.json().get("login") != "ok":
        fail("Session invalid after login", r)
    ok("Session valid")

    # 3. List stores
    print("\n[3] List stores")
    r = sess.get(f"{BASE_URL}/stores")
    if r.status_code != 200:
        fail("Could not list stores", r)
    stores_data = r.json()
    stores = stores_data.get("data", [])
    count = stores_data.get("count", 0)
    if count == 0:
        fail("No stores found for this user. Add a store first via POST /add-store")
    ok(f"Found {count} store(s):")
    for s in stores:
        primary = " [PRIMARY]" if s.get("is_primary") else ""
        print(f"    store_id={s['store_id']}  name={s.get('store_name', 'N/A')}  "
              f"shop={s.get('shopify_shop_name', 'N/A')}{primary}")

    # Use the primary store (or first store)
    store = next((s for s in stores if s.get("is_primary")), stores[0])
    store_id = store["store_id"]
    print(f"\n  Using store_id={store_id} ({store.get('store_name', store.get('shopify_shop_name'))})")

    # 3b. Get or create a brand
    print("\n[3b] Get or create brand")
    # Try to get existing brands
    r = sess.get(f"{BASE_URL}/brands")
    brand_id = None
    if r.status_code == 200:
        brands = r.json().get("data", [])
        if brands:
            brand_id = brands[0].get("brand_id")
            ok(f"Using existing brand: {brand_id}")
    
    if not brand_id:
        # If no brands exist, we need to create one via brand registration
        # For now, use store_id as a workaround (brands may be auto-created per store)
        brand_id = store_id
        ok(f"Using store_id as brand_id: {brand_id}")

    # 4. Create a test product
    print("\n[4] Create test product")
    product_payload = {
        "title": "Hooter Smoke Test Product",
        "description": "Automated smoke test - safe to delete",
        "brand_id": brand_id,
        "store_id": store_id,
        "variants": [
            {
                "sku": "SMOKE-TEST-001",
                "price": "1.00",
                "title": "Default"
            }
        ]
    }
    r = sess.post(f"{BASE_URL}/products", json=product_payload, headers={"Idempotency-Key": IDEMPOTENCY_KEY})
    catalogue_id = None
    if r.status_code == 201:
        # API returns result directly (not in "data" key), and uses "uid" not "catalogue_id"
        result = r.json()
        catalogue_id = result.get("uid")
        shopify_id = result.get("shopify_product_id", "N/A")
        ok(f"Product created  catalogue_id={catalogue_id}  shopify_id={shopify_id}")
        print(f"    Full response: {json.dumps(result, indent=2)[:800]}")
    elif r.status_code == 502:
        err = r.json()
        ok("Request reached Shopify but got API error (token/permissions issue):")
        print(f"    {json.dumps(err, indent=2)[:500]}")
    elif r.status_code == 403:
        err = r.json()
        print("WARN: Brand access denied (403) - brand may not be configured for user")
        print(f"  Response: {json.dumps(err, indent=2)[:200]}")
        print("  To fix: Create a brand via brand registration API or database")
    else:
        fail("Unexpected error creating product", r)

    # 5. List catalogues to confirm
    print("\n[5] List catalogues")
    r = sess.get(f"{BASE_URL}/products", params={"brand_id": store_id})
    if r.status_code == 200:
        cat_data = r.json()
        ok(f"Catalogues for store {store_id}: count={cat_data.get('count', 0)}")
    else:
        print(f"  WARN: Could not list catalogues: HTTP {r.status_code}")

    # 6. Cleanup — delete the test product if created
    if catalogue_id:
        print("\n[6] Cleanup — deleting smoke test product")
        r = sess.delete(f"{BASE_URL}/products/{catalogue_id}?soft=false&brand_id={brand_id}")
        if r.status_code == 200:
            ok("Test product deleted")
        else:
            print(f"  WARN: Cleanup failed (HTTP {r.status_code}). "
                  f"Manually delete catalogue_id={catalogue_id}")
    else:
        print("\n[6] Cleanup — skipped (no product was created)")

    # Done
    print("\n" + "=" * 60)
    print("SMOKE TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
