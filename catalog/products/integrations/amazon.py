from platforms.amazon.client import AmazonAPIClient

async def upload_product(brand_id: str, selling_partner_id: str, product: dict,) -> dict:

    required_fields = ["sku", "marketplace_id", "product_type", "amazon_attributes",]

    missing_fields = [
        field for field in required_fields
        if product.get(field) is None or product.get(field) == ""
    ]

    if missing_fields:
        return {
            "success": False,
            "status": "MISSING_REQUIRED_FIELDS",
            "errors": [
                f"Missing required field: {field}"
                for field in missing_fields
            ],
        }

    body = {
        "productType": product["product_type"],
        "requirements": "LISTING",
        "attributes": product["amazon_attributes"],
    }

    amazon_client = AmazonAPIClient(brand_id=brand_id, selling_partner_id=selling_partner_id,)

    try:
        response = await amazon_client.put_listings_item(
            sku=product["sku"],
            marketplace_id=product["marketplace_id"],
            body=body,
        )

    except Exception as e:
        return {"success": False, "status": "API_ERROR", "message": str(e),}

    status = response.get("status")
    submission_id = response.get("submissionId")
    issues = response.get("issues") or []

    if status == "ACCEPTED":
        return {"success": True, "status": status, "submission_id": submission_id, "issues": issues,}

    if status == "VALID":
        return {"success": True, "status": status, "submission_id": submission_id, "issues": issues, "message": "Validation passed; this response does not mean the listing was submitted.",}

    if status == "INVALID":
        return {"success": False, "status": status, "submission_id": submission_id, "issues": issues,}

    return {"success": False, "status": status or "UNKNOWN", "submission_id": submission_id, "issues": issues, "response": response,}



async def update_product_stock(brand_id: str, selling_partner_id: str, product: dict,) -> dict:
    required_fields = ["sku", "marketplace_id", "product_type", "quantity",]

    missing_fields = [
        field for field in required_fields
        if product.get(field) is None or product.get(field) == ""
    ]

    if missing_fields:
        return {
            "success": False,
            "status": "MISSING_REQUIRED_FIELDS",
            "errors": [
                f"Missing required field: {field}"
                for field in missing_fields
            ],
        }

    quantity = product["quantity"]

    if (isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0):
        return {"success": False, "status": "INVALID_QUANTITY", "message": "quantity must be a non-negative integer",}

    patches = [
        {
            "op": "merge",
            "path": "/attributes/fulfillment_availability",
            "value": [
                {
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": quantity,
                }
            ],
        }
    ]

    # Required PATCH request body
    body = {
        "productType": product["product_type"],
        "patches": patches,
    }

    amazon_client = AmazonAPIClient(brand_id=brand_id, selling_partner_id=selling_partner_id,)

    try:
        response = await amazon_client.patch_listings_item(
            sku=product["sku"],
            marketplace_id=product["marketplace_id"],
            body=body,
        )
    except Exception as e:
        return {"success": False, "status": "API_ERROR", "message": str(e),}

    status = response.get("status")
    submission_id = response.get("submissionId")
    issues = response.get("issues") or []

    if status == "ACCEPTED":
        return {"success": True, "status": status, "submission_id": submission_id, "issues": issues, "message": "Amazon accepted the listing submission for processing.",}

    if status == "VALID":
        return {"success": False, "status": status, "submission_id": submission_id, "issues": issues, "message": "Validation passed; the listing was not submitted.",
        }

    return {"success": False, "status": status or "UNKNOWN", "submission_id": submission_id, "issues": issues, "response": response,}
