from pydantic import BaseModel, Field, create_model, ConfigDict
# from float import float
from typing import Literal



TYPE_MAP = {
    "string": str,
    "decimal": float,
    "integer": int,
    "boolean": bool,
    "list": list
}


class CatalogListPagitation(BaseModel):
    rows: int| None = Field(default=1, ge=1, le=20)
    page: int| None = Field(default=1, ge=1)



class ListingAttributesModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    sku_id: str = Field(..., max_length=100)
    product_title: str = Field(..., max_length=200)
    product_desc: str = Field(..., max_length=5000)
    vendor: str | None = Field(default=None, max_length=255)
    brand_name: str = Field(..., max_length=255)
    tags: str | None = Field(default=None, max_length=500)

    price: float
    compared_price: float
    purchasing_cost: float | None = None

    upc: str | None = Field(default=None, max_length=12)
    ean: str | None = Field(default=None, max_length=13)
    gtin: str | None = Field(default=None, max_length=14)
    isbn: str | None = Field(default=None, max_length=13)
    hsn_code: str | None = Field(default=None, max_length=8)

    net_weight_kg: float | None = None
    dead_weight_kg: float | None = None
    volumetric_weight_kg: float | None = None



def buildProductAttributeModel(attributes: dict[str, str|bool]):
    """
    FUNCTION FOR DYNAMIC PRODUCT DETAILS

    Args:
        attributes:
            Attributes for dynamic product details which includes `field`, `name`, `description`, `required` etc.

    Returns:
        pydantic model for product attributes
    """
    fields = {}

    for attr in attributes:
        attr_type = attr["type"]

        if attr_type == "enum":
            values = attr.get("options", [])

            if not values:
                raise ValueError(
                    f"Enum attribute '{attr['field']}' has no allowed values"
                )

            python_type = Literal.__getitem__(tuple(values))
        else:
            python_type = TYPE_MAP[attr_type]

        if not attr["required"]:
            python_type = python_type | None

        default = ... if attr["required"] else None

        constraints = {}

        if attr_type == "string" and "max_length" in attr:
            constraints["max_length"] = attr["max_length"]

        fields[attr["field"]] = (
            python_type,
            Field(default, **constraints)
        )

    return create_model("ListingAttributesModel", **fields)
