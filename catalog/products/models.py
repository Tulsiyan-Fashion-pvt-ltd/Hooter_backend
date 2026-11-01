from pydantic import BaseModel, Field, create_model, ConfigDict, model_validator, BeforeValidator
# from float import float
from typing import Literal, Annotated



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
    model_config = ConfigDict(extra="forbid")

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



def buildProductAttributeModel(attributes: dict[str, str|bool|int]):
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
        attr_type = attr.get("type", "string")

        if attr_type == "enum":
            values = attr.get("options", [])

            if not values:
                raise ValueError(
                    f"Enum attribute '{attr['field']}' has no allowed values"
                )

            python_type = Literal.__getitem__(tuple(values))
        else:
            python_type = TYPE_MAP[attr_type]

        if not attr.get("required"):
            python_type = python_type | None

        default = ... if attr.get("required", False) else None

        constraints = {}

        if attr_type == "string" and "max_length" in attr:
            constraints["max_length"] = attr.get("max_length")

        fields[attr["field"]] = (
            python_type,
            Field(default, **constraints)
        )

    return create_model("ListingAttributesModel", **fields, __config__=ConfigDict(extra="allow"))



class productUpdatePayloadModel(BaseModel):
    listing_attributes: dict| None = None
    category_attributes: dict| None = None

    @model_validator(mode='before')
    @classmethod
    def validate_payload(cls, data):
        print(data)
        if "listing_attributes" in data and data.get("listing_attributes") is None:
            raise ValueError('listing_attributes must contain listing payload')
        
        if "category_attributes" in data and data.get('category_attributes') is None:
            raise ValueError('listing_attributes must contain listing payload')

        return data



def not_null_optional_attr(value: any) -> any:
    """
    FUNCTION TO VALIDATORS
    """
    if value is None:
        raise ValueError('Value can not be ', None)
    else:
        return value



def createUpdateProductAttributeModel(attributes: dict[str, str|bool|int]):
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
        attr_type = attr.get("type", "string")

        if attr_type == "enum":
            values = attr.get("options", [])

            if not values:
                raise ValueError(
                    f"Enum attribute '{attr['field']}' has no allowed values"
                )

            python_type = Literal[*values]
        else:
            python_type = TYPE_MAP[attr_type]

        default =  None
        constraints = {}

        ### Since this is for a patch method, all the keys should be optional but the required type attribute's 
        ### value must not be None
        if attr.get('required') == True:
            python_type = Annotated[python_type, BeforeValidator(not_null_optional_attr)]
        else:
            # adding the None optional null
            python_type = python_type | None

        if attr_type == "string" and "max_length" in attr:
            constraints["max_length"] = attr.get("max_length")

        fields[attr["field"]] = (
            python_type,
            Field(default, **constraints)
        )

    return create_model("UpdateProductAttributeModel", **fields, __config__=ConfigDict(extra="allow"))