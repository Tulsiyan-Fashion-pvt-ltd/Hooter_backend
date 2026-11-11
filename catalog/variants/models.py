from pydantic import BaseModel, Field, BeforeValidator, ConfigDict, create_model, model_validator
from typing import Annotated, Literal
from catalog.products.models import TYPE_MAP
from typing import Type

def not_null_optional_attr(value: any) -> any:
    """
    FUNCTION TO VALIDATORS
    """
    if value is None:
        raise ValueError(f'Value can not be {None}')
    else:
        return value





class CreateVariantsModels():

    def __init__(self, listing_attributes_schema: dict|None = None, category_attributes_schema: dict|None = None):
        """Create a TYPE(BaseModel) from product's listing and category attributes"""
        self.ListingAttributesModel = self.createVariantAttributesTypeModel(listing_attributes_schema)
        self.CategoryAttributesModel = self.createVariantAttributesTypeModel(category_attributes_schema)
        self.CreateVariantModel = self.createCreateVariantModel()
        self.CreateVariantsModel = self.createCreateVariantsModel()


    def createVariantAttributesTypeModel(self, attributes: dict[str, str|bool|int]) -> Type[BaseModel]:
        """
        FUNCTION FOR DYNAMIC LISTING AND CATEGORY ATTRIBUTES OF PRODUCT FOR VARIANT CREATION.
        This functions takes a list of valid values for fields and create a pydantic validation model

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

            ### all the keys should be optional but the required type attribute's value must not be None
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

        return create_model("VariantAttributesTypeModel", 
                            **fields, 
                            __config__=ConfigDict(extra="allow"),
                            __pydantic_extra__=(
                                dict[str, list[str]],
                                Field(init=False)
                            ))


    def createCreateVariantModel(self) -> Type[BaseModel]:
        """
        Validate `listing_attributes` and `category_attributes` fields for variant creation payload for a single variant
        """
        return create_model(
            "CreateVariantModel",
            listing_attributes = (self.ListingAttributesModel, None),
            category_attributes = (self.CategoryAttributesModel| None , None)
        )


    def createCreateVariantsModel(self) -> Type[BaseModel]:
        """
        Validate list of variants paylaod to create new variants
        """
        return create_model(
            "CreateVariantsModel",
            variants = list[self.CreateVariantModel]
        )