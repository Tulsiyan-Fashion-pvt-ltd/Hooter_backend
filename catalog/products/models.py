from pydantic import BaseModel, Field


class CatalogListPagitation(BaseModel):
    rows: int| None = Field(default=1, ge=1, le=20)
    page: int| None = Field(default=1, ge=1)