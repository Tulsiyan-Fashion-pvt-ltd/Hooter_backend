from pydantic import BaseModel, Field
from typing import Literal
from config import _allow_max_records_per_page


class StocksRecordModel(BaseModel):
    filter: Literal["in-stock", "out-of-stock", "low-stock", None] = None
    page: int = Field(default=1, ge=1)
    records: int = Field(default=10, ge=1, le=_allow_max_records_per_page)
    type_id: str| None = None
    warehouse_id: str| None = None