from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class ItemResponse(BaseModel):
    item_id: int
    name: str
    quality: str
    item_class: str
    item_subclass: str
    icon_url: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class AuctionResponse(BaseModel):
    id: int
    item_id: int
    quantity: int
    unit_price: Optional[int] = None
    buyout: Optional[int] = None
    time_left: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)