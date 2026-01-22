from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import date, datetime

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

class DailyMarketSummaryResponse(BaseModel):
    item_id: int
    snapshot_date: date
    item_name: Optional[str] = None
    icon_url: Optional[str] = None
    min_buyout: Optional[int] = None
    max_buyout: Optional[int] = None
    median_buyout: Optional[int] = None
    quantity_available: Optional[int] = None
    market_cap: Optional[int] = None
    price_volatility: Optional[float] = None
    auction_count: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)

class PriceHistoryResponse(BaseModel):
    item_id: int
    snapshot_hour: datetime
    item_name: Optional[str] = None
    open_price: Optional[float] = None
    high_price: Optional[int] = None
    low_price: Optional[int] = None
    close_price: Optional[float] = None
    avarage_price: Optional[float] = None
    volume: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)

class MarketOpportunityResponse(BaseModel):
    snapshot_date: date
    item_id: int
    item_name: Optional[str]
    icon_url: Optional[str]
    current_price: Optional[int]
    avg_price_7d: Optional[float]
    z_score: Optional[float]
    recommendation: str
    model_config = ConfigDict(from_attributes=True)

class ItemDemandResponse(BaseModel):
    snapshot_date: date
    item_id: int
    item_name: Optional[str]
    estimated_daily_sales: Optional[int]
    turnover_percentage: Optional[float]
    model_config = ConfigDict(from_attributes=True)

class MarketConcentrationResponse(BaseModel):
    snapshot_date: date
    item_id: int
    item_name: Optional[str]
    floor_concentration_pct: Optional[float]
    market_status: str
    model_config = ConfigDict(from_attributes=True)

class MarketIndexResponse(BaseModel):
    snapshot_date: date
    items_in_index: int
    index_value: float
    inflation_pct_daily: Optional[float]
    model_config = ConfigDict(from_attributes=True)