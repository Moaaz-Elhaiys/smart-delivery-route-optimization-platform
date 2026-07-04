# ingestion/schemas.py
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

# ── Order Contract ──
class Order(BaseModel):
    order_id: str
    lat: float = Field(ge=29.5, le=30.5, description="Latitude within Cairo bbox")
    lon: float = Field(ge=31.0, le=31.8, description="Longitude within Cairo bbox")
    district: str
    priority: Literal["high", "medium", "low"]
    weight_kg: float = Field(gt=0, le=100)
    created_at: datetime
    delivery_window_start: str
    delivery_window_end: str

    @field_validator("delivery_window_start", "delivery_window_end")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%H:%M")
        except ValueError:
            raise ValueError(f"Invalid time format: {v}. Expected HH:MM")
        return v

# ── Driver Contract ──
class Driver(BaseModel):
    driver_id: str
    lat: float = Field(ge=29.5, le=30.5)
    lon: float = Field(ge=31.0, le=31.8)
    capacity_kg: float = Field(gt=0)
    status: Literal["available", "busy", "offline"]
    district: str

# ── Bulk validators ──
def validate_orders(orders: list[dict]) -> list[Order]:
    """Validate all orders. Raises ValidationError with details on failure."""
    return [Order(**o) for o in orders]

def validate_drivers(drivers: list[dict]) -> list[Driver]:
    return [Driver(**d) for d in drivers]

def validate_roads(osm_data: dict) -> dict:
    """Lightweight OSM validation — Pydantic is overkill for the full OSM schema."""
    if not isinstance(osm_data, dict):
        raise TypeError("OSM response must be dict")
    if "elements" not in osm_data:
        raise ValueError("Missing 'elements' from OSM response")
    if len(osm_data["elements"]) == 0:
        raise ValueError("No road elements received")
    for road in osm_data["elements"][:20]:  # sample check
        if "id" not in road:
            raise ValueError(f"Road element missing 'id': {road}")
    return osm_data