from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class FocusTickerAssessment(StrictModel):
    ticker: str = Field(pattern=r"^[A-Z0-9][A-Z0-9.^/_-]{0,19}$")
    catalyst_bias: Optional[int] = Field(default=None, ge=-100, le=100)
    confidence: int = Field(ge=0, le=100)
    horizon: Literal["intraday", "days", "weeks", "uncertain"]
    supporting_event_ids: list[str] = Field(default_factory=list, max_length=8)
    conflicting_event_ids: list[str] = Field(default_factory=list, max_length=8)
    summary: str = Field(min_length=1, max_length=1000)
    risks: list[str] = Field(default_factory=list, max_length=8)
    insufficient_evidence: bool

    @model_validator(mode="after")
    def evidence_semantics(self):
        if self.insufficient_evidence and self.catalyst_bias is not None:
            raise ValueError("insufficient evidence requires a null catalyst bias")
        if not self.insufficient_evidence and self.catalyst_bias is None:
            raise ValueError("supported assessment requires a catalyst bias")
        if set(self.supporting_event_ids) & set(self.conflicting_event_ids):
            raise ValueError("supporting and conflicting evidence must be disjoint")
        return self


class DominantEvent(StrictModel):
    event_group_id: str = Field(min_length=1, max_length=100)
    summary: str = Field(min_length=1, max_length=1000)
    affected_sectors: list[str] = Field(default_factory=list, max_length=10)


class MarketFocusCycleAnalysis(StrictModel):
    cycle_id: str = Field(min_length=1, max_length=100)
    as_of: datetime
    market_summary: str = Field(min_length=1, max_length=3000)
    dominant_events: list[DominantEvent] = Field(default_factory=list, max_length=8)
    market_uncertainties: list[str] = Field(default_factory=list, max_length=20)
    affected_sectors: list[str] = Field(default_factory=list, max_length=20)
    focus_ticker_assessments: list[FocusTickerAssessment] = Field(default_factory=list, max_length=20)
    no_new_material_catalyst: bool
    insufficient_context: bool

    @field_validator("as_of")
    @classmethod
    def require_aware_as_of(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("as_of must include a timezone")
        return value

    @field_validator("focus_ticker_assessments")
    @classmethod
    def unique_tickers(cls, value: list[FocusTickerAssessment]) -> list[FocusTickerAssessment]:
        tickers = [item.ticker for item in value]
        if len(tickers) != len(set(tickers)):
            raise ValueError("focus ticker assessments must be unique")
        return value

    @model_validator(mode="after")
    def honest_empty_cycle(self):
        if self.no_new_material_catalyst and self.dominant_events:
            raise ValueError("an empty cycle cannot claim dominant events")
        return self


class PublicFocusTickerAssessment(FocusTickerAssessment):
    supporting_weight: float = Field(default=0.0, ge=0)
    conflicting_weight: float = Field(default=0.0, ge=0)
    conflict_ratio: float = Field(default=0.0, ge=0, le=1)
    effective_reliability: float = Field(default=0.0, ge=0, le=1)
    weighted_catalyst_context: Optional[float] = Field(default=None, ge=-100, le=100)

    @model_validator(mode="after")
    def weighted_context_semantics(self):
        if self.insufficient_evidence and self.weighted_catalyst_context is not None:
            raise ValueError("insufficient evidence requires null weighted catalyst context")
        return self


class MarketFocusCyclePublicAnalysis(StrictModel):
    cycle_id: str = Field(min_length=1, max_length=100)
    as_of: datetime
    market_summary: str = Field(min_length=1, max_length=3000)
    dominant_events: list[DominantEvent] = Field(default_factory=list, max_length=8)
    market_uncertainties: list[str] = Field(default_factory=list, max_length=20)
    affected_sectors: list[str] = Field(default_factory=list, max_length=20)
    focus_ticker_assessments: list[PublicFocusTickerAssessment] = Field(default_factory=list, max_length=20)
    no_new_material_catalyst: bool
    insufficient_context: bool
    display_only: Literal[True] = True
