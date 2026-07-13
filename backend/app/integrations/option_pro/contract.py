from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel

from app.models.catalysts import (
    AnalysisJobCreateRequest,
    AnalysisJobResponse,
    CalendarResponse,
    CatalystBatchRequest,
    CatalystBatchResponse,
    CatalystItem,
    CatalystTickerResponse,
    ErrorBody,
    FeedResponse,
    IntegrationHealthResponse,
    HotspotListResponse,
    HotspotPreparationItem,
    HotspotStatusResponse,
    LatestResponse,
    MarketFocusCycleCreateRequest,
    MarketFocusCyclePublic,
    MarketFocusCycleResponse,
    NewsImpactAnalysis,
    NewsResponse,
    PublicAnalysis,
    SCHEMA_VERSION,
)
from app.models.market_focus import MarketFocusCyclePublicAnalysis


CONTRACT_FILENAME = "macrolens-option-pro-v2.json"


def resolve_contract_path(
    module_file: Path = Path(__file__),
    configured: Optional[str] = None,
) -> Path:
    """Resolve both the source-tree and `/app` runtime image layouts."""

    if configured:
        configured_path = Path(configured).expanduser()
        if not configured_path.is_absolute():
            raise RuntimeError("MACROLENS_OPTION_PRO_CONTRACT_PATH must be absolute")
        return configured_path

    resolved = module_file.resolve()
    source_candidate = resolved.parents[4] / "contracts" / CONTRACT_FILENAME
    image_candidate = resolved.parents[3] / "contracts" / CONTRACT_FILENAME
    for candidate in (source_candidate, image_candidate):
        if candidate.is_file():
            return candidate
    if resolved.parents[3].name == "backend":
        return source_candidate
    return image_candidate


CONTRACT_PATH = resolve_contract_path(
    configured=os.getenv("MACROLENS_OPTION_PRO_CONTRACT_PATH")
)
MODELS: tuple[Type[BaseModel], ...] = (
    NewsImpactAnalysis,
    PublicAnalysis,
    CatalystItem,
    FeedResponse,
    LatestResponse,
    NewsResponse,
    CatalystTickerResponse,
    CatalystBatchRequest,
    CatalystBatchResponse,
    CalendarResponse,
    IntegrationHealthResponse,
    AnalysisJobCreateRequest,
    AnalysisJobResponse,
    ErrorBody,
    MarketFocusCycleCreateRequest,
    MarketFocusCyclePublicAnalysis,
    HotspotStatusResponse,
    HotspotPreparationItem,
    HotspotListResponse,
    MarketFocusCyclePublic,
    MarketFocusCycleResponse,
)


def contract_document() -> dict:
    return {
        "contract": "MacroLens Option Pro Integration API",
        "schema_version": SCHEMA_VERSION,
        "models": {
            model.__name__: model.model_json_schema(mode="validation")
            for model in MODELS
        },
    }


def generated_bytes() -> bytes:
    return (json.dumps(contract_document(), ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def schema_sha256() -> str:
    data = CONTRACT_PATH.read_bytes() if CONTRACT_PATH.is_file() else generated_bytes()
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or validate the Option Pro integration contract")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = generated_bytes()
    if args.write:
        CONTRACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONTRACT_PATH.write_bytes(expected)
        print(f"wrote {CONTRACT_PATH} sha256={hashlib.sha256(expected).hexdigest()}")
        return 0
    if not CONTRACT_PATH.is_file() or CONTRACT_PATH.read_bytes() != expected:
        print(f"contract is stale: {CONTRACT_PATH}")
        return 1
    print(f"contract ok sha256={hashlib.sha256(expected).hexdigest()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
