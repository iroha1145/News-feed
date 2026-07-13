# Focus context, hotspot preparation, and market-focus cycles

## Cost gates

`NEWS_LLM_AUTO_ANALYZE_ENABLED` defaults to `false`. The 60-second maintenance job creates no single-news analysis jobs and changes no ordinary news status unless automatic analysis is explicitly enabled. Automatic analysis additionally requires both `NEWS_LLM_DAILY_JOB_LIMIT` and `NEWS_LLM_DAILY_OUTPUT_TOKEN_LIMIT`; a missing value reports `budget_configuration_required` and is never interpreted as unlimited.

All other paid entry points are fail-closed as well. `CALENDAR_LLM_MANUAL_ENABLED=false` and `X_SENTIMENT_ENABLED=false` are the defaults. Calendar analysis additionally requires both of its daily budgets; the web buttons read capability state and remain disabled until the corresponding switch and budgets are configured.

Explicit single-news requests default off. They require `NEWS_LLM_MANUAL_ENABLED=true` plus non-empty `NEWS_LLM_MANUAL_DAILY_JOB_LIMIT` and `NEWS_LLM_MANUAL_DAILY_OUTPUT_TOKEN_LIMIT`; otherwise every public manual route rejects the request before opening the queue database, and no Job row is written. Manual focus cycles likewise require `HOT_CYCLE_MANUAL_ENABLED=true`, the global cycle switch, and both cycle budgets. News items use `NEWS_ITEM_MAX_OUTPUT_TOKENS=32768`; market-focus cycles use `HOT_CYCLE_MAX_OUTPUT_TOKENS=49152`; the provider ceiling is 128000. An incomplete response caused by the output limit is stored as `incomplete_output`, including usage, but has no published structured result and consumes no prepared revision.

## Pull-only focus context

MacroLens signs `GET /api/integrations/macrolens/v1/focus-context` with the dedicated focus read key and validates the response against `contracts/option-pro-macrolens-focus-v2.json`. The committed contract SHA-256 is `fbc646433375bc5657ec1dcaf0f980c14191390dabe8468129fdf71f78d5cade`. A failed pull marks the latest local snapshot stale; it does not delete it or stop news collection.

The focus payload contains only ticker identity, universe reasons, dollar-volume rank, minimal price/RVOL/breakout confirmation, sector, point-in-time timestamps, and data quality. It has no strength, ranking, market-fit, option, or factor-contribution score.

## Ticker validation and event evidence

`news_ticker_mentions` stores provider tags, company-endpoint associations, exact aliases, event propagation, and model inferences with an audited validation state. `ambiguous`, `invalid`, and `unverified` mentions do not qualify an individual-stock hotspot. A ticker outside the canonical focus universe can remain `valid_external`; it is not automatically invalid.

Raw source records reach `news_event_members` before representative-news deduplication. Similar reports within 24 hours are grouped only when title, important numbers, and ticker or topic identity agree. A stable evidence fingerprint covers independent publishers, validated tickers, event type, important numbers, and bounded fact terms. Independent publisher identities determine `source_count`, so two adapters carrying Reuters remain one source and the Seeking Alpha breaking/daily feeds remain one publisher. Ordinary syndication does not increment the prepared revision; a new independent publisher, trusted ticker, event type, important number, or fact does. Every material version is re-gated, while prior prepared snapshots stay unchanged.

Novelty compares the same facts, event type, and trusted tickers with the prior 72 hours. A normal first report starts below 100, repeated facts fall sharply, and a new material fact can recover part—but not all—of the novelty score.

## Deterministic hotspot gate

The gate never calls a model:

```
hot_score =
  0.25 severity +
  0.20 focus_relevance +
  0.15 novelty +
  0.15 source_diversity +
  0.15 source_quality +
  0.10 market_confirmation
```

A missing component has zero active weight and the remaining weights are normalized. No missing value is filled with 50. Market confirmation is accepted only when `symbol.data_through >= event.available_at`、`data_status=active`、来源状态为 active 或 degraded，且数据质量达到配置下限。`as_of` 只表示快照观察时间，不能代替数据截至时间；任一条件缺失时市场确认为 `null`。

Scores at or above 75 enter `PREPARED`. Scores from 60 through 74.999 require an independent second source, a trusted ticker association, a hard event type, or material market confirmation. These bootstrap thresholds are versioned by `HOTSPOT_GATE_VERSION`.

Low-severity analyst actions, ordinary price targets, commentary, recaps, opinions, and promotions use a stricter rule: at least two independent publishers and valid market confirmation of 70 or more. A trusted ticker alone cannot prepare them, and without valid confirmation their score is capped below the preparation threshold.

## Conflict discount

Ticker assessments keep supporting and conflicting evidence separate:

```
supporting_weight = sum(weight of supporting events)
conflicting_weight = sum(weight of conflicting events)
conflict_ratio = conflicting_weight / (supporting_weight + conflicting_weight)
effective_reliability = max(0, 1 - conflict_ratio)
support_factor = min(1, supporting_weight / SUPPORT_TARGET)
weighted_catalyst_context =
  catalyst_bias * confidence / 100 * support_factor * effective_reliability
```

该展示公式版本为 `catalyst-context-v2`，`SUPPORT_TARGET` 是集中配置的 bootstrap 校准值，并随周期不可变输入保存。结果限制在 `[-100,100]`，不进入正式股票评分。没有支持证据时结果为 `null`；支持权重增加不会降低绝对值，冲突权重增加不会提高绝对值。相同事实指纹的转载只计一次支持权重，即使它们来自不同事件组。旧公式值只作为过渡期 Shadow 研究字段保留，不覆写历史结果。

## Breakout confirmation context

突破生命周期到市场活动确认强度的映射统一使用 `breakout-confirmation-context-v1`：`DISCOVERED=0`、`WATCHING=0`、`TRIGGERED=10`、`CONFIRMED=25`、`HOLDING=20`、`RETESTING=8`、`RETEST_HELD=25`、`REACCELERATING=30`、`EXTENDED=15`、`FAILED=0`、`EXPIRED=0`。兼容旧快照时仅保留 `ACTIVE=25`。这些值只说明事件后的市场活动确认，不是买入评分、上涨概率或突破质量分，也不进入正式突破评分。

## Revision and immutable cycle semantics

`analysis_revisions` remains exclusively the single-news analysis history. It is not renamed or reused.

Each row in `hotspot_preparation_sets` is one monotonic prepared revision and contains an immutable event snapshot. `hotspot_preparation_state` retains the maximum prepared and continuously consumed revisions even if old terminal data is later cleaned.

`market_focus_cycles` is an independent durable queue with idempotency key, scheduled slot, lease, fencing token, attempt counters, retrieve/cancel state, immutable input, result, and complete usage fields. A cycle leases the oldest continuous prepared revisions, at most eight. New events arriving while it runs are not added. Only a valid completed result advances the largest continuous consumed prefix. Failure, cancellation, budget blocking, unknown submission outcome, and incomplete output return the lease to `PREPARED` and do not advance consumption.

Manual creation is enabled only when there is an unconsumed prepared event, no active cycle, the cooldown has elapsed, and the cycle budget is configured. The same state request is idempotent. A fixed cycle may honestly run with `no_new_hot_events=true`; it consumes no revision.

Signed integration routes:

- `GET /api/integrations/option-pro/v1/hotspots/status`
- `GET /api/integrations/option-pro/v1/hotspots`
- `POST /api/integrations/option-pro/v1/market-focus-cycles`
- `GET /api/integrations/option-pro/v1/market-focus-cycles/latest`
- `GET /api/integrations/option-pro/v1/market-focus-cycles/{cycle_id}`
- `POST /api/integrations/option-pro/v1/market-focus-cycles/{cycle_id}/cancel`

## Schedule and retention

The scheduler uses `America/New_York` and only the configured 08:00, 12:00, and 16:00 slots. On an NYSE early-close day, the `scheduled_1600` slot runs after the actual 13:00 close. Weekends and standard NYSE holidays do not run stock cycles. The optional 20:00 slot is disabled by default; there are no 00:00 or 04:00 model jobs.

Extended retention deletes in bounded batches and checks foreign keys before commit. Old completed and failed cycles first move to `market_focus_cycle_archives`, including the exact cycle row, final `result_json`, and every immutable event snapshot. Only then may unreferenced old `CONSUMED` preparations and event members be removed. Active projection retries are retained; completed retries use their own retention window. Cleanup reports row counts plus total, free, and live database bytes.

Cycles marked `submission_outcome_unknown` are deliberately excluded from archival deletion. Their leased preparation and reserved output-token budget remain intact because the upstream charge cannot be proved absent. Integration-health warnings expose the count as `market_focus_submission_outcome_unknown:<count>`; operators must resolve this manual-review queue rather than replaying or silently deleting it.

## Deferred web search

OpenAI Web Search is not enabled. A future version may use it only for unexplained price/RVOL anomalies, insufficient high-severity context, ambiguous entities, or an explicit user request. It remains bounded per event, cached, point-in-time audited, and display-only.
