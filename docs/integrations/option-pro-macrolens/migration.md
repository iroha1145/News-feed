# 数据迁移

## 原则

迁移采用新增表与字段、幂等分批回填、核对、切换读取、稳定后清理的顺序。不得在启动时做破坏性大表重建。

## MacroLens

本轮数据库迁移号为 `PRAGMA user_version=5`。程序拒绝打开高于自身支持版本的数据库，迁移完成并校验后才写入版本号。迁移失败会回滚事务并恢复外键检查。

新增 analysis_jobs、analysis_revisions、analysis_stock_impacts、calendar_analysis_jobs、calendar_snapshots、calendar_event_revisions、integration_changes、integration_nonces、analysis_worker_state 和持久来源健康。

本次增量新增 `focus_context_snapshots`、`news_ticker_mentions`、`news_event_groups`、`news_event_members`、`hotspot_preparation_sets`、`hotspot_preparation_state`、`market_focus_cycles`、`market_focus_cycle_events`、`event_projection_retries`、`projection_safety_counters` 与 `market_focus_cycle_archives`。`analysis_revisions` 继续只保存逐条新闻分析版本，禁止复用、重命名或迁移为热点/周期表。

v4 将股票关联身份与验证状态拆开：`news_ticker_mentions` 只保留自然键身份和当前状态缓存，`ticker_validation_revisions` 追加保存每次实际状态变化，`focus_validation_state` 保存有界重验证游标与统计。模型关联绑定产生它的 `analysis_revision_id`，新分析版本不会删除旧关联；`analysis_stock_impacts` 同时关联分析版本和 Mention。

v5 迁移只创建审计表，不会根据旧密钥形状自动恢复周期。旧部署可能连接兼容端点，仅凭 `provider=openai` 和 75 字符密钥无法证明官方服务拒绝了请求。

`MARKET_FOCUS_LEGACY_RECOVERY_AUTHORIZATIONS` 默认必须为 `[]`。只有运维人员逐周期核对原始日志后，才能加入授权记录。每项必须同时提供 `cycle_id`、`input_hash`、数据库中原样复制的 `created_at`、`prompt_cache_key` 的 SHA-256、官方端点、HTTP 400、`string_above_max_length`、参数名、授权时间和证据编号：

```json
[
  {
    "cycle_id": "mfc_<32位小写十六进制>",
    "input_hash": "<64位小写十六进制>",
    "created_at": "<数据库原始时间>",
    "prompt_cache_key_sha256": "<64位小写十六进制>",
    "provider_base_url": "https://api.openai.com/v1",
    "http_status": 400,
    "error_type": "string_above_max_length",
    "error_param": "prompt_cache_key",
    "authorized_at": "<含时区的 ISO-8601 时间>",
    "evidence_reference": "<事故或日志证据编号>"
  }
]
```

恢复程序不根据当前 `OPENAI_BASE_URL` 猜测历史端点。自定义端点授权、空清单或任一指纹不符时，周期继续保持 `submission_outcome_unknown`，租赁和预算均不释放。即使数据库已经是 v5，后续启动也会在清单非空时取得独占写锁并执行同一套核对。

通过核对的周期才会改记为 `provider_request_rejected`，准备集合恢复为 `PREPARED`，对应活动周期指针才会清空。授权原文及其校验和、原状态摘要、事故证据、释放版本和实际动作写入 `market_focus_cycle_recovery_audit`。单次最多接受 100 项授权；重复启动只识别同一授权的既有审计记录，不会再次处理。核对审计记录后应把环境清单恢复为 `[]`。

升级时按自然键合并旧 Mention：保留最早创建时间、最高置信度和最新检查状态。初始验证版本优先使用旧 `validated_at`；缺失时使用迁移时刻，绝不回填到新闻发布时间。此类记录标记为 `legacy_backfill`。迁移前没有保存下来的验证变化无法恢复，因此旧历史只能从保守基线开始；无法唯一对应分析版本的旧模型关联会保留为 `legacy association`，不会猜测归属。

`analysis_stock_impacts` 增加 validation_status、validated_at、focus_revision、universe_version 和 association_method。历史股票影响按最近焦点快照与可信来源标记回填；无法确认的记录设为 unverified，不伪造 canonical。来源健康拆成抓取、新闻保存和事件投影三段状态。

现有 analyses 继续作为旧界面最新投影；新分析追加 revision。旧 affected_stocks JSON 幂等回填，坏 JSON 跳过并记录。旧记录缺失 confidence、horizon 和 mechanism 时使用 0、uncertain、other，并标记 legacy schema，不假装来自 Terra/max。

旧 logic_chain 不作为隐藏推理继续公开；兼容读取映射为有界用户摘要，无法安全映射时留空。

旧 processing 没有 Response ID，迁移回 pending；system 低上下文记录映射为 insufficient_context；failed 保留失败；skipped 不自动重跑，以免产生费用。

数据库 settings 中旧的 default_llm_provider 和 default_llm_model 覆盖在幂等迁移中删除。Terra 队列只采用 Web 与 Worker 共同的环境配置，网页设置接口拒绝再次写入这两个覆盖值。

## Option Pro

独立 `/data/catalyst-cache.db` 本轮从 v4 升级到 v6；不得改写 `/data/optix.db` 或 `breakout-db-v3`。

缓存继续包含同步 Run、水位、Staging、原始新闻、追加分析、股票影响、日历版本、来源健康、本地任务、刷新 Outbox、Worker 状态和单实例锁。v5 新增按完成交易日与算法版本隔离的 `focus_daily_strength_snapshots`；v6 为该派生缓存补齐负载摘要、覆盖率、各算法版本、数据截至时间和租约隔离字段。v5 缓存只含可重算的派生特征，升级时会在同一事务内安全失效并按 v6 结构重建，不触碰新闻、分析或正式评分数据。失去租约的旧进程不能覆盖新缓存。焦点快照增加 30/90 天分层保留，清理采用短事务分批执行，并保护周期或任务仍在引用的版本。

Option Pro 自有 AI Job 使用独立 /data/ai-jobs.db。

## 备份

迁移前使用 SQLite Backup API，或停写、checkpoint 后完整保存 db、wal 和 shm。备份记录提交、Schema 版本、校验和和时间。

## 验证

覆盖真实旧结构升级、重复执行、故障回滚、坏 JSON、重复股票、历史 as_of、日历 Actual 后到、旧模型覆盖、事务一致性和 foreign_key_check。
