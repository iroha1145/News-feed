# MacroLens ETL

MacroLens 已收敛为个人使用的单进程数据服务。它定时抓取金融新闻与经济日历，完成清洗、去重、原始 Ticker 保存、来源健康记录和数据保留；不再包含网页前端、模型分析、市场焦点或远程动作接口。

## 配置

非敏感设置集中在 `config/personal.toml`，由 Python 标准库 `tomllib` 读取。日历始终启用，缓存最长保留 7 天，单次清理最多处理 500 条；这些个人版固定边界不再暴露成配置。

服务器根目录的 `secrets.env` 只允许保存以下六项：

- `INTERNAL_API_TOKEN`：Option Pro 与 MacroLens 后端共用的内部承载令牌（Bearer Token），必须设置；
- `FINNHUB_API_KEY`、`MASSIVE_API_KEY`、`NEWSAPI_API_KEY`、`GNEWS_API_KEY`：对应新闻源密钥；
- `DATA_DIR`：可选的数据目录，数据库名固定为 `macrolens.db`。

这些值只通过服务器本地命令行工具（CLI）修改，不能手工编辑 `secrets.env`。工具从标准输入读取内容；在交互终端中输入时不会回显，值也不会进入命令参数、状态输出或校验输出。`status` 只显示是否已经配置，`validate` 只检查格式和本地文件，不请求真实新闻服务。

```bash
./personal.sh secrets status
./personal.sh secrets set INTERNAL_API_TOKEN
./personal.sh secrets set FINNHUB_API_KEY
./personal.sh secrets remove NEWSAPI_API_KEY
./personal.sh secrets validate
```

首次写入会创建权限为 `0600` 的 `secrets.env`。后续变更会在同目录加锁、备份并原子替换文件。只有 `macrolens` 已经运行时，工具才会重建这一项来载入新值；处于停止状态的服务不会被启动。

机器绑定只使用 `.env` 中的 `HOST_BIND` 与 `PORT`，示例见 `.env.example`。容器默认监听本机地址；需要跨机器读取时，应通过 Tailscale、WireGuard 或 HTTPS 反向代理提供内部地址。`INTERNAL_API_TOKEN` 只保留在两个后端，不能进入网页、浏览器存储或网络响应。

## 启动

```bash
cp .env.example .env
./personal.sh secrets set INTERNAL_API_TOKEN
./personal.sh secrets set DATA_DIR
docker compose -f docker-compose.personal.yml up -d --build
curl --fail http://127.0.0.1:8000/health
```

`docker-compose.yml` 与个人版文件保持同样的单服务结构，旧的 `docker compose up -d` 命令仍可使用。容器只监听本机地址，数据库放在具名卷 `macrolens-data` 中。

## 内部只读接口

除公开的 `/health` 外，接口都需要请求头：

```text
Authorization: Bearer <INTERNAL_API_TOKEN>
```

可读取：

- `GET /internal/v1/health`
- `GET /internal/v1/news/changes`
- `GET /internal/v1/news/{id}`
- `GET /internal/v1/calendar`

新闻增量和日历列表支持 `after_sequence`、`updated_after`、`cursor`、`limit`、`as_of`。首轮可用旧的时间水位兼容读取；此后应保存完整响应里的 `next_after_sequence`，并在下一轮首屏传回。序号是跨轮读取的主检查点，可以接住“首轮读取时尚未提交、随后才变为可见”的数据。游标已经冻结首屏条件，翻页时只发送 `cursor` 与 `limit`。新闻增量默认每页 50 条、最多 500 条；日历默认每页 200 条、最多 500 条。新闻的 `limit` 是条数上限；若完整响应接近 5 MiB，服务会提前截页并返回续页游标。

`published_at` 是格式合规的源站发布时间；源站若给出无法解析的日期，该字段会留空。`fetched_at` 是抓取时间，`updated_at` 保留原始记录时间，`available_at` 是本地可见时间。兼容时间窗口以 `available_at` 为准，新的跨轮读取则以持续递增的序号为准，因此迟到新闻和提交边界上的新闻都不会漏掉。

近似重复新闻仍归到同一条规范记录，但每次首次见到的来源、原始标题、原始网址和源站 Ticker 会写入 `news_source_observations`。新闻增量与单条详情返回去重后的 `sources` 和 `source_count`，让读取方识别多来源印证；列表读取会一次批量查询来源，不会逐条追加数据库查询。

## 数据库升级边界

升级已有数据库时，只会给原始新闻补充 `source_tickers`、`updated_at`，并新建 ETL 变化日志、来源观察、来源健康和日历快照表。旧分析表不会再初始化、迁移、写入或清理。新闻保留任务若发现旧表仍以外键引用某条新闻，会保留该新闻，避免级联删除历史分析。

日历每个快照保存完整事件集合。相同陈旧缓存会复用已有快照；内容相同但抓取时间更晚的新鲜结果仍会形成新快照，因为新的 `source_fetched_at` 代表数据覆盖时间已经推进。

`as_of` 的历史深度受 `change_retention_days` 与 `calendar_snapshot_retention_days` 限制。清理仍会为每条新闻保留最新状态或删除墓碑，足以重建当前副本，但不承诺永久保存每次中间变化。

## 离线测试

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt -r backend/requirements-dev.txt
PYTHONPATH=backend INTERNAL_API_TOKEN=test-internal-token pytest -q backend/tests
```

测试会替换新闻源和日历网络客户端，不会访问真实新闻服务，也不会调用任何模型。
