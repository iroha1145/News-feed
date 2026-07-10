# MacroLens — 宏观新闻分析平台

MacroLens 聚合金融新闻、市场行情和经济日历，再用大语言模型（LLM）生成情绪判断与影响链。界面会区分原始数据、缓存数据和模型推演，避免把估算结果包装成实时事实。

## 主要功能

- 多来源新闻流：展示来源、发布时间、分析状态和原文链接。
- 深度分析：结构化输出摘要、情绪、置信度、影响标的和逻辑链。
- 市场面板：行情缺失时显示暂无数据，不用随机数填充。
- 情绪面板：统一使用同一套恐惧—贪婪分数和中文标签。
- 经济日历：短时缓存分析结果；上游失败时保留最近一次有效数据并注明状态。
- 管理员会话：刷新新闻、触发分析和修改设置前必须登录。
- 自适应布局：支持桌面和手机浏览。

## 数据边界

- 新闻标题、摘要和时间来自对应新闻源；平台只保存必要元数据，并提供原文入口。
- “X 市场情绪”是模型根据新闻语境生成的市场情景，不是实时抓取的 X 帖文。
- 行情来自雅虎财经（Yahoo Finance）的非官方客户端，可能延迟或暂时缺失；界面不再推断交易所是否开盘。
- 缺少正文的新闻会进入低信息回退分析，不再补造正文或强行情判断。

数据源启停、轮询频率、重复覆盖与授权注意事项见 [数据源审查](docs/data-sources.md)。

## 安装

推荐使用安装脚本。密钥输入不会回显，旧配置会先备份，管理员令牌会自动生成。

```bash
git clone https://github.com/iroha1145/News-feed.git
cd News-feed
./setup.sh
```

也可以手动启动：

```bash
cp .env.example .env
# 编辑 .env，至少配置一个模型提供方和 ADMIN_TOKEN
docker compose up -d --build
```

浏览器打开 `http://localhost:3000`。管理员令牌保存在 `.env`，不会写入网页存储，也不应提交到代码仓库。

## 本地开发

后端（Backend）：

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt -r backend/requirements-dev.txt
PYTHONPATH=backend pytest -q backend/tests
cd backend && uvicorn app.main:app --reload --port 8000
```

前端（Frontend）：

```bash
cd frontend
npm ci
npm run typecheck
npm test
npm run dev
```

## 生产部署

容器以非特权用户运行，后端只绑定本机 `127.0.0.1:8000`，外部请求统一经过前端反向代理。数据库目录可通过 `MACROLENS_DATA_DIR` 指向持久化位置。

```bash
docker compose up -d --build
docker compose ps
curl --fail http://127.0.0.1:8000/health
```

公开部署建议在端口 3000 前增加传输层安全协议（TLS）反向代理，并把 `SESSION_COOKIE_SECURE` 设为 `true`。

## 技术构成

| 层级 | 组件 |
|---|---|
| 前端（Frontend） | React、TypeScript、Vite、Tailwind CSS |
| 后端（Backend） | FastAPI、SQLite、APScheduler |
| 模型 | OpenAI、Anthropic、Grok、Ollama |
| 部署 | Docker Compose、Nginx |

## 许可证

MIT
