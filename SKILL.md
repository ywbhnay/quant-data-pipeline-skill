---
name: quant-data-pipeline
description: 量化数据管道管理 — 数据库状态查询、全量回填、每日增量更新、分域同步、数据校验。支持 Tushare Pro API 到 PostgreSQL 的数据同步。
origin: ECC
---

# Quant Data Pipeline Skill

量化数据管道智能体工具，管理 PostgreSQL 数据库中的 A 股行情、财务、宏观等数据的同步。

## When to Activate

- 用户要求执行量化回测、数据分析、策略研究前
- 用户要求更新数据库、同步最新行情数据时
- 用户询问数据库当前数据状态时
- 涉及历史行情、财务报表、宏观指标等数据查询前

## 数据管家准则

> 在执行任何涉及历史数据查询或 Qlib/量化回测的任务前，你**必须**首先调用 `manage_quant_data`（`action="status"`）来探查 PostgreSQL 数据库的最新数据状态。
> 如果发现数据日期落后于当前真实日期，你**必须**主动调用该工具（`action="daily_update"`）完成数据增量同步。在收到同步成功的返回日志后，方可继续执行后续的量化计算任务。

## 可用动作

### 1. `status` — 查看数据库状态（回测前必调！）

```
manage_quant_data(action="status")
```

返回每张表的最新数据日期和记录数，附带**数据健康度摘要**（最新交易日、落后天数、覆盖范围）。用于判断数据新鲜度。

### 2. `daily_update` — 每日增量更新

```
manage_quant_data(action="daily_update")

# 预览模式（只查看计划，不实际拉数据）
manage_quant_data(action="daily_update", dry_run=True)

# 详细日志
manage_quant_data(action="daily_update", verbose=True)
```

自动按依赖顺序同步所有业务域，仅拉取 `MAX(date)` 之后的新数据。

### 3. `sync_domain` — 单独同步某个业务域

```
manage_quant_data(action="sync_domain", domain="daily")
```

**domain 可选值:**
| 域名 | 包含表 |
|------|--------|
| `basic` | stock_basic, stock_company, namechange, trade_cal |
| `daily` | daily, daily_basic, adj_factor, stk_limit, suspend_d |
| `finance` | income, balancesheet, cashflow, fina_indicator |
| `finance-aux` | disclosure_date, dividend, fina_mainbz, express |
| `trading` | top10_floatholders, margin, hk_hold |
| `macro` | macro_indicators, index_dailybasic, sw_industry |

### 4. `backfill_gaps` — 自动检测并回补交易日断层

```
manage_quant_data(action="backfill_gaps")
```

自动检测 `daily` 表中缺失的交易日，逐日调用 Tushare API 回补。适用于发现历史数据断层时使用。

### 5. `full_backfill` — 从零全量回填

```
manage_quant_data(action="full_backfill")

# 跳过某些域（如 basic 已有数据）
manage_quant_data(action="full_backfill", skip_domains="basic")
```

按 Phase 0→5 顺序串行执行全量回填，自动尊重表之间的外键依赖关系。

### 6. `validate` — 数据质量校验

```
manage_quant_data(action="validate")
manage_quant_data(action="validate", domain="daily")
```

运行 daily 表记录数/完整性校验、财务三表勾稽关系校验。

## 环境变量要求

运行前必须确认 `.env` 文件存在且配置正确：

```bash
# 在项目根目录检查
ls -la .env

# 必需变量:
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD   # PostgreSQL 连接信息
TUSHARE_TOKEN                                       # Tushare Pro API Token
```

如果 `.env` 不存在或报错，引导用户：
```bash
cp .env.example .env
# 编辑 .env 填入真实值
```

## 常见错误排查

| 错误 | 原因 | 解决方案 |
|------|------|----------|
| `TushareRateLimitError` | Tushare API 限速触发 | 代码会自动重试，等待即可 |
| `Connection refused` (DB) | PostgreSQL 未运行 | `sudo systemctl start postgresql` |
| `ModuleNotFoundError: src` | 依赖未安装 | `pip install -r requirements.txt` |
| `KeyError: 'DB_HOST'` | .env 文件不存在 | 复制 .env.example 并填写 |
| 命令执行超时 (>30min) | 全量回填数据量大 | 正常现象，用 status 检查进度 |

## 执行顺序建议

新用户从零开始：
```
status → (确认数据库为空) → full_backfill → status → (确认数据完整)
```

日常运维：
```
daily_update → validate → status
```

回测/分析前：
```
status → (如数据落后) daily_update → status → (确认最新) → 执行回测
```

发现历史数据断层：
```
status → (发现 missing dates) → backfill_gaps → status → (确认数据完整)
```
