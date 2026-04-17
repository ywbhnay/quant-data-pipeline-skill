# quant-data-pipeline

Tushare A股数据管道 — 全量回填 / 每日增量 / 数据校验 / 告警通知

## 快速开始

### 1. 安装依赖

```bash
pip install -e .
# 或: pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填入真实值
```

| 变量 | 说明 |
|------|------|
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | PostgreSQL 连接信息 |
| `TUSHARE_TOKEN` | Tushare Pro API Token |
| `TUSHARE_RATE_LIMIT` | 每分钟请求上限 (默认 200) |
| `ALERT_WECOM_WEBHOOK` | 企业微信机器人 Webhook (可选) |
| `ALERT_DINGTALK_WEBHOOK` | 钉钉机器人 Webhook (可选) |
| `ALERT_TELEGRAM_BOT_TOKEN` / `ALERT_TELEGRAM_CHAT_ID` | Telegram 告警 (可选) |

### 3. 初始化数据库

```bash
# 确保 PostgreSQL 已运行，且数据库已创建
createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
```

---

## 两种使用模式

### 模式一：全量回填 (新库从零开始)

```bash
python -m src.cli full-backfill
```

按业务域依赖顺序串行执行：

```
Phase 0: basic        → stock_basic, stock_company, namechange, trade_cal
Phase 1: daily        → daily, daily_basic, adj_factor, stk_limit
Phase 2: finance      → income, balancesheet, cashflow, fina_indicator
Phase 3: finance-aux  → disclosure_date, dividend, fina_mainbz, express
Phase 4: trading      → top10_floatholders, margin
Phase 5: macro        → macro_indicators, index_dailybasic
```

选项：

```bash
# 跳过指定域
python -m src.cli full-backfill --skip macro,trading

# 跳过数据校验
python -m src.cli full-backfill --skip-validation

# 详细日志
python -m src.cli full-backfill -v
```

### 模式二：每日增量更新 (已有数据库)

```bash
python -m src.cli daily-incremental
```

自动查询 `trade_cal` 判断是否交易日，仅拉取最新日期之后的数据。

选项：

```bash
# 仅同步指定域
python -m src.cli daily-incremental --domain daily

# 预览执行计划 (不拉数据)
python -m src.cli daily-incremental --dry-run

# 非交易日直接跳过
python -m src.cli daily-incremental --skip-holiday

# 跳过数据校验
python -m src.cli daily-incremental --skip-validation
```

---

## 其他命令

### 查看状态

```bash
python -m src.cli status
```

输出各表最新数据日期和记录数：

```
Table                     Latest Date          Count
-------------------------------------------------------
stock_basic               20260414             5,828
trade_cal                 20260415            10,234
daily                     20260414        10,576,852
...
```

### 手动同步

```bash
# 增量模式 (默认)
python -m src.cli sync daily

# 全量模式
python -m src.cli sync finance --full
```

### 数据校验

```bash
# 全部校验
python -m src.cli validate

# 仅校验指定域
python -m src.cli validate daily
```

校验项：
- **daily**: 记录数 ≥5000、最新日期 ≤3天、daily_basic/adj_factor/stk_limit 覆盖率 ≥95%
- **finance**: 四表非空、三表记录数比例 0.5-1.5x、fina_indicator 覆盖率 ≥80%

---

## 数据字典

完整表关系和字段说明见 [reports/data_dictionary.md](reports/data_dictionary.md)。

## 项目结构

```
quant_data_pipeline/
├── src/
│   ├── config.py            # 统一配置 (.env 读取)
│   ├── database.py          # engine / session factory
│   ├── exceptions.py        # 自定义异常
│   ├── rate_limiter.py      # 滑动窗口限速器
│   ├── retry.py             # tenacity 重试
│   ├── alert.py             # 多通道告警通知
│   ├── cli.py               # CLI 入口
│   ├── models/              # SQLAlchemy ORM 模型
│   ├── sync/                # 6 个业务域同步器
│   └── validator/           # 数据校验器
├── pyproject.toml
├── .env.example
├── reports/                 # 数据字典、关系图
└── README.md                # 本文件
```

---

## 定时任务 (可选)

通过 cron 实现每日自动更新：

```cron
# 每个交易日 16:30 运行增量同步 (收盘后半小时)
30 16 * * 1-5 cd /path/to/quant_data_pipeline && python -m src.cli daily-incremental --skip-holiday >> /var/log/quant-pipeline.log 2>&1
```
