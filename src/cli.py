"""CLI 入口：full-backfill / daily-incremental / status / sync / validate"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import create_engine, text

from .alert import AlertLevel, WebhookNotifier
from .config import DatabaseConfig, SyncConfig, TushareConfig
from .database import get_session
from .rate_limiter import RateLimiter

logger = logging.getLogger("cli")

# --------------------------------------------------------------------------- #
#  Phase definitions (ordered by dependency)
# --------------------------------------------------------------------------- #

VALID_DOMAINS = [
    "basic",
    "daily",
    "finance",
    "finance-aux",
    "trading",
    "macro",
]


@dataclass
class Phase:
    name: str
    syncer_class: type
    validator_class: type | None = None


def _build_phases() -> list[Phase]:
    """懒加载 Phase 列表，避免模块级循环导入。"""
    from .sync.basic import BasicSyncer
    from .sync.daily import DailySyncer
    from .sync.finance import FinanceSyncer
    from .sync.finance_aux import FinanceAuxSyncer
    from .sync.macro import MacroSyncer
    from .sync.trading import TradingSyncer

    from .validator.daily_validator import DailyValidator
    from .validator.finance_validator import FinanceValidator

    return [
        Phase("basic", BasicSyncer),
        Phase("daily", DailySyncer, DailyValidator),
        Phase("finance", FinanceSyncer, FinanceValidator),
        Phase("finance-aux", FinanceAuxSyncer),
        Phase("trading", TradingSyncer),
        Phase("macro", MacroSyncer),
    ]


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #


def _init_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )


def _init_infra():
    """初始化 engine, tushare client, rate_limiter, notifier。"""
    db_cfg = DatabaseConfig()
    ts_cfg = TushareConfig()
    sync_cfg = SyncConfig()

    engine = create_engine(
        db_cfg.url, pool_pre_ping=True, pool_size=5, max_overflow=10
    )

    import tushare as ts

    ts.set_token(ts_cfg.token)
    pro = ts.pro_api()

    rate_limiter = RateLimiter(ts_cfg.rate_limit_per_min)

    from .alert import AlertConfig

    notifier = WebhookNotifier(AlertConfig())

    return engine, pro, rate_limiter, notifier, sync_cfg, ts_cfg.token


def _get_phase_by_name(name: str) -> Phase | None:
    for phase in _build_phases():
        if phase.name == name:
            return phase
    return None


# --------------------------------------------------------------------------- #
#  Commands
# --------------------------------------------------------------------------- #


def cmd_status(args: argparse.Namespace) -> None:
    """查看各表最新数据日期。"""
    engine, _, _, _, _, _ = _init_infra()

    tables_and_dates = [
        ("stock_basic", "list_date"),
        ("trade_cal", "cal_date"),
        ("daily", "trade_date"),
        ("daily_basic", "trade_date"),
        ("adj_factor", "trade_date"),
        ("income", "end_date"),
        ("balancesheet", "end_date"),
        ("cashflow", "end_date"),
        ("macro_indicators", "month"),
        ("index_dailybasic", "trade_date"),
        ("top10_floatholders", "end_date"),
        ("margin", "trade_date"),
    ]

    with engine.connect() as conn:
        print(f"{'Table':<25} {'Latest Date':<15} {'Count':>12}")
        print("-" * 55)
        for table, date_col in tables_and_dates:
            try:
                row = conn.execute(
                    text(
                        f"SELECT MAX({date_col}), COUNT(*) FROM {table}"
                    )
                ).fetchone()
                latest = str(row[0]) if row and row[0] else "(empty)"
                count = row[1] if row and row[1] else 0
                print(f"{table:<25} {latest:<15} {count:>12,}")
            except Exception as e:
                print(f"{table:<25} {'ERROR':<15} {e}")


def cmd_full_backfill(args: argparse.Namespace) -> None:
    """从零全量回填：按 Phase 0→5 顺序串行执行。"""
    engine, pro, rate_limiter, notifier, sync_cfg, token = _init_infra()

    skip_domains = set(args.skip.split(",")) if args.skip else set()
    phases = _build_phases()
    synced: list[str] = []

    with get_session(engine) as session:
        for phase in phases:
            if phase.name in skip_domains:
                logger.info("[full-backfill] 跳过域: %s", phase.name)
                continue

            syncer = phase.syncer_class(engine, pro, rate_limiter, token)
            logger.info("[full-backfill] 开始全量回填: %s", phase.name)
            syncer.run_full(session)
            synced.append(phase.name)
            session.commit()

        # 后置校验
        if not args.skip_validation:
            run_validators(session, synced, notifier)

    # 完成通知
    notifier.send_sync(
        AlertLevel.INFO,
        "全量回填完成",
        f"已同步域: {', '.join(synced)}",
    )
    logger.info("[full-backfill] 全量回填完成: %s", ", ".join(synced))


def cmd_daily_incremental(args: argparse.Namespace) -> None:
    """每日增量更新。"""
    engine, pro, rate_limiter, notifier, sync_cfg, token = _init_infra()

    today = datetime.now().strftime("%Y%m%d")
    phases = _build_phases()

    if args.domain:
        phases = [
            p for p in phases if p.name == args.domain
        ]
        if not phases:
            logger.error("未知域: %s (可选: %s)", args.domain, ", ".join(VALID_DOMAINS))
            sys.exit(1)

    with get_session(engine) as session:
        # 假日处理
        try:
            is_open = session.execute(
                text(
                    "SELECT is_open FROM trade_cal "
                    "WHERE exchange='SSE' AND cal_date = :date"
                ),
                {"date": today},
            ).scalar()
            if is_open != 1:
                logger.info(
                    "[daily] 今日 (%s) 非交易日，trade_cal.is_open=%s", today, is_open
                )
                if args.skip_holiday:
                    notifier.send_sync(
                        AlertLevel.INFO,
                        "跳过非交易日",
                        f"今日 {today} 非交易日，跳过同步",
                    )
                    return
        except Exception:
            logger.warning("[daily] 无法查询 trade_cal，继续执行同步")

        if args.dry_run:
            for phase in phases:
                logger.info("[dry-run] 将增量同步: %s", phase.name)
            return

        synced: list[str] = []
        for phase in phases:
            syncer = phase.syncer_class(engine, pro, rate_limiter, token)
            logger.info("[daily] 开始增量同步: %s", phase.name)
            syncer.run_incremental(session)
            synced.append(phase.name)
            session.commit()

        # 后置校验
        if not args.skip_validation:
            run_validators(session, synced, notifier)

    # 完成通知
    notifier.send_sync(
        AlertLevel.INFO,
        "每日增量更新完成",
        f"已同步域: {', '.join(synced)}",
    )
    logger.info("[daily] 每日增量更新完成: %s", ", ".join(synced))


def cmd_sync(args: argparse.Namespace) -> None:
    """手动同步指定业务域。"""
    engine, pro, rate_limiter, notifier, _sync_cfg, _token = _init_infra()

    phase = _get_phase_by_name(args.domain)
    if not phase:
        logger.error("未知域: %s (可选: %s)", args.domain, ", ".join(VALID_DOMAINS))
        sys.exit(1)

    mode = "full" if args.full else "incremental"
    with get_session(engine) as session:
        syncer = phase.syncer_class(engine, pro, rate_limiter, _token)
        logger.info("[sync] %s 模式同步域: %s", mode, args.domain)
        if args.full:
            syncer.run_full(session)
        else:
            syncer.run_incremental(session)
        session.commit()

    logger.info("[sync] 域 %s 同步完成", args.domain)


def cmd_validate(args: argparse.Namespace) -> None:
    """手动运行数据校验。"""
    engine, _, _, notifier, _, _ = _init_infra()

    with engine.connect() as conn:
        # 需要 session 来运行 validators
        from sqlalchemy.orm import Session

        with Session(conn) as session:
            if args.domain:
                phases = [
                    p
                    for p in _build_phases()
                    if p.name == args.domain and p.validator_class
                ]
                if not phases:
                    logger.error("未知域或无校验器: %s", args.domain)
                    sys.exit(1)
            else:
                phases = [p for p in _build_phases() if p.validator_class]

            run_validators(session, [p.name for p in phases], notifier)


def run_validators(
    session, domain_names: list[str], notifier: WebhookNotifier
) -> None:
    """对指定域运行后置校验。"""
    phases = {p.name: p for p in _build_phases()}

    for domain in domain_names:
        phase = phases.get(domain)
        if not phase or not phase.validator_class:
            continue
        validator = phase.validator_class()
        report = validator.validate(session)
        logger.info("[validator:%s] %s", domain, report.summary)
        if not report.all_passed:
            notifier.send_sync(
                AlertLevel.WARNING,
                f"数据校验失败: {domain}",
                report.format_detail(),
            )


# --------------------------------------------------------------------------- #
#  Argument parsing
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quant-data-pipeline",
        description="量化数据管道 CLI — 全量回填 / 每日增量 / 状态查询 / 数据校验",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- full-backfill ---
    fb = sub.add_parser("full-backfill", help="从零全量回填所有业务域")
    fb.add_argument(
        "--skip",
        metavar="DOMAIN1,DOMAIN2",
        help="跳过指定域 (逗号分隔)",
    )
    fb.add_argument(
        "--skip-validation",
        action="store_true",
        help="跳过同步后数据校验",
    )
    fb.add_argument(
        "--skip-holiday",
        action="store_true",
        help="非交易日直接跳过 (默认仍尝试拉取)",
    )
    fb.add_argument("-v", "--verbose", action="store_true", help="详细日志")

    # --- daily-incremental ---
    di = sub.add_parser("daily-incremental", help="每日增量更新")
    di.add_argument(
        "--domain",
        choices=VALID_DOMAINS,
        help="仅同步指定域",
    )
    di.add_argument(
        "--dry-run",
        action="store_true",
        help="预览执行计划，不拉取数据",
    )
    di.add_argument(
        "--skip-validation",
        action="store_true",
        help="跳过同步后数据校验",
    )
    di.add_argument(
        "--skip-holiday",
        action="store_true",
        help="非交易日直接跳过",
    )
    di.add_argument("-v", "--verbose", action="store_true", help="详细日志")

    # --- status ---
    sub.add_parser("status", help="查看各表最新数据日期和记录数")

    # --- sync ---
    s = sub.add_parser("sync", help="手动同步指定业务域")
    s.add_argument("domain", choices=VALID_DOMAINS, help="业务域名称")
    s.add_argument(
        "--full",
        action="store_true",
        help="使用全量模式 (默认增量)",
    )

    # --- validate ---
    v = sub.add_parser("validate", help="手动运行数据校验")
    v.add_argument(
        "domain",
        nargs="?",
        choices=VALID_DOMAINS,
        help="校验指定域 (默认全部)",
    )

    return parser


# --------------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------------- #


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    _init_logging(getattr(args, "verbose", False))

    commands = {
        "full-backfill": cmd_full_backfill,
        "daily-incremental": cmd_daily_incremental,
        "status": cmd_status,
        "sync": cmd_sync,
        "validate": cmd_validate,
    }

    cmd = commands[args.command]
    cmd(args)


if __name__ == "__main__":
    main()
