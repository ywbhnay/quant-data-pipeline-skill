"""
Quant Data Pipeline — Claude Code Skill Tool

This script is the bridge between Claude and the quant data pipeline CLI.
It uses subprocess to call `python -m src.cli` from the project root.

PROJECT_ROOT is auto-detected: this script lives inside the skill directory,
so its parent IS the project root (contains src/).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional

# Auto-detect project root: this file is in quant-data-pipeline-skill/,
# parent dir contains src/, .env.example, etc.
PROJECT_ROOT = Path(__file__).resolve().parent


def _run_cli(args: list[str], timeout: int = 1800) -> str:
    """Run the quant data pipeline CLI and return stdout+stderr."""
    command = [sys.executable, "-m", "src.cli", *args]
    try:
        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
        output = result.stdout

        # --- 日志截断：防止 Agent 上下文爆炸 ---
        # Agent 只需要看开头（确认执行了什么）和结尾（成功与否 + 统计）
        MAX_OUTPUT_LEN = 3000
        if len(output) > MAX_OUTPUT_LEN:
            head_len = 500
            tail_len = MAX_OUTPUT_LEN - head_len - 50  # 50 留给截断标记
            output = (
                output[:head_len]
                + "\n\n... [中间日志已截断，保留首尾] ...\n\n"
                + output[-tail_len:]
            )

        if result.returncode == 0:
            return f"✅ 命令执行成功:\n\n{output}"
        else:
            return f"❌ 命令执行失败 (Return Code: {result.returncode}):\n\n{output}"
    except subprocess.TimeoutExpired:
        return (
            f"⚠️ 命令执行超时 (超过 {timeout} 秒)，"
            "同步可能仍在后台进行，请稍后使用 'status' 动作检查。"
        )
    except FileNotFoundError:
        return (
            f"🚨 找不到 Python 解释器或 CLI 模块。\n"
            f"项目根目录: {PROJECT_ROOT}\n"
            f"请确认: pip install -r {PROJECT_ROOT}/requirements.txt"
        )
    except Exception as e:
        return f"🚨 发生未知系统异常: {e}"


def manage_quant_data(
    action: str,
    domain: Optional[str] = None,
    skip_domains: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> str:
    """
    Claude Code Skill: 量化数据库管理工具

    参数说明:
    - action (str): 要执行的动作，必须是以下之一:
        "status"           — 查看数据库各表最新数据日期 (回测前必须先调用!)
        "daily_update"     — 执行每日增量更新，同步到最新交易日
        "sync_domain"      — 单独同步某个业务域 (需指定 domain 参数)
        "full_backfill"    — 从零全量回填所有业务域
        "backfill_gaps"    — 自动检测并回补 daily 表中的交易日断层
        "validate"         — 运行数据质量校验
    - domain (str, optional): 当 action="sync_domain" 时必填。
        可选值: basic, daily, finance, finance-aux, trading, macro
    - skip_domains (str, optional): 当 action="full_backfill" 时可选。
        逗号分隔的域名列表，跳过这些域不回填。
    - dry_run (bool, optional): 当 action="daily_update" 时可用。预览执行计划不拉数据。
    - verbose (bool, optional): 任何 action 可用。输出更详细的日志。
    """
    args: list[str] = []

    if action == "status":
        args.append("status")
    elif action == "daily_update":
        args.append("daily-incremental")
        if dry_run:
            args.append("--dry-run")
        if verbose:
            args.append("-v")
    elif action == "sync_domain":
        if not domain:
            return (
                "错误: action 为 sync_domain 时，必须提供 domain 参数。\n"
                "可选值: basic, daily, finance, finance-aux, trading, macro"
            )
        args.extend(["sync", domain])
    elif action == "full_backfill":
        args.append("full-backfill")
        if skip_domains:
            args.extend(["--skip", skip_domains])
        if verbose:
            args.append("-v")
    elif action == "backfill_gaps":
        args.append("backfill-gaps")
    elif action == "validate":
        args.append("validate")
        if domain:
            args.append(domain)
    else:
        return (
            f"错误: 未知的 action '{action}'\n"
            "可用 action: status, daily_update, sync_domain, full_backfill, backfill_gaps, validate"
        )

    return _run_cli(args)


# Allow direct execution for testing
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python quant_data_tool.py <action> [options]")
        print("动作: status, daily_update, sync_domain, full_backfill, validate")
        sys.exit(1)

    action_name = sys.argv[1]
    domain_name = sys.argv[2] if len(sys.argv) > 2 else None

    result = manage_quant_data(action_name, domain_name)
    print(result)
