"""告警通知模块：企业微信/钉钉/Telegram webhook"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True)
class AlertConfig:
    """告警配置，从环境变量读取"""

    wecom_webhook: str | None = os.environ.get("ALERT_WECOM_WEBHOOK")
    dingtalk_webhook: str | None = os.environ.get("ALERT_DINGTALK_WEBHOOK")
    telegram_bot_token: str | None = os.environ.get("ALERT_TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str | None = os.environ.get("ALERT_TELEGRAM_CHAT_ID")


class WebhookNotifier:
    """多通道告警通知器"""

    def __init__(self, config: AlertConfig | None = None):
        self.config = config or AlertConfig()

    async def send(self, level: AlertLevel, title: str, message: str) -> None:
        """向所有已配置的通道发送告警"""
        tasks = []
        if self.config.wecom_webhook:
            tasks.append(self._send_wecom(level, title, message))
        if self.config.dingtalk_webhook:
            tasks.append(self._send_dingtalk(level, title, message))
        if self.config.telegram_bot_token and self.config.telegram_chat_id:
            tasks.append(self._send_telegram(level, title, message))

        if not tasks:
            logger.info("[alert] 无可用告警通道，跳过通知: [%s] %s", level.value, title)
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning("[alert] 通道 %d 发送失败: %s", i, result)

    def send_sync(self, level: AlertLevel, title: str, message: str) -> None:
        """同步版本（CLI 场景使用）"""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.send(level, title, message))
        except RuntimeError:
            asyncio.run(self.send(level, title, message))

    async def _post_json(self, url: str, payload: dict[str, Any]) -> None:
        """通用 HTTP POST 封装 (使用 urllib 避免外部依赖)"""
        import urllib.request

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Webhook 返回 {resp.status}")

    async def _send_wecom(self, level: AlertLevel, title: str, message: str) -> None:
        """企业微信机器人 webhook"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"## [{level.value.upper()}] {title}\n{message}"
            },
        }
        await self._post_json(self.config.wecom_webhook, payload)
        logger.info("[alert] 企业微信告警已发送: [%s] %s", level.value, title)

    async def _send_dingtalk(self, level: AlertLevel, title: str, message: str) -> None:
        """钉钉机器人 webhook"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"[{level.value.upper()}] {title}",
                "text": f"### [{level.value.upper()}] {title}\n\n{message}",
            },
        }
        await self._post_json(self.config.dingtalk_webhook, payload)
        logger.info("[alert] 钉钉告警已发送: [%s] %s", level.value, title)

    async def _send_telegram(self, level: AlertLevel, title: str, message: str) -> None:
        """Telegram Bot API"""
        url = (
            f"https://api.telegram.org/bot{self.config.telegram_bot_token}"
            f"/sendMessage"
        )
        payload = {
            "chat_id": self.config.telegram_chat_id,
            "text": f"*[{level.value.upper()}] {title}*\n\n{message}",
            "parse_mode": "Markdown",
        }
        await self._post_json(url, payload)
        logger.info("[alert] Telegram 告警已发送: [%s] %s", level.value, title)
