"""结构化 JSON 日志配置

支持两种输出格式（通过 LOG_FORMAT 环境变量切换）：
  - json: Loki 标准 JSON 格式，每行一个 JSON 对象（生产环境）
  - text: 人类可读格式（本地开发）
"""
from __future__ import annotations

import logging
import sys
import traceback
from datetime import datetime, timezone, timedelta

from pythonjsonlogger.json import JsonFormatter


class LokiJsonFormatter(JsonFormatter):
    """输出 Loki 兼容的 JSON 日志行。

    固定字段：timestamp, level, logger, message
    动态字段：通过 extra 传入的任意 key-value 自动展开
    """

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        super().add_fields(log_record, record, message_dict)

        log_record["timestamp"] = datetime.fromtimestamp(
            record.created, tz=timezone(timedelta(hours=8))
        ).isoformat()
        log_record["level"] = record.levelname.lower()
        log_record["logger"] = record.name

        if record.exc_info and record.exc_info[0] is not None:
            log_record["traceback"] = "".join(
                traceback.format_exception(*record.exc_info)
            )

        for key in ("asctime", "color_message", "taskName"):
            log_record.pop(key, None)


class DevTextFormatter(logging.Formatter):
    """本地开发用的彩色文本格式。"""

    COLORS = {
        "DEBUG": "\033[36m",     # cyan
        "INFO": "\033[32m",      # green
        "WARNING": "\033[33m",   # yellow
        "ERROR": "\033[31m",     # red
        "CRITICAL": "\033[35m",  # magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]
        level = f"{color}{record.levelname:<7}{self.RESET}"
        base = f"{ts} {level} [{record.name}] {record.getMessage()}"

        if record.exc_info and record.exc_info[0] is not None:
            base += "\n" + "".join(traceback.format_exception(*record.exc_info))

        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord(
                "", 0, "", 0, "", (), None
            ).__dict__ and k not in ("message", "msg", "args")
        }
        if extras:
            pairs = " ".join(f"{k}={v}" for k, v in extras.items())
            base += f"  | {pairs}"

        return base


def setup_logging(*, log_level: str = "INFO", log_format: str = "json") -> None:
    """统一初始化日志系统。应在 app 启动最早期调用一次。"""
    level = getattr(logging, log_level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # 清除已有 handler，避免重复
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    if log_format == "json":
        handler.setFormatter(LokiJsonFormatter())
    else:
        handler.setFormatter(DevTextFormatter())

    root.addHandler(handler)

    # 压制第三方库噪声
    for noisy in ("httpx", "httpcore", "uvicorn.access", "uvicorn.error",
                   "asyncpg", "hpack", "httpcore.http11"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
