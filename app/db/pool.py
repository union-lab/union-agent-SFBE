from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import asyncpg

from app.config import settings

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

CST = timezone(timedelta(hours=8))
logger = logging.getLogger("union")


def row_to_dict(row: asyncpg.Record) -> dict:
    """将 asyncpg Record 转为 dict，datetime 统一转为东八区 ISO 字符串。

    asyncpg 对「无时区」列返回 naive datetime 时，若按服务器本地解释会与 PG 实际存的 UTC
    不一致（尤其本机开发机在国内、DB 在云上）。此处统一按 UTC 锚定再换算 CST。
    """
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            d[k] = v.astimezone(CST).isoformat()
    return d


def _probe_tcp(host: str, port: int, timeout: float = 10) -> str:
    """探测 TCP 端口连通性，返回诊断信息。"""
    try:
        addrs = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        info_parts = [f"DNS: {[(a[0].name, a[4]) for a in addrs]}"]
    except Exception as e:
        return f"DNS 解析失败: {e}"

    for family, _, _, _, addr in addrs:
        try:
            t0 = time.monotonic()
            s = socket.socket(family, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect(addr)
            elapsed = (time.monotonic() - t0) * 1000
            s.close()
            info_parts.append(f"TCP {addr} OK ({elapsed:.0f}ms)")
            return " | ".join(info_parts)
        except Exception as e:
            info_parts.append(f"TCP {addr} FAIL: {e}")

    return " | ".join(info_parts)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is not None:
            return _pool
        await init_pool()
    return _pool  # type: ignore[return-value]


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    dsn = settings.database_url
    if not dsn:
        raise RuntimeError("DATABASE_URL 未配置")

    parsed = urlparse(dsn)
    host = parsed.hostname or "unknown"
    port = parsed.port or 5432
    logger.info("数据库连接目标: %s:%d", host, port)

    probe = await asyncio.get_event_loop().run_in_executor(None, _probe_tcp, host, port)
    logger.info("TCP 探测结果: %s", probe)

    # 本机开发库通常未开 SSL；强制 ssl 会导致 asyncpg 协商失败：
    # "PostgreSQL server ... rejected SSL upgrade"
    host_lower = (host or "").lower()
    use_local_plain = host_lower in ("localhost", "127.0.0.1", "::1")

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    ssl_arg: ssl.SSLContext | bool = False if use_local_plain else ssl_ctx
    if use_local_plain:
        logger.info("数据库主机为环回地址，使用非 SSL 连接（ssl=False）")

    is_pooler = "pooler.supabase.com" in dsn
    is_transaction_mode = ":6543" in dsn
    cache_size = 0 if (is_transaction_mode or is_pooler) else 256

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.monotonic()
            _pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=1,
                max_size=10,
                command_timeout=60,
                timeout=60,
                statement_cache_size=cache_size,
                server_settings={"search_path": "public,app", "timezone": "Asia/Shanghai"},
                ssl=ssl_arg,
            )
            elapsed = (time.monotonic() - t0) * 1000
            logger.info("连接池创建成功 (%.0fms, 第%d次)", elapsed, attempt)
            break
        except (OSError, asyncio.TimeoutError, asyncpg.InterfaceError, asyncpg.CannotConnectNowError, TimeoutError) as e:
            if attempt == max_retries:
                logger.error("数据库连接池创建失败（已重试 %d 次）: %s", max_retries, e)
                raise
            wait = min(2 ** attempt, 16)
            logger.warning("数据库连接第 %d 次失败，%ds 后重试: %s", attempt, wait, e)
            await asyncio.sleep(wait)

    return _pool  # type: ignore[return-value]


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
