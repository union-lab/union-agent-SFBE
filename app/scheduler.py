from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings

logger = logging.getLogger("union.sf_service.scheduler")

TZ_CN = timezone(timedelta(hours=8))
scheduler = AsyncIOScheduler(timezone=TZ_CN)
_sf_auto_push_lock = asyncio.Lock()


def _env_enabled(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() not in {"false", "0", "no", "off"}


async def _run_sf_auto_push() -> None:
    """扫描金蝶已审核单据，自动推送入库/出库单到顺丰 WMS + 校验回调。"""
    if _sf_auto_push_lock.locked():
        logger.warning("顺丰自动推送上一轮仍在运行，本轮跳过")
        return

    async with _sf_auto_push_lock:
        logger.info("定时任务: 顺丰自动推送（入库+出库）")
        try:
            from app.services.sf_automation import (
                ensure_table,
                scan_and_push,
                scan_and_push_outbound,
                validate_callbacks,
                validate_outbound_callbacks,
            )

            await ensure_table()
            count_in = await scan_and_push()
            count_out = await scan_and_push_outbound()
            logger.info("顺丰自动推送完成: 入库 %d 张, 出库 %d 张", count_in, count_out)
            await validate_callbacks()
            await validate_outbound_callbacks()
        except Exception:
            logger.exception("顺丰自动推送失败")


async def _run_sf_wms_status_sync() -> None:
    """批量同步顺丰 WMS 状态到 ads_sf_push_record.sf_wms_status_text。"""
    logger.info("定时任务: 顺丰 WMS 状态批量同步")
    try:
        from app.api.routes.sf_dashboard import sync_wms_status_batch

        result = await sync_wms_status_batch(limit=40, only_empty=True)
        logger.info(
            "WMS 状态同步: 扫描%d 成功%d 失败%d 跳过%d",
            result["total"],
            result["success"],
            result["failed"],
            result["skipped"],
        )
    except Exception:
        logger.exception("WMS 状态同步失败")


async def _run_sf_kingdee_recheck() -> None:
    """复检异常单：金蝶里手动救回的单据，自动转 auto_resolved。"""
    logger.info("定时任务: 顺丰异常单金蝶复检")
    try:
        from app.api.routes.sf_dashboard import recheck_kingdee_status_for_abnormal

        result = await recheck_kingdee_status_for_abnormal(limit=80)
        logger.info(
            "金蝶复检完成: 扫描%d 自动转正常%d 仍异常%d 错误%d",
            result["total"],
            result["resolved"],
            result["still_abnormal"],
            result["errors"],
        )
    except Exception:
        logger.exception("顺丰异常单金蝶复检失败")


async def _run_sf_unaudit_watcher() -> None:
    """扫描金蝶反审核单，自动调顺丰撤回接口。"""
    logger.info("定时任务: 顺丰反审核监控（自动撤回）")
    try:
        from app.services.sf_automation import scan_unaudit_and_cancel

        await scan_unaudit_and_cancel()
    except Exception:
        logger.exception("顺丰反审核监控失败")


async def _run_sf_first_camp_sync() -> None:
    """增量把金蝶首营已审批且物料已审核的商品档案同步到顺丰。"""
    logger.info("定时任务: 顺丰首营档案同步")
    try:
        from app.services.sf_item_push import sync_first_camp_approved_to_sf

        result = await sync_first_camp_approved_to_sf(source="cron_first_camp")
        push_result = result.get("push_result") or {}
        logger.info(
            "首营同步完成: 金蝶首营%d 已推送%d 本轮推%d 成功%d 失败%d",
            result.get("first_camp_total", 0),
            result.get("already_pushed", 0),
            result.get("to_push", 0),
            push_result.get("success", 0) if push_result else 0,
            push_result.get("failed", 0) if push_result else 0,
        )
    except Exception:
        logger.exception("首营档案同步失败")


def get_scheduler_state() -> dict:
    return {
        "enabled": _env_enabled("SCHEDULER_ENABLED", "false"),
        "running": scheduler.running,
        "jobs": [job.id for job in scheduler.get_jobs()],
    }


def start_scheduler() -> None:
    """注册并启动独立顺丰服务的定时任务。"""
    if not _env_enabled("SCHEDULER_ENABLED", "false"):
        logger.warning("SCHEDULER_ENABLED=false，独立顺丰定时调度器已跳过")
        return

    if not settings.sf_module_enabled:
        logger.warning("SF_MODULE_ENABLED=false，独立顺丰定时任务已跳过")
        return

    if scheduler.running:
        logger.info("独立顺丰定时调度器已在运行，跳过重复启动")
        return

    scheduler.add_job(_run_sf_auto_push, CronTrigger(minute="*/5"), id="sf_auto_push", replace_existing=True)
    scheduler.add_job(
        _run_sf_wms_status_sync,
        CronTrigger(minute="2-59/5"),
        id="sf_wms_status_sync",
        replace_existing=True,
    )
    scheduler.add_job(_run_sf_first_camp_sync, CronTrigger(minute=15), id="sf_first_camp_sync", replace_existing=True)
    scheduler.add_job(
        _run_sf_kingdee_recheck,
        CronTrigger(minute="4-59/10"),
        id="sf_kingdee_recheck",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_sf_unaudit_watcher,
        CronTrigger(minute="8-59/10"),
        id="sf_unaudit_watcher",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("独立顺丰定时调度器已启动，%d 个任务", len(scheduler.get_jobs()))
    for job in scheduler.get_jobs():
        logger.info("  -> %s: %s", job.id, job.trigger)


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("独立顺丰定时调度器已关闭")
