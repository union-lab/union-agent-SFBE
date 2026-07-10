"""顺丰商品档案独立推送服务

场景：李静茹反馈「产品档案都没推送，顺丰入不了库存」。
原有逻辑是在单据推送时顺带推商品档案，失败也不会阻断单据，
导致顺丰收到单据但档案缺失 → 货到了上不了账。

本模块提供独立入口：
1. 按 SKU 列表主动推送商品档案到顺丰 ITEM_SERVICE（-05 货主）
2. 失败详情落库 ads_sf_item_push_record，便于排查和重推
3. 与单据流程解耦，新品可先铺档案再开单

使用方式：
- API：POST /api/sf/dashboard/item-push  { "skus": [...] }
- 函数：await push_items(skus, source="manual") → dict
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
import re
import ssl
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

# 顺丰 ITEM_SERVICE 不接受 SKU 含汉字/全角字符
# 例：金蝶里用 "HC0SW0054-禁" 表示停用，推到顺丰会被整批退回
_HAN_RE = re.compile(r"[\u4e00-\u9fff\uff00-\uffef]")


def _has_cjk(sku: str) -> bool:
    return bool(_HAN_RE.search(sku or ""))

from app.clients.kingdee import KingdeeClient
from app.config import settings
from app.db.pool import get_pool

logger = logging.getLogger("union.sf_item_push")

TZ_CN = timezone(timedelta(hours=8))

# 一次 ITEM_SERVICE 调用打包多少个 SKU（顺丰接口允许多 Item，过多会超包体）
BATCH_SIZE = 50
# 金蝶 BD_MATERIAL 一批查多少个 SKU（IN 子句）
# ⚠ 不能设太大：金蝶返回上限 2000 行，而 BD_MATERIAL 是多组织结构（同一个 FNumber 在
#    每个启用的使用组织里都有一条），友联目前 ~15 个组织。
#    为避免结果被截断，我们强制加 FUseOrgId.FNumber='100' 过滤主组织，
#    这样每个 SKU 最多 1 行，可以安全开大批量。
KD_BATCH_SIZE = 500
# 金蝶组织：顺丰对接只关心友联主组织（100），其他组织不用重复建档
KD_MAIN_ORG = "100"


DDL_SF_ITEM_PUSH_RECORD = """
CREATE TABLE IF NOT EXISTS ads_sf_item_push_record (
    id              BIGSERIAL PRIMARY KEY,
    sku             TEXT NOT NULL,
    name            TEXT,
    spec            TEXT,
    unit            TEXT,
    approval_no     TEXT,
    license_no      TEXT,
    manufacturer    TEXT,
    company_code    TEXT,
    status          TEXT NOT NULL,
    source          TEXT,
    sf_response     TEXT,
    error_message   TEXT,
    pushed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sku, company_code)
);
CREATE INDEX IF NOT EXISTS idx_sf_item_push_status ON ads_sf_item_push_record(status);
CREATE INDEX IF NOT EXISTS idx_sf_item_push_pushed_at ON ads_sf_item_push_record(pushed_at DESC);
"""


async def ensure_table() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(DDL_SF_ITEM_PUSH_RECORD)
    logger.info("ads_sf_item_push_record 表就绪")


# ── SF 基础 helper（与 sf_automation 保持一致）──

def _sf_sign(content: str) -> str:
    md5 = hashlib.md5((content + settings.sf_special_str).encode()).hexdigest()
    return base64.b64encode(md5.encode()).decode()


def _sf_xml(service: str, body: str) -> str:
    return (
        f'<Request service="{service}" lang="zh-CN">'
        f"<Head><AccessCode>{settings.sf_access_code}</AccessCode>"
        f"<Checkword>{settings.sf_checkword}</Checkword></Head>"
        f"<Body>{body}</Body></Request>"
    )


def _sf_ssl() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def _sf_send(service: str, content: str) -> tuple[bool, str]:
    sign = _sf_sign(content)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "sysSource": settings.sf_company_code,
        "serviceCode": service,
    }
    async with httpx.AsyncClient(timeout=30, verify=_sf_ssl()) as client:
        resp = await client.post(
            settings.sf_base_url,
            headers=headers,
            data={"logistics_interface": content, "data_digest": sign},
        )
    raw = resp.text
    try:
        root = ET.fromstring(raw)
        head = root.findtext("Head", "UNKNOWN")
        return head == "OK", raw
    except ET.ParseError:
        return False, raw


# ── 金蝶批量取商品资料 ──

def _shelf_life_days(period: Any, unit: Any) -> int | None:
    """把金蝶 FExpPeriod + FExpUnit 转成顺丰 ShelfLife 要求的天数。
    单位：D=日 / M=月(按30) / Y=年(按365)。空值返回 None。"""
    try:
        n = int(period or 0)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    u = str(unit or "").strip().upper()
    if u == "D":
        return n
    if u == "Y":
        return n * 365
    return n * 30  # 默认按月


async def _query_materials_batch(client: KingdeeClient, skus: list[str]) -> dict[str, dict]:
    """批量从金蝶 BD_MATERIAL 取商品资料。

    返回 {sku: {name, spec, unit, barcode, shelf_life_days,
              approval_no, license_no, manufacturer, doc_status}}
    """
    if not skus:
        return {}
    out: dict[str, dict] = {}
    for i in range(0, len(skus), KD_BATCH_SIZE):
        chunk = skus[i : i + KD_BATCH_SIZE]
        quoted = ",".join(f"'{s}'" for s in chunk)
        # ⚠ 必须加 FUseOrgId 过滤：BD_MATERIAL 是多组织数据结构，
        #    同一 FNumber 在每个启用组织里都有一条独立记录。
        #    不加过滤的话 500 个 SKU × 15 组织 = 7500 行，远超金蝶 2000 行返回上限，
        #    会导致后面的 SKU 被静默截断、误判为"金蝶中未找到"。
        rows = await client.query(
            "BD_MATERIAL",
            (
                "FNumber,FName,FSpecification,FBaseUnitId.FName,"
                "FDocumentStatus,"
                "F_BGP_ApprovalNO,F_BGP_ApprovalNo1,F_BGP_ProductEnt.FName,"
                "FBARCODE,FExpPeriod,FExpUnit,"
                "F_YLYL_Text1"  # 品牌（友联自定义字段，如"优益诺"）
            ),
            filter_string=(
                f"FNumber IN ({quoted}) "
                f"AND FUseOrgId.FNumber = '{KD_MAIN_ORG}'"
            ),
            limit=2000,
        )
        for row in rows or []:
            if not row or len(row) < 12:
                continue
            sku = str(row[0] or "").strip()
            if not sku:
                continue
            out[sku] = {
                "name": str(row[1] or ""),
                "spec": str(row[2] or ""),
                "unit": str(row[3] or "") or "个",
                "doc_status": str(row[4] or ""),
                "approval_no": str(row[5] or ""),
                "license_no": str(row[6] or ""),
                "manufacturer": str(row[7] or ""),
                "barcode": str(row[8] or ""),
                "shelf_life_days": _shelf_life_days(row[9], row[10]),
                "brand": str(row[11] or "").strip(),
            }
    return out


# ── 推送主逻辑 ──

def _build_item_xml(item: dict) -> str:
    # 静茹 2026-04-21 要求：商品名称 = 品牌 + 商品名称 + 规格型号，按顺序拼接
    # 空值跳过，用空格分隔（比如"优益诺 酒精棉片（小猴） 60mmX30mm 50片/盒"）
    brand = (item.get("brand") or "").strip()
    name = (item.get("name") or "").strip()
    spec = (item.get("spec") or "").strip()
    full_name = " ".join(p for p in (brand, name, spec) if p)
    parts = [
        "<Item>",
        f"<SkuNo>{item['sku']}</SkuNo>",
        f"<ItemName>{_x(full_name)}</ItemName>",
        f"<ItemSpecifications>{_x(spec)}</ItemSpecifications>",
        f"<ItemUom>{_x(item.get('unit') or '个')}</ItemUom>",
    ]
    # 过滤 '/' 空格等占位符，只推真实 69 码
    # 顺丰 WMS 数据库字段叫 BarCode1（不是 ItemBarcode，静茹 2026-04-21 确认）
    barcode = (item.get("barcode") or "").strip()
    if barcode and barcode not in ("/", "-", "无", "N/A"):
        parts.append(f"<BarCode1>{_x(barcode)}</BarCode1>")
    if item.get("shelf_life_days"):
        parts.append(f"<ShelfLife>{int(item['shelf_life_days'])}</ShelfLife>")
    parts.append("</Item>")
    return "".join(parts)


def _x(val: Any) -> str:
    """XML 转义（顺丰只关心 < > & 三个字符）"""
    if val is None:
        return ""
    s = str(val)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


async def _push_batch(batch: list[dict]) -> tuple[bool, str, dict[str, tuple[bool, str]]]:
    """一批 SKU 打包成一个 ITEM_SERVICE 请求

    Returns:
        (batch_ok, raw_response, per_sku_result)
        per_sku_result: {sku: (success, note)}

    ⚠ 顺丰 ITEM_SERVICE 部分成功时 Head=PART（不是 OK），需要按每个 <Item>
      的 <Result> 逐行判断（1=成功, 2=失败）。之前直接看 Head==OK 会导致
      一批 50 个里只要有 1 个失败就把整批 49 个合法的也标记为失败。
    """
    items_xml = "".join(_build_item_xml(it) for it in batch)
    body = (
        f"<ItemRequest><CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<Items>{items_xml}</Items></ItemRequest>"
    )
    content = _sf_xml("ITEM_SERVICE", body)
    ok, raw = await _sf_send("ITEM_SERVICE", content)
    per_sku: dict[str, tuple[bool, str]] = {}
    try:
        root = ET.fromstring(raw)
        for item in root.iter("Item"):
            sku = (item.findtext("SkuNo") or "").strip()
            result = (item.findtext("Result") or "").strip()
            note = (item.findtext("Note") or "").strip()
            if sku:
                per_sku[sku] = (result == "1", note)
    except ET.ParseError:
        pass
    return ok, raw, per_sku


async def push_items(
    skus: list[str],
    source: str = "manual",
) -> dict:
    """批量推送商品档案到顺丰 -05 货主

    Args:
        skus: 金蝶商品编码列表
        source: 推送来源标记（manual / auto / retry / bill_<bill_no>）

    Returns:
        {
            "total": int, "success": int, "failed": int,
            "missing_in_kingdee": [sku, ...],   # 金蝶里查不到的 SKU
            "items": [{sku, status, error_message}, ...],
        }
    """
    skus = [s.strip() for s in skus if s and s.strip()]
    skus = list(dict.fromkeys(skus))  # 去重保序
    if not skus:
        return {"total": 0, "success": 0, "failed": 0, "missing_in_kingdee": [], "items": []}

    await ensure_table()

    # 先过滤含汉字/全角字符的 SKU（顺丰 ITEM_SERVICE 不接受）
    invalid_sku = [s for s in skus if _has_cjk(s)]
    if invalid_sku:
        logger.warning(
            "跳过 %d 个含汉字的非法 SKU（顺丰不受理）: %s",
            len(invalid_sku), invalid_sku[:10],
        )
    skus_valid = [s for s in skus if not _has_cjk(s)]

    # 1. 金蝶取商品资料
    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()
    materials = await _query_materials_batch(kingdee, skus_valid)

    missing = [s for s in skus_valid if s not in materials]
    if missing:
        logger.warning("金蝶未查到 %d 个 SKU: %s", len(missing), missing[:10])

    payload_list: list[dict] = []
    for sku in skus_valid:
        if sku not in materials:
            continue
        m = materials[sku]
        payload_list.append({"sku": sku, **m})

    # 2. 分批推送
    pool = await get_pool()
    results: list[dict] = []
    company = settings.sf_company_code

    for i in range(0, len(payload_list), BATCH_SIZE):
        batch = payload_list[i : i + BATCH_SIZE]
        try:
            ok, raw, per_sku = await _push_batch(batch)
        except Exception as e:
            logger.exception("ITEM_SERVICE 请求异常")
            ok, raw, per_sku = False, f"EXCEPTION: {e}", {}

        async with pool.acquire() as conn:
            for item in batch:
                sku_code = item["sku"]
                # 优先看逐行结果;如果顺丰没返回这个 SKU 则 fallback 到整批 ok
                if sku_code in per_sku:
                    item_ok, item_note = per_sku[sku_code]
                else:
                    item_ok, item_note = ok, ("" if ok else raw[:500])
                item_status = "success" if item_ok else "failed"
                item_err = "" if item_ok else (item_note or raw[:500])

                await conn.execute(
                    """
                    INSERT INTO ads_sf_item_push_record
                        (sku, name, spec, unit, approval_no, license_no, manufacturer,
                         company_code, status, source, sf_response, error_message, pushed_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
                    ON CONFLICT (sku, company_code) DO UPDATE SET
                        name = EXCLUDED.name,
                        spec = EXCLUDED.spec,
                        unit = EXCLUDED.unit,
                        approval_no = EXCLUDED.approval_no,
                        license_no = EXCLUDED.license_no,
                        manufacturer = EXCLUDED.manufacturer,
                        status = EXCLUDED.status,
                        source = EXCLUDED.source,
                        sf_response = EXCLUDED.sf_response,
                        error_message = EXCLUDED.error_message,
                        pushed_at = now()
                    """,
                    sku_code,
                    item.get("name"),
                    item.get("spec"),
                    item.get("unit"),
                    item.get("approval_no"),
                    item.get("license_no"),
                    item.get("manufacturer"),
                    company,
                    item_status,
                    source,
                    raw[:5000] if item_ok else None,
                    item_err or None,
                )
                results.append({
                    "sku": sku_code,
                    "status": item_status,
                    "error_message": item_err or None,
                })
        # 避免触发顺丰限流
        await asyncio.sleep(0.3)

    # missing 也落一条记录，方便前端看到
    if missing:
        async with pool.acquire() as conn:
            for sku in missing:
                await conn.execute(
                    """
                    INSERT INTO ads_sf_item_push_record
                        (sku, company_code, status, source, error_message, pushed_at)
                    VALUES ($1, $2, 'missing_in_kingdee', $3, '金蝶 BD_MATERIAL 未查到该编码', now())
                    ON CONFLICT (sku, company_code) DO UPDATE SET
                        status = 'missing_in_kingdee',
                        source = EXCLUDED.source,
                        error_message = EXCLUDED.error_message,
                        pushed_at = now()
                    """,
                    sku, company, source,
                )

    # 跳过的含汉字 SKU 也落一条记录，方便前端看到
    if invalid_sku:
        async with pool.acquire() as conn:
            for sku in invalid_sku:
                await conn.execute(
                    """
                    INSERT INTO ads_sf_item_push_record
                        (sku, company_code, status, source, error_message, pushed_at)
                    VALUES ($1, $2, 'invalid_code', $3, 'SKU 含汉字/全角字符,顺丰不受理', now())
                    ON CONFLICT (sku, company_code) DO UPDATE SET
                        status = 'invalid_code',
                        source = EXCLUDED.source,
                        error_message = EXCLUDED.error_message,
                        pushed_at = now()
                    """,
                    sku, company, source,
                )

    success = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")

    logger.info(
        "顺丰档案推送完成 source=%s 总计=%d 成功=%d 失败=%d 金蝶缺失=%d 含汉字跳过=%d",
        source, len(skus), success, failed, len(missing), len(invalid_sku),
    )

    return {
        "total": len(skus),
        "success": success,
        "failed": failed,
        "missing_in_kingdee": missing,
        "invalid_code": invalid_sku,
        "items": results,
        "company_code": company,
        "pushed_at": datetime.now(TZ_CN).isoformat(),
    }


async def list_records(
    status: str | None = None,
    keyword: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    """查询档案推送记录"""
    await ensure_table()
    pool = await get_pool()
    where = ["1=1"]
    params: list[Any] = []
    if status:
        params.append(status)
        where.append(f"status = ${len(params)}")
    if keyword:
        params.append(f"%{keyword}%")
        where.append(f"(sku ILIKE ${len(params)} OR name ILIKE ${len(params)})")
    params.append(limit)
    params.append(offset)
    sql = f"""
        SELECT id, sku, name, spec, unit, approval_no, license_no, manufacturer,
               company_code, status, source, error_message, pushed_at
        FROM ads_sf_item_push_record
        WHERE {' AND '.join(where)}
        ORDER BY pushed_at DESC
        LIMIT ${len(params) - 1} OFFSET ${len(params)}
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


async def get_stats() -> dict:
    """推送记录汇总"""
    await ensure_table()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'success')             AS success,
                COUNT(*) FILTER (WHERE status = 'failed')              AS failed,
                COUNT(*) FILTER (WHERE status = 'missing_in_kingdee')  AS missing,
                COUNT(*)                                               AS total,
                MAX(pushed_at)                                         AS last_pushed_at
            FROM ads_sf_item_push_record
            """
        )
    return dict(row) if row else {}


# ── 首营已审批 → 顺丰 档案自动同步 ─────────────────────────────
# 背景（阿潘 2026-03-31）：金蝶里"首营条件已审批"的商品（BGP_FirstCampBreedCheck
# FDocumentStatus='C'），顺丰侧应当都要有档案。否则到货时顺丰无法入库上账。
# 范围：只同步"首营已审批 + 物料也已审核（BD_MATERIAL FDocumentStatus='C'）"的 SKU，
#      避免把仍在草稿的物料推到顺丰。

FIRST_CAMP_QUERY_LIMIT = 2000  # 金蝶 ExecuteBillQuery 单页上限


async def _fetch_first_camp_approved_skus(client: KingdeeClient) -> list[str]:
    """分页抓 BGP_FirstCampBreedCheck 所有 FDocumentStatus='C' 的 SKU。"""
    skus: list[str] = []
    seen: set[str] = set()
    start = 0
    while True:
        rows = await client.query(
            "BGP_FirstCampBreedCheck",
            "F_BGP_ProductId.FNumber",
            filter_string="FDocumentStatus = 'C'",
            start_row=start,
            limit=FIRST_CAMP_QUERY_LIMIT,
        )
        if not rows:
            break
        for row in rows:
            if not row:
                continue
            sku = str(row[0] or "").strip()
            if sku and sku not in seen:
                seen.add(sku)
                skus.append(sku)
        if len(rows) < FIRST_CAMP_QUERY_LIMIT:
            break
        start += FIRST_CAMP_QUERY_LIMIT
    return skus


async def sync_first_camp_approved_to_sf(
    *,
    dry_run: bool = False,
    max_push: int | None = None,
    source: str = "auto_first_camp",
) -> dict:
    """一次性把金蝶「首营已审批 + 物料已审核」的 SKU 推到顺丰 -05 货主。

    只推差集：过滤掉 ads_sf_item_push_record 里 status='success' 的 SKU。

    Args:
        dry_run: True 时只返回要推的清单，不实际调用顺丰
        max_push: 限制本次最多推多少个 SKU（None 不限）
        source: 落库 source 字段，手动触发传 "manual_first_camp"，定时任务传 "cron_first_camp"

    Returns:
        {
            "first_camp_total": int,    # 金蝶首营已审批总数
            "already_pushed": int,       # 已在顺丰有成功记录的数
            "material_not_audited": int, # 首营已过但物料未审核（跳过）
            "kingdee_missing": int,      # 金蝶查不到物料（跳过）
            "to_push": int,              # 本轮实际要推的
            "push_result": {...},        # push_items 的返回
            "dry_run": bool,
        }
    """
    await ensure_table()
    pool = await get_pool()
    company = settings.sf_company_code

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    logger.info("[首营同步] 开始抓取 BGP_FirstCampBreedCheck 已审核清单…")
    first_camp_skus = await _fetch_first_camp_approved_skus(kingdee)
    logger.info("[首营同步] 金蝶首营已审批 SKU 数=%d", len(first_camp_skus))

    async with pool.acquire() as conn:
        pushed_rows = await conn.fetch(
            """
            SELECT sku FROM ads_sf_item_push_record
            WHERE company_code = $1 AND status = 'success'
            """,
            company,
        )
    pushed_set = {r["sku"] for r in pushed_rows}
    logger.info("[首营同步] 顺丰已成功推送的 SKU 数=%d", len(pushed_set))

    candidates = [s for s in first_camp_skus if s not in pushed_set]
    logger.info("[首营同步] 候选差集 SKU 数=%d", len(candidates))

    if not candidates:
        return {
            "first_camp_total": len(first_camp_skus),
            "already_pushed": len(pushed_set),
            "material_not_audited": 0,
            "kingdee_missing": 0,
            "to_push": 0,
            "push_result": None,
            "dry_run": dry_run,
        }

    materials = await _query_materials_batch(kingdee, candidates)
    kingdee_missing = [s for s in candidates if s not in materials]
    not_audited = [
        s for s in candidates
        if s in materials and materials[s].get("doc_status") != "C"
    ]
    to_push = [
        s for s in candidates
        if s in materials and materials[s].get("doc_status") == "C"
    ]
    if max_push is not None and max_push > 0:
        to_push = to_push[:max_push]

    logger.info(
        "[首营同步] 物料已审核=%d 未审核=%d 金蝶缺失=%d",
        len(to_push), len(not_audited), len(kingdee_missing),
    )

    result = {
        "first_camp_total": len(first_camp_skus),
        "already_pushed": len(pushed_set),
        "material_not_audited": len(not_audited),
        "kingdee_missing": len(kingdee_missing),
        "to_push": len(to_push),
        "push_result": None,
        "dry_run": dry_run,
        "sample_not_audited": not_audited[:10],
        "sample_missing": kingdee_missing[:10],
    }

    if dry_run or not to_push:
        return result

    result["push_result"] = await push_items(to_push, source=source)
    return result
