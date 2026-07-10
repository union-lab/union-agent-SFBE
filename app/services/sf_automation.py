"""顺丰自动化推送服务

监控金蝶已审核单据，自动推送到顺丰 WMS（-05 货主）。

入库：直接调拨单/分布式调入单/采购入库单 → PURCHASE_ORDER_SERVICE
出库：GSP发货通知单（FHTZ） → SALE_ORDER_SERVICE

流程：
1. 查金蝶：仓库=CK043(友联sf) + 已审核 + 尚未推送
2. 同步商品到顺丰（ITEM_SERVICE）
3. 推入库/出库单到顺丰
4. 记录推送结果到 ads_sf_push_record
5. 回调校验（数量不一致/超时告警）
"""

from __future__ import annotations

import asyncio
import hashlib
import base64
import html
import json
import os
import ssl
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import httpx

from app.config import settings
from app.clients.kingdee import KingdeeClient
from app.db.pool import get_pool

logger = logging.getLogger("union.sf_auto")

TZ_CN = timezone(timedelta(hours=8))

SF_WAREHOUSE = settings.sf_warehouse  # 金蝶仓库编码：友联sf仓（从 Settings 读取，.env SF_WAREHOUSE=CK043）


def _scan_since() -> str:
    """扫描单据的起始日期（FDate >= since）。

    规则：取 max(今天-7天, SF_PUSH_START_DATE)。
    - 默认 7 天回溯兜底，对齐历史行为
    - 生产环境用环境变量 SF_PUSH_START_DATE=YYYY-MM-DD 做硬起点，
      防止重启/补数据后把历史已发货单全部当作新单推一遍。
    """
    default_since = (datetime.now(TZ_CN) - timedelta(days=7)).strftime("%Y-%m-%d")
    hard_start = (os.getenv("SF_PUSH_START_DATE") or "").strip()
    if hard_start and hard_start > default_since:
        return hard_start
    return default_since


def _scan_since_datetime() -> str:
    """扫描单据的起始时间点（FApproveDate >= since），到分钟级。

    规则：读 SF_PUSH_START_DATETIME（格式 'YYYY-MM-DD HH:MM:SS'），如果没配,
    则 fallback 到 _scan_since() 只用日期。
    - 例：SF_PUSH_START_DATETIME='2026-04-21 08:30:00'
    - 用 FApproveDate 而非 FDate 才能精确卡住「审核时间」这个业务生效点
    """
    hard_dt = (os.getenv("SF_PUSH_START_DATETIME") or "").strip()
    if hard_dt:
        return hard_dt
    return f"{_scan_since()} 00:00:00"


def _push_blacklist() -> set[str]:
    """从环境变量 SF_PUSH_BLACKLIST 读取单号黑名单（逗号分隔），
    监控扫描时遇到这些单号直接跳过，不下发顺丰。
    """
    raw = (os.getenv("SF_PUSH_BLACKLIST") or "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}

DDL_ADS_SF_PUSH_RECORD = """
CREATE TABLE IF NOT EXISTS ads_sf_push_record (
    id              BIGSERIAL PRIMARY KEY,
    bill_no         TEXT NOT NULL,
    bill_type       TEXT NOT NULL,
    erp_order       TEXT NOT NULL,
    sf_receipt_id   TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    items_json      JSONB,
    sf_response     TEXT,
    error_message   TEXT,
    fhtz_fid        BIGINT,
    outstock_no     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bill_no, bill_type)
);
CREATE INDEX IF NOT EXISTS idx_sf_push_status ON ads_sf_push_record(status);
"""

DDL_ALTER_PUSH_RECORD = """
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS fhtz_fid BIGINT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS outstock_no TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS carrier_code TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS carrier_name TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS waybill_no TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS sf_wms_status TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS sf_wms_status_text TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS cancel_reason TEXT;
ALTER TABLE ads_sf_push_record ADD COLUMN IF NOT EXISTS unaudit_detected_at TIMESTAMPTZ;
"""

# FormId → 中文名 + 查询字段 + 方向（direction: inbound/outbound）
# ⚠ 双向调拨规则（2026-04-21 业务确认）：
#   - 直接调拨 / 分布式调入单：目标仓=SF_WAREHOUSE → 推入库；源仓=SF_WAREHOUSE → 推出库
#   - 分布式调出单（FBDC）→ 不抓（只是 FBDR 的上游，不代表业务完结）
#   - 采购入库单：仓库=SF_WAREHOUSE → 推入库
#   - 时间条件统一用 FApproveDate（审核时间），精确到分钟
BILL_CONFIGS = {
    "STK_TransferDirect_inbound": {
        "form_id": "STK_TransferDirect",
        "name": "直接调拨单(入库)",
        "bill_type": "transfer",
        "direction": "inbound",
        "field_keys": (
            "FBillNo,FDocumentStatus,FApproveDate,"
            "FMaterialId.FNumber,FMaterialId.FName,"
            "FQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
            "FDestStockId.FNumber,FDestStockId.FName,"
            "FSrcStockId.FNumber,FSrcStockId.FName"
        ),
        "filter_tpl": (
            "FDestStockId.FNumber = '{warehouse}' "
            "AND FDocumentStatus = 'C' "
            "AND FApproveDate >= '{since}'"
        ),
        "sku_idx": 3,
    },
    "STK_TransferDirect_outbound": {
        "form_id": "STK_TransferDirect",
        "name": "直接调拨单(出库)",
        "bill_type": "transfer_out",
        "direction": "outbound",
        "field_keys": (
            "FBillNo,FDocumentStatus,FApproveDate,"
            "FMaterialId.FNumber,FMaterialId.FName,"
            "FQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
            "FDestStockId.FNumber,FDestStockId.FName,"
            "FSrcStockId.FNumber,FSrcStockId.FName"
        ),
        "filter_tpl": (
            "FSrcStockId.FNumber = '{warehouse}' "
            "AND FDocumentStatus = 'C' "
            "AND FApproveDate >= '{since}'"
        ),
        "sku_idx": 3,
    },
    "STK_TransferIn_inbound": {
        "form_id": "STK_TransferIn",
        "name": "分布式调入单(入库)",
        "bill_type": "transfer_in",
        "direction": "inbound",
        "field_keys": (
            "FBillNo,FDocumentStatus,FApproveDate,"
            "FMaterialId.FNumber,FMaterialId.FName,"
            "FQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
            "FDestStockId.FNumber,FDestStockId.FName,"
            "FSrcStockId.FNumber,FSrcStockId.FName"
        ),
        "filter_tpl": (
            "FDestStockId.FNumber = '{warehouse}' "
            "AND FDocumentStatus = 'C' "
            "AND FApproveDate >= '{since}'"
        ),
        "sku_idx": 3,
    },
    "STK_TransferIn_outbound": {
        "form_id": "STK_TransferIn",
        "name": "分布式调入单(出库)",
        "bill_type": "transfer_in_out",
        "direction": "outbound",
        "field_keys": (
            "FBillNo,FDocumentStatus,FApproveDate,"
            "FMaterialId.FNumber,FMaterialId.FName,"
            "FQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
            "FDestStockId.FNumber,FDestStockId.FName,"
            "FSrcStockId.FNumber,FSrcStockId.FName"
        ),
        "filter_tpl": (
            "FSrcStockId.FNumber = '{warehouse}' "
            "AND FDocumentStatus = 'C' "
            "AND FApproveDate >= '{since}'"
        ),
        "sku_idx": 3,
    },
    # 2026-04-22 更正：友联的"采购入库单"(CGRK) 真正 FormId 是 STK_InStock
    # PUR_ReceiveBill (CGSH) 是"采购收货单"，是中间单，不影响账面库存
    # STK_InStock (CGRK) 是GSP审核后真正入账面的入库单，顺丰推送必须用这张
    "STK_InStock_inbound": {
        "form_id": "STK_InStock",
        "name": "采购入库单",
        "bill_type": "purchase_in",
        "direction": "inbound",
        "field_keys": (
            "FBillNo,FDocumentStatus,FApproveDate,"
            "FMaterialId.FNumber,FMaterialId.FName,"
            "FRealQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
            "FStockId.FNumber,FStockId.FName"
        ),
        "filter_tpl": (
            "FStockId.FNumber = '{warehouse}' "
            "AND FDocumentStatus = 'C' "
            "AND FApproveDate >= '{since}'"
        ),
        "sku_idx": 3,
    },
}


# ── SF API helpers (reuse settings) ──

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


async def _sf_send(service: str, content: str) -> tuple[bool, str, str]:
    """发送到顺丰，返回 (success, receipt_id, raw_response)"""
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
        body = root.find("Body")
        receipt_id = ""
        if body is not None:
            for tag in ("ReceiptId", "ShipmentId"):
                el = body.find(f".//*{tag}")
                if el is not None and el.text:
                    receipt_id = el.text
                    break
        return head in ("OK",), receipt_id, raw
    except ET.ParseError:
        return False, "", raw


def _extract_receipt_or_shipment_id(raw: str) -> str:
    """从顺丰响应 XML 中提取 ReceiptId 或 ShipmentId"""
    try:
        root = ET.fromstring(raw)
        for tag in ("ReceiptId", "ShipmentId"):
            for el in root.iter(tag):
                if el.text:
                    return el.text
    except ET.ParseError:
        pass
    return ""


async def _sf_sync_item(
    sku: str, name: str, spec: str, unit: str,
    source: str = "bill",
    brand: str = "", barcode: str = "",
) -> bool:
    """同步商品到顺丰，结果落库 ads_sf_item_push_record 便于排查

    2026-04-22 对齐 sf_item_push._build_item_xml：
      - ItemName = 品牌 + 名称 + 规格（空值跳过，静茹 04-21 确认的规则）
      - XML 字段用 <ItemSpecifications>（顺丰标准字段名）
      - 条码字段 <BarCode1>（静茹 04-21 确认，非 ItemBarcode）
      - 过滤 '/' '-' 空格等占位符 69 码
    """
    brand = (brand or "").strip()
    spec = (spec or "").strip()
    pure_name = (name or "").strip()
    full_name = " ".join(p for p in (brand, pure_name, spec) if p)

    item_parts = [
        "<Item>",
        f"<SkuNo>{sku}</SkuNo>",
        f"<ItemName>{full_name}</ItemName>",
        f"<ItemSpecifications>{spec}</ItemSpecifications>",
        f"<ItemUom>{unit}</ItemUom>",
    ]
    bc = (barcode or "").strip()
    if bc and bc not in ("/", "-", "无", "N/A"):
        item_parts.append(f"<BarCode1>{bc}</BarCode1>")
    item_parts.append("</Item>")

    body = (
        f"<ItemRequest><CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<Items>{''.join(item_parts)}</Items></ItemRequest>"
    )
    content = _sf_xml("ITEM_SERVICE", body)
    ok, _, raw = await _sf_send("ITEM_SERVICE", content)
    if not ok:
        logger.warning("商品同步失败 %s: %s", sku, raw[:300])
    # 结果落库（与独立推送入口共享 ads_sf_item_push_record）
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ads_sf_item_push_record (
                    id              BIGSERIAL PRIMARY KEY,
                    sku             TEXT NOT NULL,
                    name            TEXT, spec TEXT, unit TEXT,
                    approval_no     TEXT, license_no TEXT, manufacturer TEXT,
                    company_code    TEXT,
                    status          TEXT NOT NULL,
                    source          TEXT,
                    sf_response     TEXT,
                    error_message   TEXT,
                    pushed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (sku, company_code)
                )
                """
            )
            await conn.execute(
                """
                INSERT INTO ads_sf_item_push_record
                    (sku, name, spec, unit, company_code, status, source, sf_response, error_message, pushed_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                ON CONFLICT (sku, company_code) DO UPDATE SET
                    name = EXCLUDED.name, spec = EXCLUDED.spec, unit = EXCLUDED.unit,
                    status = EXCLUDED.status, source = EXCLUDED.source,
                    sf_response = EXCLUDED.sf_response, error_message = EXCLUDED.error_message,
                    pushed_at = now()
                """,
                sku, name, spec, unit, settings.sf_company_code,
                "success" if ok else "failed",
                source,
                raw[:5000] if ok else None,
                None if ok else raw[:1000],
            )
    except Exception:
        logger.exception("写入 ads_sf_item_push_record 失败")
    return ok


# ── 金蝶 GSP 属性查询 ──

async def _query_material_gsp(client: KingdeeClient, sku: str) -> dict:
    """查金蝶商品的 GSP 属性（注册证号/厂家许可证/生产厂家/规格/品牌/条码）"""
    rows = await client.query(
        "BD_MATERIAL",
        "FNumber,FName,FSpecification,F_BGP_ApprovalNO,F_BGP_ApprovalNo1,"
        "F_BGP_ProductEnt.FName,F_YLYL_Text1,FBARCODE",
        filter_string=f"FNumber = '{sku}'",
        limit=1,
    )
    if not rows:
        return {}
    row = rows[0]
    return {
        "name": row[1] or "",
        "spec": row[2] or "",
        "approval_no": row[3] or "",
        "license_no": row[4] or "",
        "manufacturer": row[5] or "",
        "brand": row[6] or "",
        "barcode": row[7] or "",
    }


# ── 核心逻辑 ──

async def ensure_table():
    """确保 ads_sf_push_record 表存在，并补齐新列"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(DDL_ADS_SF_PUSH_RECORD)
        await conn.execute(DDL_ALTER_PUSH_RECORD)
    logger.info("ads_sf_push_record 表就绪")


async def scan_and_push():
    """扫描金蝶已审核单据，推送到顺丰"""
    pool = await get_pool()

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    since = _scan_since_datetime()
    blacklist = _push_blacklist()
    logger.info(
        "[scan_and_push] 扫描起始 FApproveDate >= %s, 黑名单 %d 条",
        since, len(blacklist),
    )
    total_pushed = 0

    for cfg_key, cfg in BILL_CONFIGS.items():
        form_id = cfg["form_id"]
        bill_type_name = cfg["name"]
        bill_type = cfg["bill_type"]
        direction = cfg["direction"]

        # 两阶段扫描（2026-04-22 修复 limit=500 截断 bug）：
        # 阶段1 只查 FBillNo 识别候选单；阶段2 对每张单按 FBillNo 精准查全部明细
        try:
            filter_str = cfg["filter_tpl"].format(warehouse=SF_WAREHOUSE, since=since)
            scan_rows = await kingdee.query(
                form_id,
                "FBillNo",
                filter_string=filter_str,
                order_string="FApproveDate DESC",
                limit=2000,
            )
        except Exception as e:
            logger.warning("查询%s失败(可能FormId不支持): %s", bill_type_name, e)
            continue

        if not scan_rows or (len(scan_rows) == 1 and isinstance(scan_rows[0], (dict, list)) and len(scan_rows[0]) == 1 and isinstance(scan_rows[0][0] if isinstance(scan_rows[0], list) else None, dict)):
            if scan_rows and isinstance(scan_rows[0], (dict, list)):
                logger.warning("%s 查询返回错误: %s", bill_type_name, str(scan_rows[0])[:200])
                scan_rows = []

        if not scan_rows:
            logger.info("%s: 无新单据", bill_type_name)
            continue

        candidate_bills: list[str] = []
        _seen = set()
        for row in scan_rows:
            if not isinstance(row, list) or len(row) < 1:
                continue
            bno = str(row[0])
            if bno and bno not in _seen:
                _seen.add(bno)
                candidate_bills.append(bno)

        bills: dict[str, list] = {}
        truncated_bills: dict[str, tuple[int, int]] = {}
        for bno in candidate_bills:
            try:
                bill_rows_raw = await kingdee.query(
                    form_id,
                    cfg["field_keys"],
                    filter_string=f"FBillNo='{bno}'",
                    limit=2000,
                )
            except Exception as e:
                logger.warning("%s 查询 %s 明细失败: %s", bill_type_name, bno, e)
                continue
            if not bill_rows_raw or (isinstance(bill_rows_raw[0], dict)):
                continue
            rows_ok = [r for r in bill_rows_raw if isinstance(r, list)]
            if not rows_ok:
                continue

            # 🔒 双重校验
            try:
                verify_rows = await kingdee.query(
                    form_id,
                    "FBillNo,FMaterialId.FNumber",
                    filter_string=f"FBillNo='{bno}'",
                    limit=2000,
                )
                expected_n = len([r for r in (verify_rows or []) if isinstance(r, list)])
                actual_n = len(rows_ok)
                # 幽灵单检测：金蝶精确查询返回0行，单据可能已删除或反审核
                if expected_n == 0:
                    logger.error(
                        "🚨 [push] 幽灵单拒推: %s 金蝶精确查询返回0行，"
                        "单据可能已删除或未审核，拒绝推送",
                        bno,
                    )
                    await _mark_failed(
                        pool, bno,
                        "幽灵单检测：金蝶查不到该单据(已删除或未审核)，拒绝推送",
                    )
                    continue
                if expected_n != actual_n:
                    truncated_bills[bno] = (expected_n, actual_n)
                    logger.error(
                        "🚨 %s 行数不一致：校验%d 实际%d，拒推",
                        bno, expected_n, actual_n,
                    )
                    await _mark_failed(
                        pool, bno,
                        f"行数校验失败：应有 {expected_n} 行，实际只拿到 {actual_n} 行",
                    )
                    continue
            except Exception as e:
                logger.warning("%s 行数校验异常: %s（放行）", bno, e)

            bills[bno] = rows_ok

        if truncated_bills:
            for bno, (exp, act) in truncated_bills.items():
                logger.error(
                    "[SF行数告警-%s] %s 期望%d 实际%d 已标failed等重试",
                    bill_type_name, bno, exp, act,
                )

        if not bills:
            logger.info("%s: 无新单据(阶段2过滤后)", bill_type_name)
            continue

        logger.info(
            "%s: 候选 %d 张，取到明细 %d 张 (共 %d 行)",
            bill_type_name, len(candidate_bills), len(bills),
            sum(len(v) for v in bills.values()),
        )

        async with pool.acquire() as conn:
            existing = await conn.fetch(
                "SELECT bill_no FROM ads_sf_push_record WHERE bill_type = $1 AND status NOT IN ('failed', 'cancelled_by_unaudit', 'unaudit_cancel_failed')",
                bill_type,
            )
            already_pushed = {r["bill_no"] for r in existing}

        for bill_no, bill_rows in bills.items():
            if bill_no in already_pushed:
                continue
            if bill_no in blacklist:
                logger.info("跳过黑名单单据 %s %s", bill_type_name, bill_no)
                continue

            logger.info(
                "推送 %s %s (方向=%s, %d 行商品)",
                bill_type_name, bill_no, direction, len(bill_rows),
            )

            items_data = []
            skus_synced = set()
            failed_skus: list[str] = []

            si = cfg.get("sku_idx", 3)  # sku 起始列
            for row in bill_rows:
                if len(row) < si + 7:
                    logger.warning("跳过不完整的行 (仅 %d 列): %s", len(row), str(row)[:200])
                    continue
                sku = str(row[si])
                name = str(row[si + 1])
                qty = int(float(row[si + 2]))
                unit = str(row[si + 3]) if row[si + 3] else "个"
                lot = str(row[si + 4]) if row[si + 4] else ""
                mfg = str(row[si + 5])[:10] if row[si + 5] else ""
                exp = str(row[si + 6])[:10] if row[si + 6] else ""

                if sku not in skus_synced:
                    gsp = await _query_material_gsp(kingdee, sku)
                    ok_item = await _sf_sync_item(
                        sku, name, gsp.get("spec", ""), unit,
                        source=f"bill_{bill_no}",
                        brand=gsp.get("brand", ""),
                        barcode=gsp.get("barcode", ""),
                    )
                    if not ok_item:
                        failed_skus.append(sku)
                    skus_synced.add(sku)
                else:
                    gsp = {}

                if not gsp and sku in skus_synced:
                    gsp = await _query_material_gsp(kingdee, sku)

                items_data.append({
                    "sku": sku, "name": name, "qty": qty, "unit": unit,
                    "lot": lot, "mfg_date": mfg, "exp_date": exp,
                    "approval_no": gsp.get("approval_no", ""),
                    "license_no": gsp.get("license_no", ""),
                    "manufacturer": gsp.get("manufacturer", ""),
                })

            items_xml = ""
            for idx, item in enumerate(items_data, 1):
                lot_attr2 = f"<LotAttr2>{item['approval_no']}</LotAttr2>" if item["approval_no"] else ""
                lot_attr3 = f"<LotAttr3>{item['license_no']}</LotAttr3>" if item["license_no"] else ""
                lot_attr4 = f"<LotAttr4>{item['manufacturer']}</LotAttr4>" if item["manufacturer"] else ""

                items_xml += (
                    f"<Item>"
                    f"<ErpOrderLineNum>{idx}</ErpOrderLineNum>"
                    f"<SkuNo>{item['sku']}</SkuNo>"
                    f"<ItemName>{item['name']}</ItemName>"
                    f"<Qty>{item['qty']}</Qty>"
                    f"<PlanQty>{item['qty']}</PlanQty>"
                    f"<ItemUom>{item['unit']}</ItemUom>"
                    f"<InventoryStatus>正品</InventoryStatus>"
                    f"<Lot>{item['lot']}</Lot>"
                    f"<MfgDate>{item['mfg_date']}</MfgDate>"
                    f"<ExpDate>{item['exp_date']}</ExpDate>"
                    f"{lot_attr2}{lot_attr3}{lot_attr4}"
                    f"</Item>"
                )

            # 档案推送失败时直接标记单据失败，避免顺丰收到单据但商品档案缺失导致入不了库存
            if failed_skus:
                err_msg = f"商品档案推送失败 {len(failed_skus)} 个，中止单据推送: {failed_skus[:10]}"
                logger.error("%s %s: %s", bill_type_name, bill_no, err_msg)
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ads_sf_push_record
                            (bill_no, bill_type, erp_order, status, items_json, error_message)
                        VALUES ($1, $2, $3, 'failed', $4::jsonb, $5)
                        ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                            status = 'failed',
                            error_message = EXCLUDED.error_message,
                            updated_at = now()
                        """,
                        bill_no, bill_type, bill_no,
                        json.dumps(items_data, ensure_ascii=False),
                        err_msg,
                    )
                continue

            order_date = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

            if direction == "outbound":
                # 调拨出库（SF_WAREHOUSE→其他仓）走 SALE_ORDER_SERVICE
                # ⚠ 顺丰 WMS 要求：
                #   1. SaleOrder 必填 OrderCarrier + OrderReceiverInfo（receiverAddress 不能空）
                #   2. 商品明细节点格式与入库不同：<OrderItem><ItemQuantity> 而非 <Item><Qty>
                # 方案：承运用 ZT(客户自提)，收货方写目标仓库对应的友联主体信息（环境变量可覆盖），
                #      仓库管理员派车到顺丰仓提货后运到目标仓（CK042 不合格品仓等）。

                # 按出库格式重建 items_xml（<OrderItem>/<ItemQuantity>）
                order_items_xml = ""
                for idx, item in enumerate(items_data, 1):
                    order_items_xml += (
                        f"<OrderItem>"
                        f"<ErpOrderLineNum>{idx}</ErpOrderLineNum>"
                        f"<SkuNo>{item['sku']}</SkuNo>"
                        f"<ItemName>{html.escape(item['name'], quote=False)}</ItemName>"
                        f"<ItemQuantity>{item['qty']}</ItemQuantity>"
                        f"<ItemUom>{item['unit']}</ItemUom>"
                        f"<InventoryStatus>正品</InventoryStatus>"
                        f"<Lot>{item['lot']}</Lot>"
                        f"<MfgDate>{item['mfg_date']}</MfgDate>"
                        f"<ExpDate>{item['exp_date']}</ExpDate>"
                        f"</OrderItem>"
                    )
                dst_stock_name = ""
                if len(bill_rows[0]) > 11 and bill_rows[0][11]:
                    dst_stock_name = str(bill_rows[0][11])
                src_stock_name = ""
                if len(bill_rows[0]) > 13 and bill_rows[0][13]:
                    src_stock_name = str(bill_rows[0][13])

                # 兜底收货方（可通过环境变量 SF_INTERNAL_* 覆盖）
                recv_name = os.getenv("SF_INTERNAL_RECEIVER_NAME", "友联仓管")
                recv_mobile = os.getenv("SF_INTERNAL_RECEIVER_MOBILE", "17750577775")
                recv_province = os.getenv("SF_INTERNAL_RECEIVER_PROVINCE", "福建省")
                recv_city = os.getenv("SF_INTERNAL_RECEIVER_CITY", "泉州市")
                recv_area = os.getenv("SF_INTERNAL_RECEIVER_AREA", "丰泽区")
                recv_address = os.getenv(
                    "SF_INTERNAL_RECEIVER_ADDRESS",
                    "泉州市丰泽区宝洲街689号（友联医疗器械有限公司）",
                )
                recv_company = (
                    dst_stock_name
                    or os.getenv("SF_INTERNAL_RECEIVER_COMPANY", "泉州市友联医疗器械有限公司")
                )
                remark = f"内部调拨: {src_stock_name} → {dst_stock_name} ({bill_no})"

                body = (
                    f"<SaleOrderRequest>"
                    f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
                    f"<SaleOrders><SaleOrder>"
                    f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
                    f"<WarehouseCode>{settings.sf_warehouse_code}</WarehouseCode>"
                    f"<ErpOrder>{bill_no}</ErpOrder>"
                    f"<ErpOrderType>40</ErpOrderType>"
                    f"<SFOrderType>40</SFOrderType>"
                    f"<OrderDate>{order_date}</OrderDate>"
                    f"<OrderMark>{html.escape(remark, quote=False)}</OrderMark>"
                    f"<OrderCarrier>"
                    f"<Carrier>ZT</Carrier>"
                    f"<CarrierProduct>ZT</CarrierProduct>"
                    f"<PaymentOfCharge>1</PaymentOfCharge>"
                    f"</OrderCarrier>"
                    f"<OrderReceiverInfo>"
                    f"<ReceiverName>{html.escape(recv_name, quote=False)}</ReceiverName>"
                    f"<ReceiverMobile>{html.escape(recv_mobile, quote=False)}</ReceiverMobile>"
                    f"<ReceiverCountry>中国</ReceiverCountry>"
                    f"<ReceiverProvince>{html.escape(recv_province, quote=False)}</ReceiverProvince>"
                    f"<ReceiverCity>{html.escape(recv_city, quote=False)}</ReceiverCity>"
                    f"<ReceiverArea>{html.escape(recv_area, quote=False)}</ReceiverArea>"
                    f"<ReceiverAddress>{html.escape(recv_address, quote=False)}</ReceiverAddress>"
                    f"<ReceiverCompany>{html.escape(recv_company, quote=False)}</ReceiverCompany>"
                    f"</OrderReceiverInfo>"
                    f"<OrderItems>{order_items_xml}</OrderItems>"
                    f"</SaleOrder></SaleOrders>"
                    f"</SaleOrderRequest>"
                )
                content = _sf_xml("SALE_ORDER_SERVICE", body)
                ok, receipt_id, raw = await _sf_send("SALE_ORDER_SERVICE", content)
            else:
                body = (
                    f"<PurchaseOrderRequest>"
                    f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
                    f"<PurchaseOrders>"
                    f"<PurchaseOrder>"
                    f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
                    f"<WarehouseCode>{settings.sf_warehouse_code}</WarehouseCode>"
                    f"<ErpOrder>{bill_no}</ErpOrder>"
                    f"<ErpOrderType>30</ErpOrderType>"
                    f"<SFOrderType>10</SFOrderType>"
                    f"<OrderDate>{order_date}</OrderDate>"
                    f"<Items>{items_xml}</Items>"
                    f"</PurchaseOrder>"
                    f"</PurchaseOrders>"
                    f"</PurchaseOrderRequest>"
                )
                content = _sf_xml("PURCHASE_ORDER_SERVICE", body)
                ok, receipt_id, raw = await _sf_send("PURCHASE_ORDER_SERVICE", content)

            if not ok and "订单号已存在" in raw:
                ok = True
                logger.info("单据 %s 已在顺丰中，标记为成功", bill_no)
                receipt_id = _extract_receipt_or_shipment_id(raw) or receipt_id

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ads_sf_push_record
                        (bill_no, bill_type, erp_order, sf_receipt_id, status, items_json, sf_response)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                    ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                        sf_receipt_id = EXCLUDED.sf_receipt_id,
                        status = EXCLUDED.status,
                        items_json = EXCLUDED.items_json,
                        sf_response = EXCLUDED.sf_response,
                        error_message = CASE WHEN EXCLUDED.status = 'success'
                                             THEN NULL
                                             ELSE ads_sf_push_record.error_message END,
                        updated_at = now()
                    """,
                    bill_no, bill_type, bill_no,
                    receipt_id,
                    "success" if ok else "failed",
                    json.dumps(items_data, ensure_ascii=False),
                    raw[:2000],
                )

            if ok:
                logger.info("✅ %s %s 推送成功，顺丰收据: %s", bill_type_name, bill_no, receipt_id)
                total_pushed += 1
            else:
                logger.warning("❌ %s %s 推送失败: %s", bill_type_name, bill_no, raw[:300])

    logger.info("本轮扫描完成，成功推送 %d 张单据", total_pushed)
    return total_pushed


async def retry_single_push(record_id: int) -> dict:
    """立即重推一条记录（从金蝶重查最新数据，直推顺丰）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id, bill_no, bill_type, status, fhtz_fid FROM ads_sf_push_record WHERE id = $1",
            record_id,
        )
    if not rec:
        return {"success": False, "message": "记录不存在"}

    bill_no = rec["bill_no"]
    bill_type = rec["bill_type"]

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    if bill_type == OUTBOUND_BILL_TYPE:
        return await _retry_push_outbound(pool, kingdee, rec)
    elif bill_type in {cfg["bill_type"] for cfg in BILL_CONFIGS.values()}:
        return await _retry_push_inbound(pool, kingdee, rec)
    else:
        return {"success": False, "message": f"未知单据类型: {bill_type}"}


async def _retry_push_inbound(pool, kingdee: KingdeeClient, rec) -> dict:
    """重推单条入库单"""
    bill_no = rec["bill_no"]
    bill_type = rec["bill_type"]

    cfg = None
    form_id = None
    for fid, c in BILL_CONFIGS.items():
        if c["bill_type"] == bill_type:
            cfg = c
            form_id = fid
            break
    if not cfg:
        return {"success": False, "message": f"找不到 bill_type={bill_type} 对应的配置"}

    try:
        filter_str = f"FBillNo = '{bill_no}'"
        rows = await kingdee.query(
            form_id, cfg["field_keys"],
            filter_string=filter_str,
            limit=100,
        )
    except Exception as e:
        return {"success": False, "message": f"查询金蝶失败: {e}"}

    if not rows or (isinstance(rows[0], dict)):
        return {"success": False, "message": "金蝶中找不到该单据"}

    si = cfg.get("sku_idx", 3)
    items_data = []
    skus_synced: set[str] = set()
    for row in rows:
        if len(row) < si + 7:
            continue
        sku = str(row[si])
        name = str(row[si + 1])
        qty = int(float(row[si + 2]))
        unit = str(row[si + 3]) if row[si + 3] else "个"
        lot = str(row[si + 4]) if row[si + 4] else ""
        mfg = str(row[si + 5])[:10] if row[si + 5] else ""
        exp = str(row[si + 6])[:10] if row[si + 6] else ""

        if sku not in skus_synced:
            gsp = await _query_material_gsp(kingdee, sku)
            await _sf_sync_item(
                sku, name, gsp.get("spec", ""), unit,
                brand=gsp.get("brand", ""), barcode=gsp.get("barcode", ""),
            )
            skus_synced.add(sku)
        else:
            gsp = await _query_material_gsp(kingdee, sku)

        items_data.append({
            "sku": sku, "name": name, "qty": qty, "unit": unit,
            "lot": lot, "mfg_date": mfg, "exp_date": exp,
            "approval_no": gsp.get("approval_no", ""),
            "license_no": gsp.get("license_no", ""),
            "manufacturer": gsp.get("manufacturer", ""),
        })

    if not items_data:
        return {"success": False, "message": "单据无有效商品行"}

    items_xml = ""
    for idx, item in enumerate(items_data, 1):
        lot_attr2 = f"<LotAttr2>{item['approval_no']}</LotAttr2>" if item["approval_no"] else ""
        lot_attr3 = f"<LotAttr3>{item['license_no']}</LotAttr3>" if item["license_no"] else ""
        lot_attr4 = f"<LotAttr4>{item['manufacturer']}</LotAttr4>" if item["manufacturer"] else ""
        items_xml += (
            f"<Item>"
            f"<ErpOrderLineNum>{idx}</ErpOrderLineNum>"
            f"<SkuNo>{item['sku']}</SkuNo>"
            f"<ItemName>{item['name']}</ItemName>"
            f"<Qty>{item['qty']}</Qty>"
            f"<PlanQty>{item['qty']}</PlanQty>"
            f"<ItemUom>{item['unit']}</ItemUom>"
            f"<InventoryStatus>正品</InventoryStatus>"
            f"<Lot>{item['lot']}</Lot>"
            f"<MfgDate>{item['mfg_date']}</MfgDate>"
            f"<ExpDate>{item['exp_date']}</ExpDate>"
            f"{lot_attr2}{lot_attr3}{lot_attr4}"
            f"</Item>"
        )

    order_date = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    body = (
        f"<PurchaseOrderRequest>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<PurchaseOrders><PurchaseOrder>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<WarehouseCode>{settings.sf_warehouse_code}</WarehouseCode>"
        f"<ErpOrder>{bill_no}</ErpOrder>"
        f"<ErpOrderType>30</ErpOrderType>"
        f"<SFOrderType>10</SFOrderType>"
        f"<OrderDate>{order_date}</OrderDate>"
        f"<Items>{items_xml}</Items>"
        f"</PurchaseOrder></PurchaseOrders>"
        f"</PurchaseOrderRequest>"
    )

    content = _sf_xml("PURCHASE_ORDER_SERVICE", body)
    ok, receipt_id, raw = await _sf_send("PURCHASE_ORDER_SERVICE", content)

    if not ok and "订单号已存在" in raw:
        ok = True
        receipt_id = _extract_receipt_or_shipment_id(raw) or receipt_id

    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE ads_sf_push_record
               SET sf_receipt_id = $1, status = $2, items_json = $3::jsonb,
                   sf_response = $4, error_message = NULL, updated_at = now()
               WHERE id = $5""",
            receipt_id,
            "success" if ok else "failed",
            json.dumps(items_data, ensure_ascii=False),
            raw[:2000],
            rec["id"],
        )

    if ok:
        logger.info("retry OK: %s → %s", bill_no, receipt_id)
    else:
        logger.warning("retry FAIL: %s → %s", bill_no, raw[:300])

    return {
        "success": ok,
        "message": f"{'推送成功' if ok else '推送失败'} {bill_no}",
        "sf_receipt_id": receipt_id if ok else None,
        "error": raw[:500] if not ok else None,
    }


async def _retry_push_outbound(pool, kingdee: KingdeeClient, rec) -> dict:
    """重推单条出库单"""
    bill_no = rec["bill_no"]

    try:
        filter_str = f"FBillNo = '{bill_no}'"
        rows = await kingdee.query(
            OUTBOUND_FORM_ID, OUTBOUND_FIELD_KEYS,
            filter_string=filter_str,
            limit=100,
        )
    except Exception as e:
        return {"success": False, "message": f"查询金蝶失败: {e}"}

    if not rows or isinstance(rows[0], dict):
        return {"success": False, "message": "金蝶中找不到该发货通知单"}

    first = rows[0]
    if len(first) < 19:
        return {"success": False, "message": f"金蝶返回数据列数不足: {len(first)}"}

    fhtz_fid = int(first[0])
    receiver_name = str(first[6]) if first[6] else ""
    receiver_mobile = str(first[7]) if first[7] else ""
    # 2026-04-29: FLinkMan 为空时回退到 FReceiverContactId（基础资料引用）
    if not receiver_name and len(first) > 26 and first[26]:
        receiver_name = str(first[26])
    if not receiver_mobile and len(first) > 27 and first[27]:
        receiver_mobile = str(first[27])
    full_address = str(first[8]) if first[8] else ""
    delivery_code = str(first[9]).strip() if first[9] else ""
    delivery_name = str(first[10]).strip() if first[10] else ""
    customer_code = str(first[4]) if first[4] else ""
    customer_name = str(first[5]) if first[5] else ""

    # 备注拼接（与主推送路径 scan_and_push_outbound 保持一致）
    kd_note = str(first[19]).strip() if len(first) > 19 and first[19] else ""
    _cb_fields = [
        ("F_YLYL_CheckBox",     20),
        ("F_YLYL_CheckBox1",    21),
        ("F_YLYL_CheckBox11",   22),
        ("F_YLYL_CheckBox12",   23),
        ("F_YLYL_CheckBox111",  24),
    ]
    checked_labels = []
    for cb_key, idx in _cb_fields:
        val = bool(first[idx]) if len(first) > idx else False
        label = GSP_CHECKBOX_LABELS.get(cb_key)
        if val and label:
            checked_labels.append(label)
    # F_YLYL_CheckBox1111（首营资料）需 view 接口补齐
    try:
        view_res = await kingdee.view("SAL_DELIVERYNOTICE", number=bill_no)
        view_head = view_res.get("Result", {}).get("Result", {}) or {}
        cb1111 = bool(view_head.get("F_YLYL_CheckBox1111", False))
        lab1111 = GSP_CHECKBOX_LABELS.get("F_YLYL_CheckBox1111")
        if cb1111 and lab1111:
            checked_labels.append(lab1111)
    except Exception:
        pass
    print_template = str(first[25]).strip() if len(first) > 25 and first[25] else ""
    order_mark_parts = []
    if print_template:
        order_mark_parts.append(print_template)
    if checked_labels:
        order_mark_parts.extend(checked_labels)
    if kd_note:
        order_mark_parts.append(kd_note)
    order_mark = "；".join(order_mark_parts)
    if len(order_mark) > 200:
        order_mark = order_mark[:197] + "..."

    if not delivery_code:
        return {"success": False, "message": f"发货通知单 {bill_no} 未选择交货方式，请在金蝶中补充"}
    if not receiver_name:
        return {"success": False, "message": f"发货通知单 {bill_no} 收货人姓名为空，请在金蝶中补充收货联系人"}
    if not full_address:
        return {"success": False, "message": f"发货通知单 {bill_no} 收货地址为空，请在金蝶中补充"}

    province, city, area, detail_addr = _split_address(full_address)

    items_data = []
    skus_synced: set[str] = set()
    for row in rows:
        if len(row) < 19:
            continue
        sku = str(row[12])
        name = str(row[13])
        qty = int(float(row[14]))
        unit = str(row[15]) if row[15] else "个"
        lot = str(row[16]) if row[16] else ""
        mfg = str(row[17])[:10] if row[17] else ""
        exp = str(row[18])[:10] if row[18] else ""

        if sku not in skus_synced:
            gsp = await _query_material_gsp(kingdee, sku)
            await _sf_sync_item(
                sku, name, gsp.get("spec", ""), unit,
                brand=gsp.get("brand", ""), barcode=gsp.get("barcode", ""),
            )
            skus_synced.add(sku)

        items_data.append({
            "sku": sku, "name": name, "qty": qty, "unit": unit,
            "lot": lot, "mfg_date": mfg, "exp_date": exp,
        })

    if not items_data:
        return {"success": False, "message": "单据无有效商品行"}

    items_xml = ""
    for idx, item in enumerate(items_data, 1):
        items_xml += (
            f"<OrderItem>"
            f"<ErpOrderLineNum>{idx}</ErpOrderLineNum>"
            f"<SkuNo>{item['sku']}</SkuNo>"
            f"<ItemName>{html.escape(item['name'], quote=False)}</ItemName>"
            f"<ItemQuantity>{item['qty']}</ItemQuantity>"
            f"<ItemUom>{item['unit']}</ItemUom>"
            f"<InventoryStatus>正品</InventoryStatus>"
            f"<Lot>{item['lot']}</Lot>"
            f"<MfgDate>{item['mfg_date']}</MfgDate>"
            f"<ExpDate>{item['exp_date']}</ExpDate>"
            f"</OrderItem>"
        )

    carrier_tuple = CARRIER_MAP.get(delivery_code, DEFAULT_CARRIER)
    carrier, carrier_product, payment = carrier_tuple
    order_date = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    order_note = _sf_order_note_from_delivery_way(delivery_name, delivery_code)

    body = (
        f"<SaleOrderRequest>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<SaleOrders><SaleOrder>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        f"<WarehouseCode>{settings.sf_warehouse_code}</WarehouseCode>"
        f"<ErpOrder>{bill_no}</ErpOrder>"
        f"<ErpOrderType>10</ErpOrderType>"
        f"<SFOrderType>10</SFOrderType>"
        f"<OrderDate>{order_date}</OrderDate>"
        f"<OrderMark>{html.escape(order_mark, quote=False)}</OrderMark>"
        f"<OrderNote>{html.escape(order_note, quote=False)}</OrderNote>"
        f"<CompanyNote>{html.escape(order_mark, quote=False)}</CompanyNote>"
        f"<ShopName>{html.escape(customer_name, quote=False)}</ShopName>"
        f"<CustomerId>{customer_code}</CustomerId>"
        f"<OrderCarrier>"
        f"<Carrier>{carrier}</Carrier>"
        f"<CarrierProduct>{carrier_product}</CarrierProduct>"
        f"<PaymentOfCharge>{payment}</PaymentOfCharge>"
        f"</OrderCarrier>"
        f"<OrderReceiverInfo>"
        f"<ReceiverName>{html.escape(receiver_name, quote=False)}</ReceiverName>"
        f"<ReceiverMobile>{html.escape(receiver_mobile, quote=False)}</ReceiverMobile>"
        f"<ReceiverCountry>中国</ReceiverCountry>"
        f"<ReceiverProvince>{html.escape(province, quote=False)}</ReceiverProvince>"
        f"<ReceiverCity>{html.escape(city, quote=False)}</ReceiverCity>"
        f"<ReceiverArea>{html.escape(area, quote=False)}</ReceiverArea>"
        f"<ReceiverAddress>{html.escape(full_address, quote=False)}</ReceiverAddress>"
        f"<ReceiverCompany>{html.escape(customer_name, quote=False)}</ReceiverCompany>"
        f"</OrderReceiverInfo>"
        f"<OrderItems>{items_xml}</OrderItems>"
        f"</SaleOrder></SaleOrders>"
        f"</SaleOrderRequest>"
    )

    content = _sf_xml("SALE_ORDER_SERVICE", body)
    ok, receipt_id, raw = await _sf_send("SALE_ORDER_SERVICE", content)

    if not ok and "订单号已存在" in raw:
        ok = True
        receipt_id = _extract_receipt_or_shipment_id(raw) or receipt_id

    # 对于已进入金蝶出库补偿阶段的单（已有 outstock_no），重推顺丰成功后不能回写成 success，
    # 否则会跳过后续 _create_kingdee_outstock 的幂等补偿分支（需要 callback_ok/outstock_failed 才会继续补审）。
    next_status = "success" if ok else "failed"
    if ok and rec.get("outstock_no") and rec.get("status") in ("outstock_failed", "callback_ok", "duplicate_outstock"):
        next_status = "callback_ok"

    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE ads_sf_push_record
               SET sf_receipt_id = $1, status = $2, items_json = $3::jsonb,
                   sf_response = $4, error_message = NULL, fhtz_fid = $5,
                   carrier_code = $6, carrier_name = $7, updated_at = now()
               WHERE id = $8""",
            receipt_id,
            next_status,
            json.dumps(items_data, ensure_ascii=False),
            raw[:2000],
            fhtz_fid,
            carrier,
            CARRIER_CN_NAME.get(carrier, delivery_name),
            rec["id"],
        )

    if ok:
        logger.info("retry outbound OK: %s → %s (carrier=%s)", bill_no, receipt_id, carrier)
    else:
        logger.warning("retry outbound FAIL: %s → %s", bill_no, raw[:300])

    cn_name = CARRIER_CN_NAME.get(carrier, delivery_name)
    return {
        "success": ok,
        "message": f"{'推送成功' if ok else '推送失败'} {bill_no}" + (f" (承运: {cn_name})" if ok else ""),
        "sf_receipt_id": receipt_id if ok else None,
        "carrier": f"{cn_name}({carrier})" if ok else None,
        "error": raw[:500] if not ok else None,
    }


# ── 回调校验 + 超时监控 + 企微告警 ──

CALLBACK_TIMEOUT_MINUTES = 60  # 推送后超过 60 分钟没收到回调 → 告警

# 你的企微 userid（告警接收人）
ALERT_USERS = os.getenv("SF_ALERT_WECOM_USERS", "Ao.oBu")


async def _send_wecom_alert(title: str, content: str):
    """发企微 markdown 告警"""
    try:
        from app.services import wecom_client
        agent_id = settings.wecom_agent_id
        if not agent_id:
            logger.warning("未配置 WECOM_AGENT_ID，跳过告警推送")
            return
        await wecom_client.send_message(
            msgtype="markdown",
            agentid=agent_id,
            payload={
                "touser": ALERT_USERS,
                "markdown": {"content": content},
            },
        )
        logger.info("企微告警已推送: %s", title)
    except Exception as e:
        logger.error("企微告警推送失败: %s", e)


async def validate_callbacks():
    """校验已推送单据的回调状态：数量不一致 / 超时未回调"""
    pool = await get_pool()

    inbound_types = [bt for bt in ("transfer", "transfer_in", "purchase_in") ]
    async with pool.acquire() as conn:
        records = await conn.fetch(
            """
            SELECT id, bill_no, bill_type, erp_order, sf_receipt_id, items_json, created_at, status
            FROM ads_sf_push_record
            WHERE bill_type = ANY($1)
              AND status IN ('success', 'callback_ok', 'callback_mismatch', 'timeout_alert')
            ORDER BY created_at DESC
            LIMIT 200
            """,
            inbound_types,
        )

    if not records:
        return

    now = datetime.now(TZ_CN)
    alerts = []

    async with pool.acquire() as conn:
        for rec in records:
            bill_no = rec["bill_no"]
            current_status = rec["status"]

            # 查回调记录
            callback = await conn.fetchrow(
                """
                SELECT raw_payload, received_at
                FROM ods_sf_push_log
                WHERE erp_order = $1
                  AND service_code = 'PURCHASE_ORDER_INBOUND_PUSH_SERVICE'
                ORDER BY id DESC LIMIT 1
                """,
                bill_no,
            )

            if callback:
                # 有回调 → 校验数量
                try:
                    payload = json.loads(callback["raw_payload"])
                    orders = payload.get("PurchaseOrders", [])
                    if orders:
                        items = orders[0].get("Items", [])
                        actual_total = sum(float(it.get("ActualQty", 0)) for it in items)
                        reject_total = sum(float(it.get("RejectionQty", 0)) for it in items)
                    else:
                        actual_total = 0
                        reject_total = 0
                except (json.JSONDecodeError, KeyError, IndexError):
                    actual_total = 0
                    reject_total = 0

                # 计算我们推的总数量
                try:
                    pushed_items = json.loads(rec["items_json"]) if rec["items_json"] else []
                    pushed_total = sum(it.get("qty", 0) for it in pushed_items)
                except (json.JSONDecodeError, TypeError):
                    pushed_total = 0

                if reject_total > 0 or actual_total != pushed_total:
                    # 数量不一致
                    if current_status != "callback_mismatch":
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'callback_mismatch', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        alert_msg = (
                            f"**⚠️ 顺丰入库数量异常**\n"
                            f"> 单号: {bill_no}\n"
                            f"> 推送数量: {pushed_total}\n"
                            f"> 实收数量: {int(actual_total)}\n"
                            f"> 拒收数量: {int(reject_total)}\n"
                            f"> 回调时间: {callback['received_at']}"
                        )
                        alerts.append(("数量异常", alert_msg))
                        logger.warning("⚠️ %s 入库数量不一致: 推%d 收%d 拒%d",
                                       bill_no, pushed_total, int(actual_total), int(reject_total))
                else:
                    # 数量一致
                    if current_status != "callback_ok":
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'callback_ok', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        logger.info("✅ %s 入库校验通过: 推%d = 收%d", bill_no, pushed_total, int(actual_total))

            else:
                # 没有回调 → 检查是否超时
                if rec["sf_receipt_id"]:
                    # 顺丰已受理（有 PO 回执号）但回调通道不稳定时，避免长期卡在 timeout_alert 误导仓库。
                    # 统一回落为 success，表示“已推送/已受理，待仓库入库回传”。
                    if current_status == "timeout_alert":
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'success', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        logger.info("↩ 入库 %s 有回执号(%s)但无回调，timeout_alert 回落为 success",
                                    bill_no, rec["sf_receipt_id"])
                    continue
                if current_status == "success":
                    # ⚠ asyncpg 查出的 created_at 已是 tz-aware (UTC)，now 也是 tz-aware (TZ_CN)
                    # 直接相减 Python 会自动归一化，千万不能用 .replace(tzinfo=TZ_CN)
                    # 那会把 UTC 时间"强制标注"为北京时间，导致误算差 8 小时 = 480 分钟 → 误报超时
                    elapsed = (now - rec["created_at"]).total_seconds() / 60
                    if elapsed > CALLBACK_TIMEOUT_MINUTES:
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'timeout_alert', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        alert_msg = (
                            f"**⏰ 顺丰入库超时未回调**\n"
                            f"> 单号: {bill_no}\n"
                            f"> 推送时间: {rec['created_at']}\n"
                            f"> 已等待: {int(elapsed)} 分钟\n"
                            f"> 请联系顺丰仓确认入库进度"
                        )
                        alerts.append(("超时未回调", alert_msg))
                        logger.warning("⏰ %s 推送已 %d 分钟未收到回调", bill_no, int(elapsed))

    # 批量发企微告警
    for title, msg in alerts:
        await _send_wecom_alert(title, msg)


# ══════════════════════════════════════════════════════════════
#  出库自动推送（GSP发货通知单 → SALE_ORDER_SERVICE）
# ══════════════════════════════════════════════════════════════

import re

OUTBOUND_FORM_ID = "SAL_DELIVERYNOTICE"
OUTBOUND_BILL_TYPE = "outbound"

OUTBOUND_FIELD_KEYS = (
    "FID,FBillNo,FDocumentStatus,FDate,"
    "FCustomerId.FNumber,FCustomerId.FName,"
    # 收货人姓名/电话在金蝶是文本字段 FLinkMan / FLinkPhone，
    # 不是 FReceiverContactId.FName（那是收货方基础资料引用=客户主档）
    "FLinkMan,FLinkPhone,FReceiveAddress,"
    "FHeadDeliveryWay.FNumber,FHeadDeliveryWay.FDataValue,"
    "FStockId.FNumber,"
    "FMaterialId.FNumber,FMaterialId.FName,"
    "FQty,FUnitID.FName,FLot.FNumber,FProduceDate,FExpiryDate,"
    # 备注 + GSP 勾选框（合并到顺丰 OrderMark，2026-04-21 阿潘确认需求）
    # FIsIncludedTax 是金蝶系统内置字段，全返回 True 无意义（AGENTS.md），
    # 不是界面上 6 个勾选框之一，不查
    # ⚠ F_YLYL_CheckBox1111（首营资料）在金蝶 FormId 元数据里不存在，
    #   虽然 view 接口能返回它，但 ExecuteBillQuery 查询会整体失败。
    #   去掉后只能拿到 5 个勾选框，首营资料这个标签无法识别（影响不大）
    "FNote,"
    "F_YLYL_CheckBox,F_YLYL_CheckBox1,"
    "F_YLYL_CheckBox11,F_YLYL_CheckBox12,"
    "F_YLYL_CheckBox111,"
    # 套打文件（辅助资料引用，2026-04-22 阿潘需求加入 OrderMark）
    # 只打编号（如 56）不打中文（如"友联销售出库单新五联"），
    # 顺丰库房按编号映射到套打模板，中文会被判定为无效模板
    "F_YLYL_Assistant.FNumber,"
    # 2026-04-29 修复：部分发货通知单收货人存在 FReceiverContactId（基础资料引用）
    # 而非 FLinkMan（文本），两个都查，取非空的那个
    "FReceiverContactId.FName,FReceiverContactId.FMobile"
)

# 金蝶 GSP 勾选框字段 → UI 标签
# 2026-04-21 通过 FHTZ555 逐字段精准测试确认映射：
#   F_YLYL_CheckBox      = 是否含税    ✅
#   F_YLYL_CheckBox1     = 发货清单    ✅
#   F_YLYL_CheckBox11    = 货票同行    ✅
#   F_YLYL_CheckBox12    = 检验报告    ✅
#   F_YLYL_CheckBox111   = 后续开票    ✅
#   F_YLYL_CheckBox1111  = 首营资料    ⚠ ExecuteBillQuery 元数据不识别，不查
#   F_YLYL_CheckBox2     = UI 上不存在（隐藏历史字段），不查
# GSP 勾选项：勾选时的显示文案（阿潘 2026-04-22 规则）
# 只在打钩时体现，未打钩直接不打印（跟纸质发货单刀口一致）
# 特别注意：CheckBox1 勾选时打印"发票清单"（不是"发货清单"！阿潘原话）
# 注意：F_YLYL_CheckBox1111（首营资料）ExecuteBillQuery 元数据不识别，
#       需要额外调 View 接口单独补齐
GSP_CHECKBOX_LABELS: dict[str, str] = {
    "F_YLYL_CheckBox":     "含税",
    "F_YLYL_CheckBox1":    "发票清单",
    "F_YLYL_CheckBox11":   "货票同行",
    "F_YLYL_CheckBox12":   "检验报告",
    "F_YLYL_CheckBox111":  "后续开票",
    "F_YLYL_CheckBox1111": "首营资料",
}


def _sf_order_note_from_delivery_way(delivery_name: str, delivery_code: str = "") -> str:
    """顺丰 OrderNote 单独承载金蝶交货方式，例如 司机配送。"""
    note = (delivery_name or "").strip() or (delivery_code or "").strip()
    if len(note) > 200:
        return note[:197] + "..."
    return note


# 金蝶交货方式编码 → 顺丰 WMS 承运商映射
# 格式: 金蝶发货通知单交货方式编码 → (Carrier, CarrierProduct, PaymentOfCharge)
CARRIER_MAP: dict[str, tuple[str, str, str]] = {
    "ZT_AUTO":    ("ZT", "ZT", "1"),   # 金蝶未填交货方式 → 默认客户自提
    "1":          ("CP", "1", "1"),     # 顺丰快递
    "1024":       ("JD", "JD", "1"),    # 京东快递/京邦达
    "1058":       ("YTO", "YTO", "1"),  # 圆通快递
    "JHFS02_SYS": ("ZT", "ZT", "1"),   # 客户自提
}
DEFAULT_CARRIER = ("CP", "1", "1")

# SF 承运商编码 → 金蝶交货方式编码（FNumber）— 回调时反向映射
CARRIER_REVERSE_MAP: dict[str, str] = {
    "CP":  "1",           # 顺丰快递
    "JD":  "1024",        # 京东快递
    "JDKD": "1024",       # 京东快递/京邦达（顺丰回调常用编码）
    "YTO": "1058",        # 圆通快递
    "ZTO": "1059",        # 中通快递
    "STO": "1060",        # 申通快递
    "YD":  "1061",        # 韵达快递
    "ZT":  "JHFS02_SYS",  # 客户自提
}
DELIVERY_WAY_CARRIER_MAP: dict[str, str] = {
    delivery_way: carrier
    for carrier, delivery_way in CARRIER_REVERSE_MAP.items()
}

CARRIER_CN_NAME: dict[str, str] = {
    "CP": "\u987a\u4e30\u901f\u8fd0",
    "YTO": "\u5706\u901a\u5feb\u9012",
    "JD": "\u4eac\u4e1c\u7269\u6d41",
    "JDKD": "\u4eac\u4e1c\u7269\u6d41",
    "ZT": "\u5ba2\u6237\u81ea\u63d0",
}

WAYBILL_CARRIER_PREFIXES: tuple[tuple[str, str], ...] = (
    ("JDVC", "JDKD"),
    ("JDL", "JDKD"),
    ("JD", "JDKD"),
    ("SF", "CP"),
    ("YT", "YTO"),
)


def _normalize_carrier_code(carrier_code: str) -> str:
    code = (carrier_code or "").strip().upper()
    if code == "JD":
        return "JDKD"
    return code


def _infer_carrier_from_waybill(waybill_no: str) -> str:
    normalized = (waybill_no or "").strip().upper()
    for prefix, carrier_code in WAYBILL_CARRIER_PREFIXES:
        if normalized.startswith(prefix):
            return carrier_code
    return ""


def _resolve_actual_carrier(waybill_no: str, actual_carrier: str) -> str:
    """以运单号前缀为准修正承运商，避免 JDVC 被旧 YTO 记录误标为圆通。"""
    return _infer_carrier_from_waybill(waybill_no) or _normalize_carrier_code(actual_carrier)


def _resolve_carrier_from_outstock(waybill_no: str, delivery_way_code: str) -> str:
    """销售出库单以运单号优先，其次用物流方式字段反推承运商。"""
    return _infer_carrier_from_waybill(waybill_no) or DELIVERY_WAY_CARRIER_MAP.get(
        (delivery_way_code or "").strip(),
        "",
    )


async def _sync_push_record_logistics_from_outstock(
    pool,
    kingdee: KingdeeClient,
    record_id: int,
    outstock_no: str,
    *,
    log_prefix: str,
) -> bool:
    """以金蝶销售出库单当前字段回灌 ADS，修正 ZT/null 这类滞后记录。"""
    if not outstock_no:
        return False

    try:
        rows = await kingdee.query(
            "SAL_OUTSTOCK",
            "FBillNo,FCarriageNO,F_YLYL_Assistant.FNumber,F_YLYL_Assistant.FDataValue",
            filter_string=f"FBillNo = '{outstock_no}'",
            limit=1,
        )
    except Exception as e:
        logger.warning("%s: 读取 XSCK 物流字段失败 %s: %s", log_prefix, outstock_no, e)
        return False

    if not rows or not isinstance(rows[0], list):
        return False

    row = rows[0]
    waybill_no = str(row[1]).strip() if len(row) > 1 and row[1] else ""
    delivery_way_code = str(row[2]).strip() if len(row) > 2 and row[2] else ""
    delivery_way_name = str(row[3]).strip() if len(row) > 3 and row[3] else ""
    carrier = _resolve_carrier_from_outstock(waybill_no, delivery_way_code)
    carrier_name = CARRIER_CN_NAME.get(carrier, delivery_way_name)

    if not waybill_no and not carrier:
        return False

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE ads_sf_push_record
            SET waybill_no = CASE WHEN $2::text <> '' THEN $2 ELSE waybill_no END,
                carrier_code = CASE WHEN $3::text <> '' THEN $3 ELSE carrier_code END,
                carrier_name = CASE WHEN $4::text <> '' THEN $4 ELSE carrier_name END,
                updated_at = now()
            WHERE id = $1
            """,
            record_id,
            waybill_no,
            carrier,
            carrier_name,
        )
    logger.info(
        "%s: ADS物流字段已按金蝶XSCK回灌 %s → 运单=%s, 承运=%s",
        log_prefix, outstock_no, waybill_no, carrier,
    )
    return True


async def _sync_recent_outstock_logistics(limit: int = 30) -> dict:
    """补齐近期已建 XSCK 的 ADS 物流字段，处理后补运单/手工修正后的滞后记录。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, bill_no, outstock_no
            FROM ads_sf_push_record
            WHERE bill_type = 'outbound'
              AND status IN ('outstock_created', 'auto_resolved', 'manual_resolved')
              AND outstock_no IS NOT NULL
              AND created_at > NOW() - INTERVAL '14 days'
              AND (
                    waybill_no IS NULL OR waybill_no = ''
                    OR carrier_code IS NULL OR carrier_code = ''
                    OR carrier_code = 'ZT'
                  )
            ORDER BY updated_at ASC
            LIMIT $1
            """,
            limit,
        )

    if not rows:
        return {"total": 0, "synced": 0, "failed": 0}

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    synced = 0
    failed = 0
    for row in rows:
        try:
            ok = await _sync_push_record_logistics_from_outstock(
                pool,
                kingdee,
                row["id"],
                row["outstock_no"],
                log_prefix=f"近期XSCK物流复检 {row['bill_no']}",
            )
            if ok:
                synced += 1
        except Exception as e:
            failed += 1
            logger.warning(
                "近期XSCK物流复检失败: %s/%s err=%s",
                row["bill_no"], row["outstock_no"], e,
            )
    return {"total": len(rows), "synced": synced, "failed": failed}


async def _save_outstock_waybill_and_delivery(
    kingdee: KingdeeClient,
    outstock_id: int,
    outstock_no: str,
    waybill_no: str,
    actual_carrier: str,
    *,
    log_prefix: str,
) -> bool:
    """回填销售出库单运单号和交货方式。

    已存在的 XSCK 可能已经审核，仍先尝试补字段；失败只记录日志，不中断主流程。
    """
    resolved_carrier = _resolve_actual_carrier(waybill_no, actual_carrier)
    if not waybill_no and not resolved_carrier:
        return False

    try:
        save_model: dict = {"FID": outstock_id}
        update_fields = []
        if waybill_no:
            save_model["FCarriageNO"] = waybill_no
            update_fields.append("FCarriageNO")
        if resolved_carrier:
            delivery_way_code = CARRIER_REVERSE_MAP.get(resolved_carrier)
            if delivery_way_code:
                # SAL_OUTSTOCK 的物流方式字段是自定义辅助资料 F_YLYL_Assistant；
                # FHeadDeliveryWay 只存在于发货通知单，出库单上不存在。
                save_model["F_YLYL_Assistant"] = {"FNumber": delivery_way_code}
                update_fields.append("F_YLYL_Assistant")
        if not update_fields:
            return False

        await kingdee.save("SAL_OUTSTOCK", {
            "NeedUpDateFields": update_fields,
            "IsDeleteEntry": "false",
            "Model": save_model,
        })
        logger.info(
            "%s: %s → 运单=%s, 承运=%s, 字段=%s",
            log_prefix, outstock_no, waybill_no, resolved_carrier, ",".join(update_fields),
        )
        return True
    except Exception as e:
        logger.warning(
            "%s失败（不影响主流程）: %s → 运单=%s, 承运=%s, err=%s",
            log_prefix, outstock_no, waybill_no, resolved_carrier, e,
        )
        return False

# 中文地址 → 省/市/区 拆分
_PROVINCE_RE = re.compile(
    r"^(北京市?|天津市?|上海市?|重庆市?|"
    r"[^省市]{2,6}(?:省|自治区|特别行政区))"
)
_CITY_RE = re.compile(
    r"^(.{2,10}?(?:市|自治州|地区|盟))"
)
_AREA_RE = re.compile(
    r"^([^县区旗]{1,10}(?:县|区|旗))"
)


_ZHIXIA = {"北京", "天津", "上海", "重庆"}


async def _mark_failed(pool, bill_no: str, error_msg: str):
    """将推送前校验失败的单据写入 ads_sf_push_record (status=failed)"""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ads_sf_push_record
                (bill_no, bill_type, erp_order, sf_receipt_id, status, error_message)
            VALUES ($1, 'outbound', $1, '', 'failed', $2)
            ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                status = 'failed',
                error_message = EXCLUDED.error_message,
                updated_at = now()
            """,
            bill_no, error_msg,
        )


def _split_address(full_addr: str) -> tuple[str, str, str, str]:
    """从完整地址中拆分省、市、区、详细地址。直辖市省=市。"""
    if not full_addr:
        return ("", "", "", "")
    addr = full_addr.strip()
    province = city = area = ""

    m = _PROVINCE_RE.match(addr)
    if m:
        province = m.group(1)
        addr = addr[m.end():].lstrip()
        raw = province.rstrip("市")
        if raw in _ZHIXIA:
            city = raw + "市"

    if not city:
        m = _CITY_RE.match(addr)
        if m:
            city = m.group(1)
            addr = addr[m.end():].lstrip()

    m = _AREA_RE.match(addr)
    if m:
        area = m.group(1)
        addr = addr[m.end():].lstrip()

    return (province, city, area, addr)


async def scan_and_push_outbound():
    """扫描金蝶已审核的GSP发货通知单（出库仓=SF_WAREHOUSE），推出库单到顺丰"""
    pool = await get_pool()

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    since = _scan_since_datetime()
    blacklist = _push_blacklist()
    logger.info(
        "[scan_and_push_outbound] 扫描起始 FApproveDate >= %s, 黑名单 %d 条",
        since, len(blacklist),
    )
    filter_str = (
        f"FStockId.FNumber = '{SF_WAREHOUSE}' "
        f"AND FDocumentStatus = 'C' "
        f"AND FApproveDate >= '{since}'"
    )

    # ⚠ 两阶段扫描（2026-04-22 修复 XSCK527 明细截断 bug）：
    # 阶段 1：只查 FBillNo+FID，识别近期有哪些 FHTZ 单（每单多行会重复，去重后得候选单号）
    # 阶段 2：对每张候选单按 FBillNo 精准查完整明细，彻底规避 limit 截断
    # 旧代码一次扫全仓 limit=500，一单 69 行的大单会把额度吃爆，单据被截成部分行下推顺丰。
    try:
        scan_rows = await kingdee.query(
            OUTBOUND_FORM_ID,
            "FID,FBillNo",
            filter_string=filter_str,
            order_string="FApproveDate DESC",
            limit=2000,
        )
    except Exception as e:
        logger.warning("查询GSP发货通知单（阶段1）失败: %s", e)
        return 0

    if not scan_rows or (isinstance(scan_rows[0], dict)):
        if scan_rows and isinstance(scan_rows[0], dict):
            logger.warning("GSP发货通知单查询返回错误: %s", str(scan_rows[0])[:200])
        else:
            logger.info("GSP发货通知单: 无新单据")
        return 0

    candidate_bills: list[str] = []
    _seen = set()
    for row in scan_rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        bno = str(row[1])
        if bno and bno not in _seen:
            _seen.add(bno)
            candidate_bills.append(bno)

    logger.info("GSP发货通知单阶段1: 发现 %d 张候选单据", len(candidate_bills))

    # field_keys 列序 (FID 在最前):
    # 0:FID, 1:FBillNo, 2:FDocumentStatus, 3:FDate,
    # 4:FCustomerId.FNumber, 5:FCustomerId.FName,
    # 6:FLinkMan, 7:FLinkPhone, 8:FReceiveAddress,
    # 9:FHeadDeliveryWay.FNumber, 10:FHeadDeliveryWay.FDataValue,
    # 11:FStockId.FNumber,
    # 12:FMaterialId.FNumber, 13:FMaterialId.FName,
    # 14:FQty, 15:FUnitID.FName, 16:FLot.FNumber, 17:FProduceDate, 18:FExpiryDate
    # 19:FNote, 20-24:CheckBox系列, 25:F_YLYL_Assistant.FNumber,
    # 26:FReceiverContactId.FName, 27:FReceiverContactId.FMobile

    bills: dict[str, list] = {}
    truncated_bills: dict[str, tuple[int, int]] = {}  # 行数校验失败的单子 {bno: (期望, 实际)}
    for bno in candidate_bills:
        try:
            bill_rows_raw = await kingdee.query(
                OUTBOUND_FORM_ID,
                OUTBOUND_FIELD_KEYS,
                filter_string=f"FBillNo='{bno}'",
                limit=2000,
            )
        except Exception as e:
            logger.warning("查询 %s 明细失败: %s", bno, e)
            continue
        if not bill_rows_raw or isinstance(bill_rows_raw[0], dict):
            continue
        rows_ok = [r for r in bill_rows_raw if isinstance(r, list) and len(r) >= 19]
        if not rows_ok:
            continue

        # 🔒 双重校验（2026-04-22 新增，XSCK527 事故后补的保护闸）：
        # 独立再用极简字段 query 一次同一张单的行数，对不上拒推
        # 防御点：金蝶分页/limit 截断、XML 解析异常、字段键跑偏导致某些行被过滤
        try:
            verify_rows = await kingdee.query(
                OUTBOUND_FORM_ID,
                "FBillNo,FMaterialId.FNumber",
                filter_string=f"FBillNo='{bno}'",
                limit=2000,
            )
            expected_n = len([r for r in (verify_rows or []) if isinstance(r, list)])
            actual_n = len(rows_ok)
            if expected_n > 0 and expected_n != actual_n:
                truncated_bills[bno] = (expected_n, actual_n)
                logger.error(
                    "🚨 %s 行数不一致：校验查询 %d 行 vs 明细查询 %d 行，拒绝推送",
                    bno, expected_n, actual_n,
                )
                await _mark_failed(
                    pool, bno,
                    f"金蝶行数校验失败：应有 {expected_n} 行，实际只拿到 {actual_n} 行。"
                    f"可能触发金蝶查询 limit 截断，不推送以防订单残缺。已自动告警，下一轮扫描会自动重试。",
                )
                continue
        except Exception as e:
            logger.warning("%s 行数校验失败: %s（放行不拦截，避免误杀）", bno, e)

        bills[bno] = rows_ok

    logger.info(
        "GSP发货通知单阶段2: 候选 %d 张，取到完整明细 %d 张 (共 %d 行)，行数不一致被拒 %d 张",
        len(candidate_bills), len(bills),
        sum(len(v) for v in bills.values()), len(truncated_bills),
    )

    # 企微告警：行数不一致的单据（走 sys_alert 或 logger，后续可接 webhook）
    if truncated_bills:
        for bno, (exp, act) in truncated_bills.items():
            logger.error(
                "[SF行数告警] %s 期望%d行 实际%d行 已标记failed等下轮重试",
                bno, exp, act,
            )

    async with pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT bill_no FROM ads_sf_push_record WHERE bill_type = $1 AND status NOT IN ('failed', 'cancelled_by_unaudit', 'unaudit_cancel_failed')",
            OUTBOUND_BILL_TYPE,
        )
        already_pushed = {r["bill_no"] for r in existing}

    total_pushed = 0
    for bill_no, bill_rows in bills.items():
        if bill_no in already_pushed:
            continue
        if bill_no in blacklist:
            logger.info("跳过黑名单发货通知单 %s", bill_no)
            continue

        first = bill_rows[0]
        fhtz_fid = int(first[0])
        receiver_name = str(first[6]) if first[6] else ""
        receiver_mobile = str(first[7]) if first[7] else ""
        # 2026-04-29: FLinkMan 为空时回退到 FReceiverContactId（基础资料引用）
        if not receiver_name and len(first) > 26 and first[26]:
            receiver_name = str(first[26])
        if not receiver_mobile and len(first) > 27 and first[27]:
            receiver_mobile = str(first[27])
        full_address = str(first[8]) if first[8] else ""
        delivery_code = str(first[9]).strip() if first[9] else ""
        delivery_name = str(first[10]).strip() if first[10] else ""
        customer_code = str(first[4]) if first[4] else ""
        customer_name = str(first[5]) if first[5] else ""
        # 备注拼接（阿潘 2026-04-22 规则）：套打编号 ；客户 ；勾选项(仅勾选) ；备注
        # OUTBOUND_FIELD_KEYS 顺序: ..., FNote[19], CheckBox[20], CheckBox1[21],
        #                                  CheckBox11[22], CheckBox12[23], CheckBox111[24],
        #                                  F_YLYL_Assistant.FNumber[25]（编号，非中文）
        kd_note = str(first[19]).strip() if len(first) > 19 and first[19] else ""
        _cb_fields = [
            ("F_YLYL_CheckBox",     20),
            ("F_YLYL_CheckBox1",    21),
            ("F_YLYL_CheckBox11",   22),
            ("F_YLYL_CheckBox12",   23),
            ("F_YLYL_CheckBox111",  24),
        ]
        # 阿潘 2026-04-22 规则：只有勾选才打印标签，未勾选直接跳过
        checked_labels = []
        for cb_key, idx in _cb_fields:
            val = bool(first[idx]) if len(first) > idx else False
            label = GSP_CHECKBOX_LABELS.get(cb_key)
            if val and label:
                checked_labels.append(label)
        # F_YLYL_CheckBox1111（首营资料）ExecuteBillQuery 元数据不识别，额外调 View 补齐
        try:
            view_res = await kingdee.view("SAL_DELIVERYNOTICE", number=bill_no)
            view_head = view_res.get("Result", {}).get("Result", {}) or {}
            cb1111 = bool(view_head.get("F_YLYL_CheckBox1111", False))
            lab1111 = GSP_CHECKBOX_LABELS.get("F_YLYL_CheckBox1111")
            if cb1111 and lab1111:
                checked_labels.append(lab1111)
        except Exception as e:
            logger.warning("拉取 %s CheckBox1111(首营资料)失败，跳过：%s", bill_no, e)
        print_template = str(first[25]).strip() if len(first) > 25 and first[25] else ""
        # 顺序：套打 ；勾选项 ；备注（方案 B，2026-04-22 静茹反馈备注太长，
        # 客户名(商家)改走独立 ShopName 字段，不再拼入 OrderMark/OrderNote/CompanyNote）
        order_mark_parts = []
        if print_template:
            order_mark_parts.append(print_template)
        if checked_labels:
            order_mark_parts.extend(checked_labels)
        if kd_note:
            order_mark_parts.append(kd_note)
        order_mark = "；".join(order_mark_parts)
        # 顺丰 OrderMark 安全截断（一般限制 ≤ 500 字节，保守控制到 200 字符）
        if len(order_mark) > 200:
            order_mark = order_mark[:197] + "..."
        logger.info(
            "OrderMark构造 %s: 套打=%r 商家(ShopName)=%r 勾选=%r 备注=%r → %r",
            bill_no, print_template, customer_name, checked_labels, kd_note, order_mark
        )

        if not delivery_code:
            # 金蝶未填交货方式 → 默认 ZT 客户自提（顺丰把货挑出来，客户自己来拿）
            # 业务确认 2026-04-21：先不拦截，自提模式 ReceiverCompany=客户名 即可
            logger.info("%s 金蝶未选交货方式，自动走 ZT 自提", bill_no)
            delivery_code = "ZT_AUTO"
            delivery_name = "客户自提(默认)"

        if not receiver_name or not full_address:
            missing = []
            if not receiver_name:
                missing.append("收货人姓名")
            if not full_address:
                missing.append("收货地址")
            logger.warning("跳过 %s: 缺少 %s", bill_no, "+".join(missing))
            await _mark_failed(
                pool, bill_no,
                f"金蝶单据缺少必填字段: {'+'.join(missing)}，请在金蝶中补充后自动重推",
            )
            continue

        province, city, area, detail_addr = _split_address(full_address)

        logger.info(
            "推送出库 %s → %s %s (%d 行商品)",
            bill_no, receiver_name, province + city, len(bill_rows),
        )

        items_data = []
        skus_synced = set()
        failed_skus: list[str] = []
        for row in bill_rows:
            sku = str(row[12])
            name = str(row[13])
            qty = int(float(row[14]))
            unit = str(row[15]) if row[15] else "个"
            lot = str(row[16]) if row[16] else ""
            mfg = str(row[17])[:10] if row[17] else ""
            exp = str(row[18])[:10] if row[18] else ""

            if sku not in skus_synced:
                gsp = await _query_material_gsp(kingdee, sku)
                ok_item = await _sf_sync_item(
                    sku, name, gsp.get("spec", ""), unit,
                    source=f"bill_{bill_no}",
                    brand=gsp.get("brand", ""),
                    barcode=gsp.get("barcode", ""),
                )
                if not ok_item:
                    failed_skus.append(sku)
                skus_synced.add(sku)
            else:
                gsp = await _query_material_gsp(kingdee, sku)

            items_data.append({
                "sku": sku, "name": name, "qty": qty, "unit": unit,
                "lot": lot, "mfg_date": mfg, "exp_date": exp,
            })

        if failed_skus:
            err_msg = f"商品档案推送失败 {len(failed_skus)} 个，中止出库单推送: {failed_skus[:10]}"
            logger.error("出库 %s: %s", bill_no, err_msg)
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ads_sf_push_record
                        (bill_no, bill_type, erp_order, status, items_json, error_message, fhtz_fid)
                    VALUES ($1, 'outbound', $1, 'failed', $2::jsonb, $3, $4)
                    ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                        status = 'failed',
                        error_message = EXCLUDED.error_message,
                        updated_at = now()
                    """,
                    bill_no,
                    json.dumps(items_data, ensure_ascii=False),
                    err_msg,
                    fhtz_fid,
                )
            continue

        # 🔒 推送前最终闸（2026-04-22 新增）：再独立去金蝶核一次行数
        # 三道保险：阶段2精准查 → 独立COUNT校验 → 推送前最后核对
        # 防止 items_data 在 sku_sync / 循环里被意外过滤
        try:
            final_verify = await kingdee.query(
                OUTBOUND_FORM_ID,
                "FBillNo,FMaterialId.FNumber",
                filter_string=f"FBillNo='{bill_no}'",
                limit=2000,
            )
            kd_final_n = len([r for r in (final_verify or []) if isinstance(r, list)])
            if kd_final_n > 0 and kd_final_n != len(items_data):
                err_msg = (
                    f"推送前闸拦截：金蝶 {kd_final_n} 行 vs items_data {len(items_data)} 行，"
                    f"数据不一致，取消推送"
                )
                logger.error("🚨 %s %s", bill_no, err_msg)
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ads_sf_push_record
                            (bill_no, bill_type, erp_order, sf_receipt_id, status, error_message, fhtz_fid)
                        VALUES ($1, 'outbound', $1, '', 'failed', $2, $3)
                        ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                            status='failed', error_message=EXCLUDED.error_message, updated_at=now()
                        """,
                        bill_no, err_msg, fhtz_fid,
                    )
                continue
        except Exception as e:
            logger.warning("%s 推送前闸异常: %s（放行）", bill_no, e)

        items_xml = ""
        for idx, item in enumerate(items_data, 1):
            items_xml += (
                f"<OrderItem>"
                f"<ErpOrderLineNum>{idx}</ErpOrderLineNum>"
                f"<SkuNo>{item['sku']}</SkuNo>"
                f"<ItemName>{html.escape(item['name'], quote=False)}</ItemName>"
                f"<ItemQuantity>{item['qty']}</ItemQuantity>"
                f"<ItemUom>{item['unit']}</ItemUom>"
                f"<InventoryStatus>正品</InventoryStatus>"
                f"<Lot>{item['lot']}</Lot>"
                f"<MfgDate>{item['mfg_date']}</MfgDate>"
                f"<ExpDate>{item['exp_date']}</ExpDate>"
                f"</OrderItem>"
            )

        carrier_tuple = CARRIER_MAP.get(delivery_code)
        if not carrier_tuple:
            logger.warning("交货方式 %s(%s) 未在映射表中，使用默认顺丰标快", delivery_code, delivery_name)
            carrier_tuple = DEFAULT_CARRIER
        carrier, carrier_product, payment = carrier_tuple
        order_date = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
        order_note = _sf_order_note_from_delivery_way(delivery_name, delivery_code)

        body = (
            f"<SaleOrderRequest>"
            f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
            f"<SaleOrders><SaleOrder>"
            f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
            f"<WarehouseCode>{settings.sf_warehouse_code}</WarehouseCode>"
            f"<ErpOrder>{bill_no}</ErpOrder>"
            f"<ErpOrderType>10</ErpOrderType>"
            f"<SFOrderType>10</SFOrderType>"
            f"<OrderDate>{order_date}</OrderDate>"
            # 2026-04-22 李静茹(顺丰)确认：打印模板取 OrderNote 或 CompanyNote，不是 OrderMark
            # 2026-06-05 业务确认：OrderNote 改放交货方式（如司机配送），
            # CompanyNote 保留原套打/GSP勾选/备注字段，避免两个字段继续重复。
            # 2026-04-22 静茹第二次反馈：备注太长，商家名单独放 ShopName 字段
            f"<OrderMark>{html.escape(order_mark, quote=False)}</OrderMark>"
            f"<OrderNote>{html.escape(order_note, quote=False)}</OrderNote>"
            f"<CompanyNote>{html.escape(order_mark, quote=False)}</CompanyNote>"
            f"<ShopName>{html.escape(customer_name, quote=False)}</ShopName>"
            f"<CustomerId>{customer_code}</CustomerId>"
            f"<OrderCarrier>"
            f"<Carrier>{carrier}</Carrier>"
            f"<CarrierProduct>{carrier_product}</CarrierProduct>"
            f"<PaymentOfCharge>{payment}</PaymentOfCharge>"
            f"</OrderCarrier>"
            f"<OrderReceiverInfo>"
            f"<ReceiverName>{html.escape(receiver_name, quote=False)}</ReceiverName>"
            f"<ReceiverMobile>{html.escape(receiver_mobile, quote=False)}</ReceiverMobile>"
            f"<ReceiverCountry>中国</ReceiverCountry>"
            f"<ReceiverProvince>{html.escape(province, quote=False)}</ReceiverProvince>"
            f"<ReceiverCity>{html.escape(city, quote=False)}</ReceiverCity>"
            f"<ReceiverArea>{html.escape(area, quote=False)}</ReceiverArea>"
            f"<ReceiverAddress>{html.escape(full_address, quote=False)}</ReceiverAddress>"
            f"<ReceiverCompany>{html.escape(customer_name, quote=False)}</ReceiverCompany>"
            f"</OrderReceiverInfo>"
            f"<OrderItems>{items_xml}</OrderItems>"
            f"</SaleOrder></SaleOrders>"
            f"</SaleOrderRequest>"
        )

        content = _sf_xml("SALE_ORDER_SERVICE", body)
        ok, receipt_id, raw = await _sf_send("SALE_ORDER_SERVICE", content)

        if not ok and "订单号已存在" in raw:
            ok = True
            logger.info("出库单 %s 已在顺丰中，标记为成功", bill_no)
            receipt_id = _extract_receipt_or_shipment_id(raw) or receipt_id

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ads_sf_push_record
                    (bill_no, bill_type, erp_order, sf_receipt_id, status, items_json,
                     sf_response, fhtz_fid, carrier_code, carrier_name)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
                ON CONFLICT (bill_no, bill_type) DO UPDATE SET
                    sf_receipt_id = EXCLUDED.sf_receipt_id,
                    status = EXCLUDED.status,
                    items_json = EXCLUDED.items_json,
                    sf_response = EXCLUDED.sf_response,
                    error_message = CASE WHEN EXCLUDED.status = 'success'
                                         THEN NULL
                                         ELSE ads_sf_push_record.error_message END,
                    fhtz_fid = EXCLUDED.fhtz_fid,
                    carrier_code = EXCLUDED.carrier_code,
                    carrier_name = EXCLUDED.carrier_name,
                    updated_at = now()
                """,
                bill_no, OUTBOUND_BILL_TYPE, bill_no,
                receipt_id,
                "success" if ok else "failed",
                json.dumps(items_data, ensure_ascii=False),
                raw[:2000],
                fhtz_fid,
                carrier,
                CARRIER_CN_NAME.get(carrier, delivery_name),
            )

        if ok:
            logger.info("✅ 出库 %s 推送成功，顺丰收据: %s", bill_no, receipt_id)
            total_pushed += 1
        else:
            logger.warning("❌ 出库 %s 推送失败: %s", bill_no, raw[:300])

    logger.info("出库扫描完成，成功推送 %d 张单据", total_pushed)
    return total_pushed


async def _lookup_fhtz_fid(bill_no: str) -> int | None:
    """从金蝶查询发货通知单的内部 ID（兼容旧记录没有 fhtz_fid 的情况）"""
    try:
        kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
        kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
        kingdee = KingdeeClient(username=kd_user, password=kd_pass)
        await kingdee.login()
        rows = await kingdee.query(
            "SAL_DELIVERYNOTICE", "FID",
            filter_string=f"FBillNo = '{bill_no}'",
            limit=1,
        )
        if rows:
            return int(rows[0][0])
    except Exception as e:
        logger.warning("查询 %s FID 失败: %s", bill_no, e)
    return None


async def _create_kingdee_outstock(
    bill_no: str, fhtz_fid: int, record_id: int, waybill_no: str = "",
    via_wms_fallback: bool = False, actual_carrier: str = "",
):
    """顺丰出库回调校验通过后，下推发货通知单→金蝶销售出库单→Submit→Audit

    金蝶单据流转标准路径：
    发货通知单 → Push 下推 → 出库单(草稿A)
    → Save 回填运单号 → Submit(B) → Audit(C)
    → 金蝶自动触发应收单

    ⚠ 2026-04-22 加并发锁：用 PG advisory_lock(按 bill_no 哈希)
      防止手动触发 + 定时任务并发时，两个协程同时查到"0张 XSCK"然后都 Push，
      导致金蝶里产生 2~4 张重复 XSCK（阿潘 4/22 事故根因：54 张 FHTZ 被重复下推）
    """
    pool = await get_pool()
    # 按 bill_no 哈希成一个 int8 给 advisory_lock
    # pg_try_advisory_lock 非阻塞；如果锁已被同事务持有就立即返回 False → 跳过本轮
    lock_key = abs(hash(f"sf_outstock:{bill_no}")) % (2**31)
    async with pool.acquire() as conn:
        got_lock = await conn.fetchval("SELECT pg_try_advisory_lock($1)", lock_key)
        if not got_lock:
            logger.warning("⏭ %s 正在被其他协程处理，跳过本轮（并发保护）", bill_no)
            return None
    try:
        return await _create_kingdee_outstock_locked(
            bill_no, fhtz_fid, record_id, waybill_no, lock_key,
            via_wms_fallback=via_wms_fallback,
            actual_carrier=actual_carrier,
        )
    finally:
        async with pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_unlock($1)", lock_key)


async def _create_kingdee_outstock_locked(
    bill_no: str, fhtz_fid: int, record_id: int, waybill_no: str, lock_key: int,
    via_wms_fallback: bool = False, actual_carrier: str = "",
):
    """_create_kingdee_outstock 的真实实现，已拿到 advisory lock

    via_wms_fallback=True 时表示走的是 WMS 兜底路径（顺丰 WMS=已出库 但回调没收到），
    建完 XSCK 后状态记为 'outstock_created_no_callback' 而非 'outstock_created'，
    在 dashboard 上标红，让阿潘能识别"业务已补救但回调通道有问题"，便于事后对账+找顺丰排查。
    """
    pool = await get_pool()
    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    # XSCK 建好后写库要用的目标状态：
    # - 正常路径（有回调+数量校验通过）→ 'outstock_created'（绿）
    # - WMS 兜底（回调缺失，业务已发货）→ 'outstock_created_no_callback'（红，需人工对账）
    final_ok_status = (
        "outstock_created_no_callback" if via_wms_fallback else "outstock_created"
    )

    outstock_no = ""
    outstock_id = 0
    try:
        # 幂等性检查：先看金蝶中是否已有从该发货通知单下推的出库单
        # ⚠ limit=5（不是 1）：防开单员人工重复下推导致金蝶里并存多张 XSCK
        # 只要发现 > 1 张立即告警 + 停手，不自动选哪张处理（避免选错更糟）
        existing = await kingdee.query(
            "SAL_OUTSTOCK", "FID,FBillNo,FDocumentStatus",
            filter_string=f"FSrcBillNo = '{bill_no}'",
            limit=5,
        )
        if existing and not isinstance(existing[0], dict):
            # 按 FBillNo 去重（保险起见，ExecuteBillQuery 返回应该已经按头字段去重）
            unique_xscks: dict[str, tuple[int, str]] = {}
            for row in existing:
                if isinstance(row, list) and len(row) >= 3:
                    xbno = str(row[1])
                    if xbno and xbno not in unique_xscks:
                        unique_xscks[xbno] = (int(row[0]), str(row[2]))

            if len(unique_xscks) > 1:
                # ⚠ 金蝶里并存多张 XSCK（多半是开单员手工重复下推）→ 告警+停手
                xscks_info = ", ".join(
                    f"{xb}(状态={st})" for xb, (_, st) in sorted(unique_xscks.items())
                )
                error_msg = (
                    f"FHTZ {bill_no} 在金蝶里对应 {len(unique_xscks)} 张 XSCK: {xscks_info}。"
                    f"可能是开单员手工重复下推，请在金蝶反审核并删除多余的那张（保留 {outstock_no or '任意一张'}），"
                    f"否则库存会多扣。代码本轮跳过不处理。"
                )
                logger.error("⚠ 重复下推: %s", error_msg)

                first_xb = sorted(unique_xscks.keys())[0]
                async with pool.acquire() as conn:
                    # 告警限流：先查旧 status，只有"首次"进入 duplicate_outstock 才发企微
                    prev_status = await conn.fetchval(
                        "SELECT status FROM ads_sf_push_record WHERE id = $1",
                        record_id,
                    )
                    await conn.execute(
                        """UPDATE ads_sf_push_record
                           SET status = 'duplicate_outstock',
                               outstock_no = COALESCE(outstock_no, $1),
                               error_message = $2, updated_at = now()
                           WHERE id = $3""",
                        first_xb, error_msg[:500], record_id,
                    )

                if prev_status != "duplicate_outstock":
                    await _send_wecom_alert(
                        "⚠ 金蝶 FHTZ 重复下推",
                        f"**FHTZ 在金蝶里对应多张 XSCK，库存可能多扣**\n"
                        f"> 发货通知单: {bill_no}\n"
                        f"> 关联出库单: {xscks_info}\n"
                        f"> **请尽快在金蝶反审核并删除多余的那张**\n"
                        f"> 代码本轮已停手，等人工清理干净后下轮自动恢复处理",
                    )
                return None

            # 正常单张 XSCK，继续原有逻辑
            only_xb = sorted(unique_xscks.keys())[0]
            outstock_id, doc_status = unique_xscks[only_xb]
            outstock_no = only_xb
            logger.info("出库单 %s 已存在（状态=%s），跳过 Push", outstock_no, doc_status)

            # 已存在的 XSCK 也要先补运单号/交货方式；旧逻辑在已审核 C 时提前 return，会漏掉历史单。
            await _save_outstock_waybill_and_delivery(
                kingdee, outstock_id, outstock_no, waybill_no, actual_carrier,
                log_prefix="补运单号+交货方式",
            )
            await _sync_push_record_logistics_from_outstock(
                pool, kingdee, record_id, outstock_no,
                log_prefix="已存在XSCK物流回灌",
            )

            if doc_status == "C":
                async with pool.acquire() as conn:
                    await conn.execute(
                        f"""UPDATE ads_sf_push_record
                           SET status = '{final_ok_status}', outstock_no = $1,
                               error_message = NULL, updated_at = now()
                           WHERE id = $2""",
                        outstock_no, record_id,
                    )
                return outstock_no

            # 出库单已存在但未审核（A=草稿/B=已提交），尝试补 Submit+Audit
            if doc_status == "A":
                await asyncio.sleep(5)
                await kingdee.submit("SAL_OUTSTOCK", {"Ids": str(outstock_id)})
                logger.info("补 Submit 成功: %s", outstock_no)
            # A→Submit→B, 或者已经是 B
            await asyncio.sleep(3)
            await kingdee.audit("SAL_OUTSTOCK", {
                "Ids": str(outstock_id),
                "IsVerifyProcInst": "false",
            })
            logger.info("补 Audit 成功: %s", outstock_no)
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""UPDATE ads_sf_push_record
                       SET status = '{final_ok_status}', outstock_no = $1,
                           error_message = NULL, updated_at = now()
                       WHERE id = $2""",
                    outstock_no, record_id,
                )
            return outstock_no

        # Push 下推：发货通知单 → 出库单
        push_result = await kingdee.push("SAL_DELIVERYNOTICE", {
            "Ids": str(fhtz_fid),
            "TargetFormId": "SAL_OUTSTOCK",
            "IsEnableDefaultRule": "true",
            "IsDraftWhenSaveFail": "true",
        })
        entities = push_result.get("entities", [])
        if not entities:
            raise RuntimeError(f"Push 返回空: {push_result}")

        outstock_id = entities[0]["Id"]
        outstock_no = entities[0].get("Number", "")
        logger.info("Push 成功: %s → %s (Id=%d)", bill_no, outstock_no, outstock_id)

        # Save 回填运单号 + 交货方式（Push 生成的草稿状态 A 可以 Save）
        await _save_outstock_waybill_and_delivery(
            kingdee, outstock_id, outstock_no, waybill_no, actual_carrier,
            log_prefix="运单号+交货方式已回填",
        )
        await _sync_push_record_logistics_from_outstock(
            pool, kingdee, record_id, outstock_no,
            log_prefix="新建XSCK物流回灌",
        )

        # 等待金蝶 GSP 模块完成内部处理（批次可用数量分配等）
        await asyncio.sleep(5)

        # Submit
        await kingdee.submit("SAL_OUTSTOCK", {"Ids": str(outstock_id)})
        logger.info("Submit 成功: %s", outstock_no)

        await asyncio.sleep(3)

        # Audit (GSP 单据需要 IsVerifyProcInst=false 跳过工作流校验)
        audit_result = await kingdee.audit("SAL_OUTSTOCK", {
            "Ids": str(outstock_id),
            "IsVerifyProcInst": "false",
        })
        logger.info("Audit 成功: %s → %s", outstock_no, audit_result)

        # 更新记录
        async with pool.acquire() as conn:
            await conn.execute(
                f"""UPDATE ads_sf_push_record
                   SET status = '{final_ok_status}', outstock_no = $1, updated_at = now()
                   WHERE id = $2""",
                outstock_no, record_id,
            )

        # 企微通知
        ar_msg = ""
        messages = audit_result.get("messages", [])
        for m in messages:
            msg_text = m.get("Message", "")
            if "应收单" in msg_text:
                ar_msg = f"\n> 应收单: {msg_text}"
                break

        alert_content = (
            f"**✅ 金蝶出库单已自动创建**\n"
            f"> 发货通知单: {bill_no}\n"
            f"> 出库单: {outstock_no}\n"
            f"> 状态: 已审核{ar_msg}"
        )
        await _send_wecom_alert("出库单自动创建", alert_content)
        return outstock_no

    except Exception as e:
        error_msg = str(e)
        # 金蝶如果已经有人工或自动下推过并审核了这张出库单，重复 Push/Submit/Audit 会返回
        # "单据编号为'XSCKxxx'的销售出库单，单据已经审核"。这种情况目标状态已达成，当作成功处理。
        if ("单据已经审核" in error_msg or "单据已审核" in error_msg
                or "已经审核" in error_msg):
            m = re.search(r"XSCK\d+", error_msg)
            already_no = m.group(0) if m else (outstock_no or "")
            logger.info("出库单已审核（幂等成功）: %s → %s", bill_no, already_no)
            if already_no:
                await _sync_push_record_logistics_from_outstock(
                    pool, kingdee, record_id, already_no,
                    log_prefix="已审核XSCK物流回灌",
                )
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""UPDATE ads_sf_push_record
                       SET status = '{final_ok_status}', outstock_no = $1,
                           error_message = NULL, updated_at = now()
                       WHERE id = $2""",
                    already_no or None, record_id,
                )
            return already_no or None

        # 幂等判定 2（2026-04-23）：金蝶对一张已提交(B)或已审核(C)的 XSCK 再调 Submit 会返回
        # "单据编号为'XSCKxxx'的销售出库单，只有暂存、创建和重新审核的数据才允许提交"。
        # 这种情况下重查 FDocumentStatus：C → 成功；B → 补 Audit；其它 → 视作真失败。
        if "只有暂存、创建和重新审核的数据才允许提交" in error_msg:
            m = re.search(r"XSCK\d+", error_msg)
            err_xb = m.group(0) if m else (outstock_no or "")
            if err_xb:
                try:
                    recheck = await kingdee.query(
                        "SAL_OUTSTOCK", "FID,FDocumentStatus",
                        filter_string=f"FBillNo = '{err_xb}'",
                        limit=1,
                    )
                    if recheck and isinstance(recheck[0], list) and len(recheck[0]) >= 2:
                        x_id = int(recheck[0][0])
                        cur_status = str(recheck[0][1])
                        if cur_status == "C":
                            logger.info(
                                "出库单已审核（Submit 被拒实为幂等）: %s → %s",
                                bill_no, err_xb,
                            )
                            await _sync_push_record_logistics_from_outstock(
                                pool, kingdee, record_id, err_xb,
                                log_prefix="Submit幂等XSCK物流回灌",
                            )
                            async with pool.acquire() as conn:
                                await conn.execute(
                                    f"""UPDATE ads_sf_push_record
                                       SET status = '{final_ok_status}', outstock_no = $1,
                                           error_message = NULL, updated_at = now()
                                       WHERE id = $2""",
                                    err_xb, record_id,
                                )
                            return err_xb
                        if cur_status == "B":
                            try:
                                await kingdee.audit("SAL_OUTSTOCK", {
                                    "Ids": str(x_id),
                                    "IsVerifyProcInst": "false",
                                })
                                logger.info(
                                    "出库单从 B 补 Audit 成功: %s → %s", bill_no, err_xb,
                                )
                                await _sync_push_record_logistics_from_outstock(
                                    pool, kingdee, record_id, err_xb,
                                    log_prefix="补Audit后XSCK物流回灌",
                                )
                                async with pool.acquire() as conn:
                                    await conn.execute(
                                        f"""UPDATE ads_sf_push_record
                                           SET status = '{final_ok_status}', outstock_no = $1,
                                               error_message = NULL, updated_at = now()
                                           WHERE id = $2""",
                                        err_xb, record_id,
                                    )
                                return err_xb
                            except Exception as audit_err:
                                logger.warning(
                                    "XSCK %s 补 Audit 失败，保留 outstock_failed: %s",
                                    err_xb, audit_err,
                                )
                except Exception as recheck_err:
                    logger.warning("重查 XSCK %s 状态失败: %s", err_xb, recheck_err)

        logger.error("创建出库单失败 %s: %s", bill_no, error_msg)
        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE ads_sf_push_record
                   SET status = 'outstock_failed', error_message = $1,
                       outstock_no = $2, updated_at = now()
                   WHERE id = $3""",
                error_msg[:500], outstock_no or None, record_id,
            )
        alert_content = (
            f"**❌ 金蝶出库单创建失败**\n"
            f"> 发货通知单: {bill_no}\n"
            f"> 出库单: {outstock_no or '未生成'}\n"
            f"> 错误: {error_msg[:200]}"
        )
        await _send_wecom_alert("出库单创建失败", alert_content)
        return None


async def validate_outbound_callbacks():
    """校验出库单回调 + 自动创建金蝶出库单

    流程：
    1. 检查 callback_ok/mismatch/timeout
    2. callback_ok 且尚未创建出库单 → 自动 Push 下推创建
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        # ⚠ 不加 LIMIT：高峰日一天就能 >200 张，加 LIMIT 会把早上推的单挤出窗口永远无法回写金蝶。
        # 只扫未终结状态（outstock_created / outstock_failed / cancelled 已终结，跳过）。
        records = await conn.fetch(
            """
            SELECT id, bill_no, bill_type, erp_order, sf_receipt_id,
                   items_json, created_at, status, fhtz_fid, outstock_no,
                   sf_wms_status_text, waybill_no, carrier_code
            FROM ads_sf_push_record
            WHERE bill_type = $1
              AND status IN ('success', 'callback_ok', 'callback_mismatch',
                             'timeout_alert', 'duplicate_outstock')
            ORDER BY created_at DESC
            """,
            OUTBOUND_BILL_TYPE,
        )

    if not records:
        return

    now = datetime.now(TZ_CN)
    alerts = []
    # 任务格式: (bill_no, fhtz_fid, record_id, waybill, via_wms_fallback, carrier_code)
    # via_wms_fallback=True 时建出来的 XSCK 状态记为 outstock_created_no_callback（异常+已补救）
    outstock_tasks: list[tuple[str, int, int, str, bool, str]] = []

    async with pool.acquire() as conn:
        for rec in records:
            bill_no = rec["bill_no"]
            current_status = rec["status"]

            if current_status in ("outstock_created", "outstock_failed", "cancelled"):
                continue

            callback = await conn.fetchrow(
                """
                SELECT raw_payload, received_at
                FROM ods_sf_push_log
                WHERE erp_order = $1
                  AND service_code = 'SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE'
                ORDER BY id DESC LIMIT 1
                """,
                bill_no,
            )

            if callback:
                try:
                    payload = json.loads(callback["raw_payload"])
                    orders = payload.get("SaleOrders", payload.get("SaleOrder", []))
                    if isinstance(orders, dict):
                        orders = [orders]
                    if orders:
                        items = orders[0].get("Items", orders[0].get("OrderItems", []))
                        if isinstance(items, dict):
                            items = [items]
                        actual_total = sum(float(it.get("ActualQty", it.get("ItemQuantity", 0))) for it in items)
                    else:
                        actual_total = 0
                except (json.JSONDecodeError, KeyError, IndexError, TypeError):
                    actual_total = 0

                try:
                    pushed_items = json.loads(rec["items_json"]) if rec["items_json"] else []
                    pushed_total = sum(it.get("qty", 0) for it in pushed_items)
                except (json.JSONDecodeError, TypeError):
                    pushed_total = 0

                if actual_total != pushed_total:
                    if current_status != "callback_mismatch":
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'callback_mismatch', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        alert_msg = (
                            f"**⚠️ 顺丰出库数量异常**\n"
                            f"> 单号: {bill_no}\n"
                            f"> 推送数量: {pushed_total}\n"
                            f"> 实出数量: {int(actual_total)}\n"
                            f"> 回调时间: {callback['received_at']}"
                        )
                        alerts.append(("出库数量异常", alert_msg))
                        logger.warning("⚠️ 出库 %s 数量不一致: 推%d 出%d", bill_no, pushed_total, int(actual_total))
                else:
                    cb_waybill_for_save = ""
                    cb_carrier_for_save = ""
                    try:
                        _p = json.loads(callback["raw_payload"]) if isinstance(callback["raw_payload"], str) else callback["raw_payload"]
                        _o = _p.get("SaleOrders", [])
                        if isinstance(_o, dict):
                            _o = [_o]
                        if _o:
                            cb_waybill_for_save = _o[0].get("WayBillNo", "")
                            cb_carrier_for_save = _resolve_actual_carrier(
                                cb_waybill_for_save,
                                _o[0].get("Carrier", "") or rec.get("carrier_code") or "",
                            )
                    except (json.JSONDecodeError, KeyError, TypeError):
                        pass

                    if cb_waybill_for_save or cb_carrier_for_save:
                        await conn.execute(
                            """
                            UPDATE ads_sf_push_record
                            SET waybill_no = COALESCE(NULLIF($2, ''), waybill_no),
                                carrier_code = COALESCE(NULLIF($3, ''), carrier_code),
                                carrier_name = COALESCE(NULLIF($4, ''), carrier_name),
                                updated_at = now()
                            WHERE id = $1
                            """,
                            rec["id"],
                            cb_waybill_for_save,
                            cb_carrier_for_save,
                            CARRIER_CN_NAME.get(cb_carrier_for_save, ""),
                        )

                    if current_status not in ("callback_ok", "outstock_created", "outstock_failed"):
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'callback_ok', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        logger.info("✅ 出库 %s 校验通过: 推%d = 出%d, 运单=%s", bill_no, pushed_total, int(actual_total), cb_waybill_for_save)

                    fhtz_fid = rec.get("fhtz_fid")
                    outstock_no = rec.get("outstock_no")

                    if current_status != "outstock_created" and not outstock_no:
                        if not fhtz_fid:
                            fhtz_fid = await _lookup_fhtz_fid(bill_no)
                            if fhtz_fid:
                                await conn.execute(
                                    "UPDATE ads_sf_push_record SET fhtz_fid = $1 WHERE id = $2",
                                    fhtz_fid, rec["id"],
                                )
                        cb_waybill = ""
                        try:
                            cb_payload = json.loads(callback["raw_payload"])
                            cb_orders = cb_payload.get("SaleOrders", [])
                            if isinstance(cb_orders, dict):
                                cb_orders = [cb_orders]
                            if cb_orders:
                                cb_waybill = cb_orders[0].get("WayBillNo", "")
                                cb_carrier_for_save = _resolve_actual_carrier(
                                    cb_waybill,
                                    cb_orders[0].get("Carrier", "") or cb_carrier_for_save,
                                )
                        except (json.JSONDecodeError, KeyError, TypeError):
                            pass
                        if fhtz_fid:
                            outstock_tasks.append((bill_no, fhtz_fid, rec["id"], cb_waybill, False, cb_carrier_for_save or rec.get("carrier_code") or ""))

                    elif outstock_no and current_status in ("outstock_failed", "callback_ok", "duplicate_outstock"):
                        # Push 已做过但可能未审核完成，或者之前被标为重复下推（等人工清理后自动恢复），触发幂等重试（带运单号）
                        retry_waybill = ""
                        retry_carrier = cb_carrier_for_save
                        try:
                            rp = json.loads(callback["raw_payload"]) if isinstance(callback["raw_payload"], str) else callback["raw_payload"]
                            ro = rp.get("SaleOrders", [])
                            if isinstance(ro, dict):
                                ro = [ro]
                            if ro:
                                retry_waybill = ro[0].get("WayBillNo", "")
                                retry_carrier = _resolve_actual_carrier(
                                    retry_waybill,
                                    ro[0].get("Carrier", "") or retry_carrier,
                                )
                        except (json.JSONDecodeError, KeyError, TypeError):
                            pass
                        outstock_tasks.append((bill_no, fhtz_fid or 0, rec["id"], retry_waybill, False, retry_carrier or rec.get("carrier_code") or ""))

            else:
                # ─── 兜底逻辑（2026-04-28）─────────────────────────────────
                # 场景：自提单 / 圆通等非顺丰承运 → 顺丰几乎不发
                #   SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE 回调，
                #   或回调字段 erp_order 对不上，导致 ods_sf_push_log 永远查不到。
                # 但 WMS 状态查询接口能拿到 OrderStatus=2900 已出库（已由 sync_wms_status_batch
                # 写入 sf_wms_status_text=已出库）。出库已成既定事实：
                #   - 必须建 XSCK（业务不能耽误，阿潘要发车）
                #   - 但状态不能伪装成正常 outstock_created（会掩盖通道异常）
                #   → 用专属状态 outstock_created_no_callback：dashboard 飘红 + 企微告警
                #     便于事后对账 + 找顺丰排查回调通道
                # 风险：跳过了「推送数量 = 实出数量」校验。但实出数量 < 推送数量这种偏差在
                #   顺丰 WMS 推 2900 之前必然会先暴露在前置回调或人工取消，本兜底只在
                #   "WMS 已确认完成出库 + 我方 callback 通道断" 时启动，整体风险可控。
                wms_text = (rec.get("sf_wms_status_text") or "").strip()
                if (current_status in ("success", "timeout_alert")
                        and wms_text in ("已出库", "已发货")
                        and not rec.get("outstock_no")):
                    fhtz_fid = rec.get("fhtz_fid")
                    if not fhtz_fid:
                        fhtz_fid = await _lookup_fhtz_fid(bill_no)
                        if fhtz_fid:
                            await conn.execute(
                                "UPDATE ads_sf_push_record SET fhtz_fid = $1 WHERE id = $2",
                                fhtz_fid, rec["id"],
                            )
                    if fhtz_fid:
                        fallback_waybill = rec.get("waybill_no") or ""
                        fallback_carrier = _resolve_actual_carrier(
                            fallback_waybill,
                            rec.get("carrier_code") or "",
                        )
                        logger.warning(
                            "🔁 WMS兜底: %s 顺丰 WMS=已出库 但回调缺失，建 XSCK 后将标为 outstock_created_no_callback (FHTZ_FID=%d, 运单=%s)",
                            bill_no, fhtz_fid, fallback_waybill,
                        )
                        alerts.append((
                            "⚠️ 顺丰回调缺失（业务已发货）",
                            f"**顺丰 WMS 已出库但我方未收到回调**\n"
                            f"> 单号: {bill_no}\n"
                            f"> 承运: {fallback_carrier or '未知'}\n"
                            f"> 运单: {fallback_waybill or '无'}\n"
                            f"> 处理: 已自动建 XSCK（标记为「回调缺失」状态，请抽查对账）"
                        ))
                        outstock_tasks.append(
                            (bill_no, fhtz_fid, rec["id"], fallback_waybill, True, fallback_carrier),
                        )
                    else:
                        logger.warning("WMS兜底: %s 找不到 FHTZ_FID，无法建 XSCK", bill_no)
                    continue
                # ─── 兜底逻辑结束 ──────────────────────────────────────────

                if current_status == "success":
                    if rec["sf_receipt_id"]:
                        logger.debug("出库 %s 已有 ShipmentId=%s，跳过超时检测", bill_no, rec["sf_receipt_id"])
                        continue
                    # ⚠ 同入库逻辑，两者都是 tz-aware 直接相减，不能 .replace(tzinfo=TZ_CN)
                    elapsed = (now - rec["created_at"]).total_seconds() / 60
                    if elapsed > CALLBACK_TIMEOUT_MINUTES:
                        await conn.execute(
                            "UPDATE ads_sf_push_record SET status = 'timeout_alert', updated_at = now() WHERE id = $1",
                            rec["id"],
                        )
                        alert_msg = (
                            f"**⏰ 顺丰出库超时未回调**\n"
                            f"> 单号: {bill_no}\n"
                            f"> 推送时间: {rec['created_at']}\n"
                            f"> 已等待: {int(elapsed)} 分钟\n"
                            f"> 请联系顺丰仓确认出库进度"
                        )
                        alerts.append(("出库超时未回调", alert_msg))
                        logger.warning("⏰ 出库 %s 推送已 %d 分钟未收到回调", bill_no, int(elapsed))

    for title, msg in alerts:
        await _send_wecom_alert(title, msg)

    for bill_no, fhtz_fid, record_id, waybill, via_fallback, carrier in outstock_tasks:
        logger.info(
            "%s开始创建金蝶出库单: %s (FID=%d, 运单=%s, 承运=%s)",
            "[WMS兜底]" if via_fallback else "回调校验通过，",
            bill_no, fhtz_fid, waybill, carrier,
        )
        await _create_kingdee_outstock(
            bill_no, fhtz_fid, record_id,
            waybill_no=waybill, via_wms_fallback=via_fallback,
            actual_carrier=carrier,
        )

    sync_result = await _sync_recent_outstock_logistics(limit=30)
    if sync_result["synced"] or sync_result["failed"]:
        logger.info("近期XSCK物流复检完成: %s", sync_result)


# ─────────────────────────────────────────────────────────────────────────────
# 反审核自动撤回（2026-04-28 新增）
#
# 业务场景（阿潘 2026-04-28 提出）：金蝶单据已审核 → 推到顺丰 WMS → 后来在金蝶
# 反审核（FDocumentStatus 从 'C' 退回 'B'/'A'）。如果顺丰还没开始包装/收货，
# 应该自动调取消接口收回，避免顺丰仍按原单作业；如果已经发货/已收货导致
# 取消失败，则告警阿潘人工处理。
#
# ⚠ 唯一触发条件：金蝶 FDocumentStatus IN ('A','B','D') = 反审核
#
# status 候选集合是「反向定义」：排除已经走到终态的记录（扫了也没意义），
# 剩下的全部交给金蝶状态去判断。换句话说，撤不撤完全由金蝶决定，
# status 候选只是单纯优化扫描效率，不参与"能不能撤"的业务判断。
#
# 失败兜底：撤回失败的单标 unaudit_cancel_failed 不再重扫；如需再试由人工把
# status 改回原值（或在 sf_dashboard 加 reset 按钮）。
# ─────────────────────────────────────────────────────────────────────────────

# 已终结状态：业务已闭环，扫了也无法/无意义触发撤回
#   - outstock_created* : 出库金蝶 XSCK 已建（业务终结）
#   - cancelled         : 人工已撤
#   - cancelled_by_unaudit / unaudit_cancel_failed : 已被本任务处理过
#   - auto_resolved / manual_resolved : 异常单已被复检/人工救回
#   - failed            : 推送时直接报错，顺丰侧没有这张单
#   - pending           : 根本没推过去
_TERMINAL_STATUSES = (
    "outstock_created",
    "outstock_created_no_callback",
    "cancelled",
    "cancelled_by_unaudit",
    "cancelled_by_deletion",
    "unaudit_cancel_failed",
    "auto_resolved",
    "manual_resolved",
    "failed",
    "pending",
)

# 出库 callback_ok 表示顺丰已贴单准备发货，调取消几乎 99% 拒，排除。
# 入库 callback_ok 仍要进删单/反审核监控：仓库可能还没真正实收完成，
# 即便顺丰拒绝取消，也要落 unaudit_cancel_failed 并告警，不能静默漏掉。
_OUTBOUND_EXTRA_TERMINAL = ("callback_ok",)
_INBOUND_EXTRA_TERMINAL: tuple[str, ...] = ()

# bill_type → 金蝶 FormId（反查 FDocumentStatus 用）
_BILL_TYPE_TO_FORM = {
    "outbound":        "SAL_DELIVERYNOTICE",
    "transfer":        "STK_TransferDirect",
    "transfer_out":    "STK_TransferDirect",
    "transfer_in":     "STK_TransferIn",
    "transfer_in_out": "STK_TransferIn",
    "purchase_in":     "STK_InStock",
}
_OUTBOUND_BILL_TYPES = {"outbound", "transfer_out", "transfer_in_out"}
_INBOUND_BILL_TYPES = {"transfer", "transfer_in", "purchase_in"}

UNAUDIT_BATCH_LIMIT = 50           # 每轮最多撤回数量，防失控
UNAUDIT_SCAN_DAYS = 14             # 只看最近 14 天的推送记录


def _is_cancel_success(resp: dict) -> bool:
    """判断顺丰取消接口的回包是否成功

    回包结构（出/入库取消通用）：<Head>SUCCESS|ERROR</Head> + <Error code="...">
    """
    if not isinstance(resp, dict):
        return False
    head = (resp.get("head") or "").upper()
    if head in ("SUCCESS", "OK", "ACCEPT", "ACCEPTED"):
        return True
    return False


async def _query_kingdee_doc_status(
    kingdee: KingdeeClient, form_id: str, bill_no: str
) -> tuple[str | None, str | None]:
    """查金蝶单据当前的 FDocumentStatus + FApproveDate"""
    try:
        rows = await kingdee.query(
            form_id,
            "FBillNo,FDocumentStatus,FApproveDate",
            filter_string=f"FBillNo='{bill_no}'",
            limit=5,
        )
    except Exception as e:
        logger.warning("[unaudit] 查 %s/%s 状态失败: %s", form_id, bill_no, e)
        return None, None
    if not rows or isinstance(rows[0], dict):
        return None, None
    for row in rows:
        if isinstance(row, list) and len(row) >= 2 and str(row[0]) == bill_no:
            status = str(row[1]) if row[1] is not None else None
            approve_date = str(row[2]) if len(row) >= 3 and row[2] is not None else None
            return status, approve_date
    return None, None


async def scan_unaudit_and_cancel():
    """扫描金蝶反审核的已推顺丰单，自动调撤回接口

    只加不改：与 scan_and_push / scan_and_push_outbound / validate_callbacks 等
    已有任务并行运行，互不影响。

    流程：
      1. 从 ads_sf_push_record 取出近 14 天内、status 在候选集合的记录
      2. 按 (bill_type → form_id) 分组，逐张查金蝶 FDocumentStatus
      3. FDocumentStatus IN ('A','B') 的命中反审核：
         - 出库 → cancel_outbound_order(remark="金蝶反审核自动撤回")
         - 入库 → cancel_inbound_order
      4. 成功 → status=cancelled_by_unaudit + 通知阿潘
         失败 → status=unaudit_cancel_failed + 高优告警阿潘
    """
    from app.services.sf_outbound import (
        cancel_outbound_order,
        cancel_inbound_order,
    )

    pool = await get_pool()

    # advisory lock 防并发（与其它定时任务错开）
    async with pool.acquire() as conn:
        got = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext('sf_unaudit_watcher'))"
        )
        if not got:
            logger.info("[unaudit] 已有同名任务在跑，跳过本轮")
            return 0

    try:
        kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
        kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
        kingdee = KingdeeClient(username=kd_user, password=kd_pass)
        await kingdee.login()

        # 1. 拉候选记录（近 14 天 + 不在终态 + 不在方向特定的额外终态）
        # 业务真正的撤回触发条件是"金蝶反审核"，这里 status 过滤纯属优化
        # 扫描效率：把已终结的单子排除掉，免得每次都去金蝶查一遍状态
        excluded_statuses = list(
            set(_TERMINAL_STATUSES)
            | set(_OUTBOUND_EXTRA_TERMINAL)
            | set(_INBOUND_EXTRA_TERMINAL)
        )
        async with pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT id, bill_no, bill_type, erp_order, status, sf_receipt_id, created_at
                FROM ads_sf_push_record
                WHERE status <> ALL($1::text[])
                  AND bill_type = ANY($2::text[])
                  AND created_at >= now() - ($3 || ' days')::interval
                ORDER BY created_at DESC
                LIMIT 1000
                """,
                excluded_statuses,
                list(_BILL_TYPE_TO_FORM.keys()),
                str(UNAUDIT_SCAN_DAYS),
            )

        if not records:
            logger.info("[unaudit] 无候选记录")
            return 0

        logger.info("[unaudit] 候选记录 %d 条，开始逐张校验金蝶状态", len(records))

        # 2. 逐张校验 + 撤回（限流 + 上限保护）
        cancelled_ok = 0
        cancelled_fail = 0
        deletion_cancelled: list[str] = []
        for rec in records:
            if cancelled_ok + cancelled_fail >= UNAUDIT_BATCH_LIMIT:
                logger.warning(
                    "[unaudit] 单轮已撤回 %d 张，达上限 %d，剩余顺延下轮",
                    cancelled_ok + cancelled_fail, UNAUDIT_BATCH_LIMIT,
                )
                break

            bill_no = rec["bill_no"]
            bill_type = rec["bill_type"]
            erp_order = rec["erp_order"] or bill_no
            rec_id = rec["id"]

            if bill_type not in _BILL_TYPE_TO_FORM:
                continue
            form_id = _BILL_TYPE_TO_FORM[bill_type]

            doc_status, approve_date = await _query_kingdee_doc_status(kingdee, form_id, bill_no)
            await asyncio.sleep(0.3)  # 限速保护金蝶

            if doc_status is None:
                # 单据在金蝶中可能已被删除 — 需要自动撤回顺丰推送
                # 安全检查：只处理确实推送成功的记录
                if rec["status"] not in ("success", "callback_ok", "outstock_created"):
                    logger.info("[unaudit] %s 状态=%s 非成功推送，跳过删除检测", bill_no, rec["status"])
                    continue

                # 防误判：连续查2次，间隔3秒，都查不到才认定为删除
                logger.warning("[unaudit] %s/%s 金蝶查不到(疑似删除)，3秒后重试确认", form_id, bill_no)
                await asyncio.sleep(3)
                doc_status_retry, _ = await _query_kingdee_doc_status(kingdee, form_id, bill_no)
                if doc_status_retry is not None:
                    logger.info("[unaudit] %s 重试后查到状态=%s，非删除，跳过", bill_no, doc_status_retry)
                    continue

                # 确认删除，执行取消
                logger.warning("[unaudit] %s/%s 金蝶确认已删除，触发自动撤回", form_id, bill_no)
                is_outbound = bill_type in _OUTBOUND_BILL_TYPES
                try:
                    if is_outbound:
                        resp = await cancel_outbound_order(
                            erp_order=erp_order,
                            shipment_id=(rec["sf_receipt_id"] or ""),
                            remark="金蝶单据已删除自动撤回",
                        )
                    else:
                        resp = await cancel_inbound_order(erp_order=erp_order)
                except Exception as e:
                    logger.exception("[unaudit] %s 删除撤回异常: %s", bill_no, e)
                    resp = {"head": "EXCEPTION", "error": str(e)[:500]}

                ok = _is_cancel_success(resp)
                err_msg = (resp.get("error") or "")[:500] if isinstance(resp, dict) else ""

                if ok:
                    async with pool.acquire() as conn2:
                        await conn2.execute(
                            """UPDATE ads_sf_push_record
                               SET status='cancelled_by_deletion',
                                   cancel_reason='金蝶单据已删除自动撤回',
                                   cancelled_at=now(), updated_at=now()
                               WHERE id=$1""",
                            rec_id,
                        )
                    cancelled_ok += 1
                    deletion_cancelled.append(f"{bill_no}({bill_type})")
                    logger.info("[unaudit] ✅ %s 金蝶已删除 → 顺丰取消成功", bill_no)
                    direction_cn = "出库" if is_outbound else "入库"
                    await _send_wecom_alert(
                        "🗑️ 金蝶删单自动撤回成功",
                        f"**金蝶单据已删除，顺丰自动撤回**\n\n"
                        f"- 单号: `{bill_no}`\n"
                        f"- 类型: {direction_cn}（{bill_type}）\n"
                        f"- 处理: 已通知顺丰撤单成功",
                    )
                else:
                    cancelled_fail += 1
                    logger.error("[unaudit] ❌ %s 金蝶已删除但撤回失败: %s", bill_no, err_msg)
                    direction_cn = "出库" if is_outbound else "入库"
                    await _send_wecom_alert(
                        "🚨 金蝶删单撤回失败-需人工",
                        f"**金蝶单据已删除，但顺丰拒绝撤回**\n\n"
                        f"- 单号: `{bill_no}`\n"
                        f"- 类型: {direction_cn}（{bill_type}）\n"
                        f"- 顺丰回包: {err_msg or '(无 error 字段)'}\n"
                        f"- 操作建议: 联系顺丰仓核实并人工处理",
                    )

                await asyncio.sleep(0.5)
                continue

            if doc_status == "C":
                # 检测是否为"反审核→改内容→重新审核"
                # 条件: status=success 且 金蝶审核日期 > 我们的推送时间
                if rec["status"] == "success" and approve_date:
                    try:
                        kd_approve = approve_date.replace("T", " ")
                        if "+" not in kd_approve and len(kd_approve) <= 19:
                            kd_approve_dt = datetime.strptime(kd_approve, "%Y-%m-%d %H:%M:%S").replace(
                                tzinfo=TZ_CN
                            )
                        else:
                            kd_approve_dt = datetime.fromisoformat(kd_approve)
                        push_time = rec["created_at"]
                        if push_time.tzinfo is None:
                            push_time = push_time.replace(tzinfo=TZ_CN)
                        # 如果金蝶审核日期比我们推送时间晚 60 秒以上，说明被重新审核过
                        if kd_approve_dt > push_time + timedelta(seconds=60):
                            logger.warning(
                                "[unaudit] 重新审核检测: %s 取消旧单并重推, 金蝶审核=%s > 推送=%s",
                                bill_no, kd_approve_dt, push_time,
                            )
                            # 走取消+重推流程
                            is_outbound = bill_type in _OUTBOUND_BILL_TYPES
                            try:
                                if is_outbound:
                                    resp = await cancel_outbound_order(
                                        erp_order=erp_order,
                                        shipment_id=(rec["sf_receipt_id"] or ""),
                                        remark="金蝶重新审核自动撤回重推",
                                    )
                                else:
                                    resp = await cancel_inbound_order(erp_order=erp_order)
                            except Exception as e:
                                logger.exception("[unaudit] 取消旧单异常: %s", bill_no)
                                resp = {"head": "EXCEPTION", "error": str(e)[:500]}

                            ok = _is_cancel_success(resp)
                            if ok:
                                async with pool.acquire() as conn2:
                                    await conn2.execute(
                                        """UPDATE ads_sf_push_record
                                           SET status='cancelled_by_unaudit',
                                               cancel_reason='金蝶重新审核自动撤回重推',
                                               cancelled_at=now(), updated_at=now()
                                           WHERE id=$1""",
                                        rec_id,
                                    )
                                # 重推
                                try:
                                    retry_result = await retry_single_push(rec_id)
                                    reaudit_label = "成功" if retry_result.get("success") else "失败"
                                    logger.info("[unaudit] 重新审核重推%s: %s → %s", reaudit_label, bill_no, retry_result)
                                except Exception:
                                    logger.exception("[unaudit] 重新审核重推异常: %s", bill_no)
                                await _send_wecom_alert(
                                    "🔄 金蝶重新审核自动重推",
                                    f"**检测到单据被重新审核（反审→改内容→再审核）**\n\n"
                                    f"- 单号: `{bill_no}`\n"
                                    f"- 金蝶审核: {approve_date}\n"
                                    f"- 原推送: {rec['created_at']}\n"
                                    f"- 处理: 旧单已取消，新单已重推",
                                )
                                cancelled_ok += 1
                            else:
                                err_msg = (resp.get("error") or "")[:500] if isinstance(resp, dict) else ""
                                logger.error("[unaudit] 重新审核但取消旧单失败: %s err=%s", bill_no, err_msg)
                                await _send_wecom_alert(
                                    "🚨 重新审核取消旧单失败",
                                    f"**金蝶重新审核，但旧顺丰单取消失败**\n\n"
                                    f"- 单号: `{bill_no}`\n"
                                    f"- 错误: {err_msg}\n"
                                    f"- 需人工处理",
                                )
                                cancelled_fail += 1
                            await asyncio.sleep(0.5)
                            continue
                    except Exception:
                        logger.debug("[unaudit] 解析审核日期失败: %s approve_date=%s", bill_no, approve_date)
                continue  # 正常已审核，跳过

            # FDocumentStatus 'A'(草稿) / 'B'(已提交未审核) / 'D'(暂存) → 视为反审核
            logger.warning(
                "[unaudit] 发现反审核单: %s (%s) FDocumentStatus=%s, status=%s",
                bill_no, bill_type, doc_status, rec["status"],
            )

            # 标记发现时间（不论后续撤回成败，都先留痕）
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE ads_sf_push_record
                    SET unaudit_detected_at = COALESCE(unaudit_detected_at, now()),
                        updated_at = now()
                    WHERE id = $1
                    """,
                    rec_id,
                )

            # 调顺丰取消接口
            is_outbound = bill_type in _OUTBOUND_BILL_TYPES
            try:
                if is_outbound:
                    resp = await cancel_outbound_order(
                        erp_order=erp_order,
                        shipment_id=(rec["sf_receipt_id"] or ""),
                        remark="金蝶反审核自动撤回",
                    )
                else:
                    resp = await cancel_inbound_order(erp_order=erp_order)
            except Exception as e:
                logger.exception("[unaudit] 调顺丰取消接口异常: %s/%s", bill_type, bill_no)
                resp = {"head": "EXCEPTION", "error": str(e)[:500]}

            ok = _is_cancel_success(resp)
            err_msg = (resp.get("error") or "")[:500] if isinstance(resp, dict) else ""
            raw_xml = (resp.get("raw") or "")[:2000] if isinstance(resp, dict) else ""

            new_status = "cancelled_by_unaudit" if ok else "unaudit_cancel_failed"
            reason = "金蝶反审核自动撤回成功" if ok else f"金蝶反审核但顺丰拒绝撤回: {err_msg}"

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE ads_sf_push_record
                    SET status = $1,
                        cancelled_at = CASE WHEN $2 THEN now() ELSE cancelled_at END,
                        cancel_reason = $3,
                        sf_response = COALESCE($4, sf_response),
                        error_message = $5,
                        updated_at = now()
                    WHERE id = $6
                    """,
                    new_status, ok, reason, raw_xml or None,
                    None if ok else err_msg, rec_id,
                )

            direction_cn = "出库" if is_outbound else "入库"
            if ok:
                cancelled_ok += 1
                logger.info(
                    "[unaudit] 反审核检测: %s 金蝶状态=%s，取消顺丰单，不重推",
                    bill_no, doc_status,
                )
                await _send_wecom_alert(
                    "✅ 顺丰自动撤回成功",
                    f"**金蝶反审核自动撤回**\n\n"
                    f"- 单号: `{bill_no}`\n"
                    f"- 类型: {direction_cn}（{bill_type}）\n"
                    f"- 金蝶状态: {doc_status}（已反审核）\n"
                    f"- 处理: 已通知顺丰撤单成功，不重推",
                )
            else:
                cancelled_fail += 1
                logger.error(
                    "[unaudit] ❌ 撤回失败需人工: %s %s err=%s",
                    direction_cn, bill_no, err_msg,
                )
                await _send_wecom_alert(
                    "🚨 顺丰自动撤回失败-需人工",
                    f"**金蝶已反审核，但顺丰拒绝撤回（很可能已发货/已收货）**\n\n"
                    f"- 单号: `{bill_no}`\n"
                    f"- 类型: {direction_cn}（{bill_type}）\n"
                    f"- 金蝶状态: {doc_status}（已反审核）\n"
                    f"- 顺丰回包: {err_msg or '(无 error 字段)'}\n"
                    f"- 操作建议: 联系顺丰仓核实是否已发货/已收货，决定是否人工干预",
                )

            await asyncio.sleep(0.5)  # 顺丰侧也限速

        if deletion_cancelled:
            logger.info(
                "[unaudit] 🗑️ 金蝶已删除自动撤回(%d张): %s",
                len(deletion_cancelled), ", ".join(deletion_cancelled),
            )
        logger.info(
            "[unaudit] 本轮完成：成功 %d（含删除撤回 %d），失败 %d",
            cancelled_ok, len(deletion_cancelled), cancelled_fail,
        )
        return cancelled_ok + cancelled_fail
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext('sf_unaudit_watcher'))"
            )
