"""金蝶云星空 WebAPI 通用客户端

封装 DynamicFormService 全部 21 个接口，任何 FormId 都可调用。
会话自动管理：登录后缓存 cookies，过期自动重新登录。

接口清单（与官方 V6.0 说明书一一对应）:
  认证: login / login_kick / logout
  查询: view / query (ExecuteBillQuery)
  写入: save / batch_save / draft / submit / audit / unaudit / delete
  流转: push / allocate
  高级: group_save / flex_save / send_msg / switch_org / execute_operation
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger("union")

_SERVICE_BASE = "Kingdee.BOS.WebApi.ServicesStub"
_AUTH_BASE = f"{_SERVICE_BASE}.AuthService"
_FORM_BASE = f"{_SERVICE_BASE}.DynamicFormService"

_SESSION_TTL = 18 * 60  # 18 分钟（金蝶会话 20 分钟超时，留 2 分钟余量）
_TIMEOUT = 60
_READONLY_SERVICES = (
    f"{_FORM_BASE}.View",
    f"{_FORM_BASE}.ExecuteBillQuery",
)
_TRANSIENT_GATEWAY_STATUS = {502, 503, 504}


class KingdeeError(Exception):
    """金蝶 API 调用失败"""

    def __init__(self, message: str, errors: list[dict] | None = None):
        super().__init__(message)
        self.errors = errors or []


class KingdeeClient:
    """金蝶云星空 WebAPI 通用客户端"""

    def __init__(
        self,
        base_url: str | None = None,
        acct_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        *,
        test: bool = False,
    ):
        raw = (base_url or settings.kingdee_url or "").rstrip("/")
        self.base_url = raw if raw.endswith("/K3Cloud") else raw
        if test:
            self.acct_id = acct_id or settings.kingdee_test_acct_id or ""
        else:
            self.acct_id = acct_id or settings.kingdee_acct_id or ""
        self.username = username or settings.kingdee_username or ""
        self.password = password or settings.kingdee_password or ""
        self.is_test = test

        self._cookies: httpx.Cookies | None = None
        self._login_at: float = 0
        self._http: httpx.AsyncClient = httpx.AsyncClient(
            timeout=_TIMEOUT,
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            ),
        )

    # ─── 内部核心 ──────────────────────────────────────────

    def _url(self, service: str) -> str:
        return f"{self.base_url}/{service}.common.kdsvc"

    def _session_alive(self) -> bool:
        return self._cookies is not None and (time.time() - self._login_at) < _SESSION_TTL

    async def _ensure_login(self) -> None:
        if not self._session_alive():
            await self.login()

    async def _call(
        self,
        service: str,
        payload: dict | str,
        *,
        retry_on_session_lost: bool = True,
        retry_on_gateway_error: bool = True,
    ) -> Any:
        """通用 HTTP 调用核心，自动处理会话和重试"""
        await self._ensure_login()

        url = self._url(service)
        body = {"data": payload} if isinstance(payload, str) else payload
        t0 = time.time()

        try:
            resp = await self._http.post(
                url,
                json=body,
                cookies=self._cookies,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if (
                retry_on_gateway_error
                and service in _READONLY_SERVICES
                and status in _TRANSIENT_GATEWAY_STATUS
            ):
                logger.warning(
                    "金蝶只读接口网关异常，短暂等待后重试: service=%s status=%s",
                    service,
                    status,
                )
                await asyncio.sleep(1.5)
                return await self._call(
                    service,
                    payload,
                    retry_on_session_lost=retry_on_session_lost,
                    retry_on_gateway_error=False,
                )
            raise

        elapsed = round((time.time() - t0) * 1000)
        result = resp.json()

        if retry_on_session_lost and self._is_session_lost(result):
            logger.warning("金蝶会话丢失，重新登录后重试", extra={"service": service})
            self._cookies = None
            return await self._call(service, payload, retry_on_session_lost=False)

        logger.info(
            "金蝶 API 调用完成",
            extra={"service": service.split(".")[-1], "elapsed_ms": elapsed},
        )
        return result

    @staticmethod
    def _is_session_lost(result: Any) -> bool:
        if isinstance(result, dict):
            rs = result.get("Result", {}).get("ResponseStatus", {})
            if rs.get("MsgCode") == 1:
                return True
        return False

    @staticmethod
    def _extract_response(result: Any) -> dict:
        """从金蝶返回中提取 ResponseStatus 并检查成功"""
        if not isinstance(result, dict):
            return {"raw": result}
        rs = result.get("Result", {}).get("ResponseStatus", {})
        if not rs:
            return result
        if not rs.get("IsSuccess"):
            errors = rs.get("Errors", [])
            msg = "；".join(e.get("Message", "") for e in errors) or "金蝶未返回具体错误"
            raise KingdeeError(msg, errors)
        return {
            "success": True,
            "entities": rs.get("SuccessEntitys", []),
            "messages": rs.get("SuccessMessages", []),
        }

    # ─── 认证 ──────────────────────────────────────────────

    async def login(self) -> dict:
        """用户名密码登录（ValidateUser）"""
        url = self._url(f"{_AUTH_BASE}.ValidateUser")
        payload = {
            "acctID": self.acct_id,
            "username": self.username,
            "password": self.password,
            "lcid": 2052,
        }
        resp = await self._http.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        resp.raise_for_status()

        data = resp.json()
        login_type = data.get("LoginResultType")
        if login_type not in (1, -5):
            raise KingdeeError(f"金蝶登录失败: {data.get('Message', '未知原因')} (code={login_type})")

        self._cookies = resp.cookies
        self._login_at = time.time()
        logger.info("金蝶登录成功（用户=%s）", self.username)
        return {"success": True, "login_result_type": login_type}

    async def login_kick(self) -> dict:
        """登录并踢掉同账号的其他会话"""
        url = self._url(f"{_AUTH_BASE}.ValidateUser2")
        payload = {
            "acctID": self.acct_id,
            "username": self.username,
            "password": self.password,
            "lcid": 2052,
            "isKickOff": True,
        }
        resp = await self._http.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        resp.raise_for_status()

        data = resp.json()
        login_type = data.get("LoginResultType")
        if login_type not in (1, -5):
            raise KingdeeError(f"金蝶登录(踢人)失败: {data.get('Message', '未知原因')}")

        self._cookies = resp.cookies
        self._login_at = time.time()
        logger.info("金蝶登录(踢人模式)成功")
        return {"success": True, "login_result_type": login_type}

    async def logout(self) -> dict:
        """登出当前会话"""
        url = self._url(f"{_AUTH_BASE}.Logout")
        resp = await self._http.post(
            url,
            json={},
            cookies=self._cookies,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        self._cookies = None
        self._login_at = 0
        return {"success": True, "result": resp.json() if resp.status_code == 200 else None}

    async def aclose(self) -> None:
        """应用关闭时调用，释放连接池"""
        if self._http:
            await self._http.aclose()

    # ─── 查询 ──────────────────────────────────────────────

    async def view(
        self,
        form_id: str,
        *,
        number: str | None = None,
        id: int | None = None,
        create_org_id: int = 0,
    ) -> dict:
        """查看表单完整数据包（View）"""
        data: dict[str, Any] = {"CreateOrgId": create_org_id}
        if number:
            data["Number"] = number
        elif id is not None:
            data["Id"] = id
        else:
            raise ValueError("view 需要提供 number 或 id")

        result = await self._call(
            f"{_FORM_BASE}.View",
            {"FormId": form_id, "data": data},
        )
        return result

    async def query(
        self,
        form_id: str,
        field_keys: str,
        *,
        filter_string: str = "",
        order_string: str = "",
        top_row_count: int = 0,
        start_row: int = 0,
        limit: int = 2000,
    ) -> list:
        """表单数据查询（ExecuteBillQuery）

        返回二维数组，每行对应 field_keys 中的字段。
        """
        import json as _json

        q = _json.dumps({
            "FormId": form_id,
            "FieldKeys": field_keys,
            "FilterString": filter_string,
            "OrderString": order_string,
            "TopRowCount": top_row_count,
            "StartRow": start_row,
            "Limit": min(limit, 2000),
        })
        result = await self._call(f"{_FORM_BASE}.ExecuteBillQuery", {"data": q})
        if isinstance(result, list):
            return result
        return []

    # ─── 写入（单据生命周期）────────────────────────────────

    async def save(self, form_id: str, data: dict) -> dict:
        """保存表单数据（新增或修改）"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Save",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def batch_save(self, form_id: str, data: dict) -> dict:
        """批量保存表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.BatchSave",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def draft(self, form_id: str, data: dict) -> dict:
        """暂存表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Draft",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def submit(self, form_id: str, data: dict) -> dict:
        """提交表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Submit",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def audit(self, form_id: str, data: dict) -> dict:
        """审核表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Audit",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def unaudit(self, form_id: str, data: dict) -> dict:
        """反审核表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.UnAudit",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def delete(self, form_id: str, data: dict) -> dict:
        """删除表单数据"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Delete",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    # ─── 单据流转 ──────────────────────────────────────────

    async def push(self, form_id: str, data: dict) -> dict:
        """下推（单据流转，如采购订单→采购入库）"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Push",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def allocate(self, form_id: str, data: dict) -> dict:
        """分配（跨组织分配基础资料）"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.Allocate",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    # ─── 高级接口 ──────────────────────────────────────────

    async def group_save(self, form_id: str, data: dict) -> dict:
        """分组保存"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.GroupSave",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def flex_save(self, form_id: str, data: dict) -> dict:
        """弹性域保存"""
        data.setdefault("FormId", form_id)
        result = await self._call(
            f"{_FORM_BASE}.FlexSave",
            {"formid": form_id, "data": data},
        )
        return self._extract_response(result)

    async def send_msg(self, data: dict) -> dict:
        """发送消息"""
        import json as _json

        payload = _json.dumps(data)
        result = await self._call(f"{_FORM_BASE}.SendMsg", {"data": payload})
        return self._extract_response(result)

    async def switch_org(self, org_number: str) -> dict:
        """切换上下文默认组织"""
        result = await self._call(f"{_FORM_BASE}.SwitchOrg", {"OrgNumber": org_number})
        return self._extract_response(result)

    async def execute_operation(self, form_id: str, op_number: str, data: dict) -> dict:
        """通用操作（可执行任意已注册操作）"""
        import json as _json

        payload = _json.dumps({
            "FormId": form_id,
            "opNumber": op_number,
            **data,
        })
        result = await self._call(f"{_FORM_BASE}.ExecuteOperation", {"data": payload})
        return self._extract_response(result)


def create_kingdee_client(
    profile: str | None = None,
    *,
    test: bool = False,
    base_url: str | None = None,
    acct_id: str | None = None,
) -> KingdeeClient:
    """Create an isolated Kingdee session for a named business profile."""
    if profile == "qa_manager":
        return KingdeeClient(
            base_url=base_url,
            acct_id=acct_id,
            username=settings.kingdee_qa_manager_username or settings.kingdee_username,
            password=settings.kingdee_qa_manager_password or settings.kingdee_password,
            test=test,
        )
    return KingdeeClient(base_url=base_url, acct_id=acct_id, test=test)


kingdee_client = KingdeeClient()
kingdee_test_client = KingdeeClient(test=True)
