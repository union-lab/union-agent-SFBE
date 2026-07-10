"""
企业微信 API 客户端（async httpx）。
文档: https://developer.work.weixin.qq.com/document/path/91039
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from typing import Any

import httpx
from Crypto.Cipher import AES

from app.config import settings

logger = logging.getLogger("union.wecom_client")

_DIRECT_BASE = "https://qyapi.weixin.qq.com/cgi-bin"

_token: str | None = None
_token_expires_at: float = 0.0

_contact_token: str | None = None
_contact_token_expires_at: float = 0.0

_approval_token: str | None = None
_approval_token_expires_at: float = 0.0

_TIMEOUT = httpx.Timeout(connect=15.0, read=30.0, write=30.0, pool=10.0)
_UPLOAD_TIMEOUT = httpx.Timeout(connect=15.0, read=120.0, write=120.0, pool=10.0)
_MAX_RETRIES = 3
_RETRY_DELAY = (1.0, 2.0, 4.0)
_PROXY_DOWN_CODES = frozenset({403, 404, 502, 503, 504})


def _api_base_candidates() -> list[tuple[str, bool]]:
    """(base_url, via_proxy) — 代理不可用时回退直连企微。"""
    proxy = (settings.wecom_proxy_url or "").strip().rstrip("/")
    out: list[tuple[str, bool]] = []
    if proxy:
        out.append((proxy, True))
    out.append((_DIRECT_BASE, False))
    return out


async def _request_with_fallback(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: httpx.Timeout | None = None,
) -> dict[str, Any]:
    candidates = _api_base_candidates()
    last_exc: Exception | None = None
    to = timeout or _TIMEOUT
    for i, (base, via_proxy) in enumerate(candidates):
        headers = _proxy_headers() if via_proxy else {}
        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        try:
            async with httpx.AsyncClient(timeout=to, headers=headers) as client:
                if method.upper() == "GET":
                    r = await client.get(url, params=params)
                else:
                    r = await client.post(url, params=params, json=json_body)
                r.raise_for_status()
                return r.json()
        except httpx.HTTPStatusError as e:
            last_exc = e
            if via_proxy and e.response.status_code in _PROXY_DOWN_CODES and i + 1 < len(candidates):
                logger.warning(
                    "企微 API 代理不可用 %s %s status=%s，切换直连",
                    method,
                    path,
                    e.response.status_code,
                )
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("企微 API 请求失败")


def _get_base() -> str:
    """优先走国内代理，未配置则直连企微 API。"""
    proxy = settings.wecom_proxy_url
    if proxy:
        return proxy.rstrip("/")
    return _DIRECT_BASE


def _proxy_headers() -> dict[str, str]:
    """代理鉴权头。直连时返回空 dict。"""
    secret = settings.wecom_proxy_secret
    if secret and settings.wecom_proxy_url:
        return {"X-Proxy-Secret": secret}
    return {}


async def _retry_request(fn, *, retries: int = _MAX_RETRIES) -> Any:
    """带指数退避的重试包装。仅重试连接超时和网络错误。"""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return await fn()
        except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout) as exc:
            last_exc = exc
            delay = _RETRY_DELAY[min(attempt, len(_RETRY_DELAY) - 1)]
            logger.warning("企微 API 请求失败 (attempt %d/%d): %s, %.1fs 后重试", attempt + 1, retries, exc, delay)
            if attempt < retries - 1:
                await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]


class WecomNotConfiguredError(RuntimeError):
    pass


class WecomApiError(RuntimeError):
    def __init__(self, msg: str, errcode: int | None = None):
        super().__init__(msg)
        self.errcode = errcode


def _require_config() -> tuple[str, str, int]:
    cid = settings.wecom_corp_id or ""
    secret = settings.wecom_corp_secret or ""
    agent = settings.wecom_agent_id or 0
    if not cid or not secret or not agent:
        raise WecomNotConfiguredError("缺少 WECOM_CORP_ID / WECOM_CORP_SECRET / WECOM_AGENT_ID")
    return cid, secret, agent


def _require_contact_config() -> tuple[str, str]:
    cid = settings.wecom_corp_id or ""
    secret = settings.wecom_contact_secret or ""
    if not cid or not secret:
        raise WecomNotConfiguredError("缺少 WECOM_CORP_ID / WECOM_CONTACT_SECRET")
    return cid, secret


async def get_access_token() -> str:
    global _token, _token_expires_at
    now = time.time()
    if _token and now < _token_expires_at - 60:
        return _token
    cid, secret, _ = _require_config()

    async def _fetch():
        return await _request_with_fallback(
            "GET",
            "gettoken",
            params={"corpid": cid, "corpsecret": secret},
        )

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"gettoken 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    _token = data["access_token"]
    _token_expires_at = now + float(data.get("expires_in", 7200))
    return _token


async def get_contact_access_token() -> str:
    """获取通讯录同步 Secret 对应的 access_token。"""
    global _contact_token, _contact_token_expires_at
    now = time.time()
    if _contact_token and now < _contact_token_expires_at - 60:
        return _contact_token
    cid, secret = _require_contact_config()

    async def _fetch():
        return await _request_with_fallback(
            "GET",
            "gettoken",
            params={"corpid": cid, "corpsecret": secret},
        )

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"gettoken(contact) 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    _contact_token = data["access_token"]
    _contact_token_expires_at = now + float(data.get("expires_in", 7200))
    return _contact_token


async def _get_with_token(path: str, token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    q = {"access_token": token}
    if params:
        q.update(params)

    async def _fetch():
        return await _request_with_fallback("GET", path, params=q)

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"{path} 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    return data


async def _post_json_with_token(path: str, token: str, body: dict[str, Any]) -> dict[str, Any]:
    async def _fetch():
        return await _request_with_fallback(
            "POST",
            path,
            params={"access_token": token},
            json_body=body,
        )

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"{path} 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    return data


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    token = await get_access_token()
    return await _get_with_token(path, token, params)


async def _post_json(path: str, body: dict[str, Any]) -> dict[str, Any]:
    token = await get_access_token()
    return await _post_json_with_token(path, token, body)


def _new_export_encoding_aeskey() -> str:
    return base64.b64encode(os.urandom(32)).decode("ascii").rstrip("=")


def _export_aes_key(encoding_aeskey: str) -> bytes:
    return base64.b64decode(encoding_aeskey + "=")


def _unpad_export_bytes(data: bytes) -> bytes:
    if not data:
        return data
    pad_len = data[-1]
    if 1 <= pad_len <= 32 and data[-pad_len:] == bytes([pad_len]) * pad_len:
        return data[:-pad_len]
    return data


def _decrypt_export_bytes(content: bytes, encoding_aeskey: str) -> bytes:
    aes_key = _export_aes_key(encoding_aeskey)
    cipher = AES.new(aes_key, AES.MODE_CBC, aes_key[:16])
    return _unpad_export_bytes(cipher.decrypt(content))


async def export_users_with_contact_secret(
    *,
    block_size: int = 1_000_000,
    timeout_sec: float = 90.0,
    poll_interval_sec: float = 2.0,
) -> dict[str, Any]:
    """
    异步导出成员详情并解密。

    仅使用 WECOM_CONTACT_SECRET，避免自建应用 Secret 拿不到 mobile 等敏感字段。
    """
    token = await get_contact_access_token()
    encoding_aeskey = _new_export_encoding_aeskey()
    submit = await _post_json_with_token(
        "export/user",
        token,
        {"encoding_aeskey": encoding_aeskey, "block_size": block_size},
    )
    jobid = str(submit.get("jobid") or submit.get("job_id") or "")
    if not jobid:
        raise WecomApiError(f"export/user 未返回 jobid: {submit}")

    deadline = time.monotonic() + timeout_sec
    result: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        result = await _get_with_token("export/get_result", token, {"jobid": jobid})
        status = int(result.get("status") or 0)
        if status == 2:
            break
        if status == 3:
            raise WecomApiError(f"export/get_result 任务失败: {result}")
        await asyncio.sleep(poll_interval_sec)
    else:
        raise WecomApiError(f"export/get_result 超时: jobid={jobid}")

    data_list = list((result or {}).get("data_list") or [])
    users: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=_UPLOAD_TIMEOUT, follow_redirects=True) as client:
        for item in data_list:
            url = item.get("url") or item.get("file_url")
            if not url:
                continue
            resp = await client.get(str(url))
            resp.raise_for_status()
            plaintext = _decrypt_export_bytes(resp.content, encoding_aeskey)
            payload = json.loads(plaintext.decode("utf-8"))
            users.extend(payload.get("userlist") or payload.get("users") or [])

    return {
        "jobid": jobid,
        "users": users,
        "file_count": len(data_list),
        "member_count": len(users),
        "with_mobile": sum(1 for u in users if u.get("mobile")),
    }


async def media_upload(
    *,
    file_content: bytes,
    filename: str,
    media_type: str = "file",
) -> str:
    """
    上传临时素材到企微，返回 media_id。
    media_type: image / voice / video / file
    临时素材有效期 3 天。
    """
    token = await get_access_token()

    async def _fetch():
        async with httpx.AsyncClient(timeout=_UPLOAD_TIMEOUT, headers=_proxy_headers()) as client:
            r = await client.post(
                f"{_get_base()}/media/upload",
                params={"access_token": token, "type": media_type},
                files={"media": (filename, file_content)},
            )
            r.raise_for_status()
            return r.json()

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"media/upload 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    return data["media_id"]


async def media_get(media_id: str) -> tuple[bytes, str]:
    """
    下载临时素材，返回 (文件内容, content_type)。
    临时素材有效期 3 天。
    """
    token = await get_access_token()

    async def _fetch():
        async with httpx.AsyncClient(timeout=_UPLOAD_TIMEOUT, headers=_proxy_headers()) as client:
            r = await client.get(
                f"{_get_base()}/media/get",
                params={"access_token": token, "media_id": media_id},
            )
            r.raise_for_status()
            ct = r.headers.get("content-type", "")
            if "json" in ct:
                data = r.json()
                raise WecomApiError(
                    f"media/get 失败: {data.get('errmsg', data)}",
                    errcode=data.get("errcode"),
                )
            return r.content, ct

    return await _retry_request(_fetch)


async def send_file_message(
    *,
    agentid: int,
    media_id: str,
    touser: str | None = None,
    toparty: str | None = None,
    totag: str | None = None,
) -> dict[str, Any]:
    """发送文件消息（msgtype=file）。需先通过 media_upload 获取 media_id。"""
    payload: dict[str, Any] = {"file": {"media_id": media_id}}
    if touser:
        payload["touser"] = touser
    if toparty:
        payload["toparty"] = toparty
    if totag:
        payload["totag"] = totag
    return await send_message(msgtype="file", agentid=agentid, payload=payload)


async def send_image_message(
    *,
    agentid: int,
    media_id: str,
    touser: str | None = None,
    toparty: str | None = None,
    totag: str | None = None,
) -> dict[str, Any]:
    """发送图片消息（msgtype=image）。需先通过 media_upload 获取 media_id。"""
    payload: dict[str, Any] = {"image": {"media_id": media_id}}
    if touser:
        payload["touser"] = touser
    if toparty:
        payload["toparty"] = toparty
    if totag:
        payload["totag"] = totag
    return await send_message(msgtype="image", agentid=agentid, payload=payload)


async def department_list() -> list[dict[str, Any]]:
    data = await _get("department/list")
    return list(data.get("department") or [])


async def user_list_department(department_id: int, fetch_child: int = 0) -> list[dict[str, Any]]:
    """获取部门成员详情（含手机号），含子部门用 fetch_child=1。"""
    data = await _get(
        "user/list",
        {"department_id": department_id, "fetch_child": fetch_child},
    )
    return list(data.get("userlist") or [])


async def send_message(
    *,
    msgtype: str,
    agentid: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    token = await get_access_token()
    body = {"agentid": agentid, "msgtype": msgtype, **payload}

    async def _fetch():
        return await _request_with_fallback(
            "POST",
            "message/send",
            params={"access_token": token},
            json_body=body,
        )

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"message/send 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    return data


def _validate_appchat_userlist(userlist: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for u in userlist:
        s = (u or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        cleaned.append(s)
    if len(cleaned) < 2:
        raise ValueError("应用群聊至少需要 2 个不同的成员 userid")
    if len(cleaned) > 2000:
        raise ValueError("应用群聊成员不可超过 2000 人")
    return cleaned


async def appchat_create(
    *,
    userlist: list[str],
    name: str | None = None,
    owner: str | None = None,
    chatid: str | None = None,
) -> dict[str, Any]:
    """
    创建应用群聊会话（appchat/create）。
    企微限制：仅自建应用；应用可见范围须为根部门；成员须在应用可见范围内。
    """
    ul = _validate_appchat_userlist(userlist)
    body: dict[str, Any] = {"userlist": ul}
    if name is not None:
        body["name"] = name
    if owner is not None:
        body["owner"] = owner.strip()
    if chatid is not None:
        body["chatid"] = chatid.strip()
    return await _post_json("appchat/create", body)


async def appchat_send(
    *,
    chatid: str,
    text: str | None = None,
    markdown: str | None = None,
    mentioned_list: list[str] | None = None,
    safe: int = 0,
) -> dict[str, Any]:
    """向应用创建的群聊发消息（appchat/send）。chatid 须为本应用创建。"""
    cid = chatid.strip()
    if not cid:
        raise ValueError("chatid 不能为空")
    if bool(text) == bool(markdown):
        raise ValueError("必须且只能提供 text 或 markdown 之一")

    if text is not None:
        payload: dict[str, Any] = {
            "chatid": cid,
            "msgtype": "text",
            "text": {"content": text},
            "safe": safe,
        }
        if mentioned_list:
            payload["text"]["mentioned_list"] = mentioned_list
        return await _post_json("appchat/send", payload)

    payload = {
        "chatid": cid,
        "msgtype": "markdown",
        "markdown": {"content": markdown},
        "safe": safe,
    }
    return await _post_json("appchat/send", payload)


async def send_textcard(
    *,
    agentid: int,
    touser: str | None = None,
    toparty: str | None = None,
    totag: str | None = None,
    title: str,
    description: str,
    url: str,
    btntxt: str = "详情",
) -> dict[str, Any]:
    """应用消息：文本卡片（textcard），适合单链接跳转。"""
    payload: dict[str, Any] = {
        "textcard": {
            "title": title,
            "description": description,
            "url": url,
            "btntxt": btntxt,
        },
    }
    if touser:
        payload["touser"] = touser
    if toparty:
        payload["toparty"] = toparty
    if totag:
        payload["totag"] = totag
    return await send_message(msgtype="textcard", agentid=agentid, payload=payload)


async def send_markdown(
    *,
    agentid: int,
    touser: str | None = None,
    toparty: str | None = None,
    totag: str | None = None,
    content: str,
) -> dict[str, Any]:
    """应用消息：markdown（支持多行、加粗、font 颜色）。"""
    payload: dict[str, Any] = {
        "markdown": {"content": content},
    }
    if touser:
        payload["touser"] = touser
    if toparty:
        payload["toparty"] = toparty
    if totag:
        payload["totag"] = totag
    return await send_message(msgtype="markdown", agentid=agentid, payload=payload)


async def send_template_card_msg(
    *,
    agentid: int,
    touser: str | None = None,
    toparty: str | None = None,
    totag: str | None = None,
    template_card: dict[str, Any],
) -> dict[str, Any]:
    """应用消息：模板卡片（template_card），如 text_notice / button_interaction 等。"""
    payload: dict[str, Any] = {"template_card": template_card}
    if touser:
        payload["touser"] = touser
    if toparty:
        payload["toparty"] = toparty
    if totag:
        payload["totag"] = totag
    return await send_message(msgtype="template_card", agentid=agentid, payload=payload)


async def update_template_card(
    *,
    userids: list[str],
    agentid: int,
    response_code: str,
    replace_text: str | None = None,
    template_card: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    更新模板卡片消息。
    - replace_text: 将按钮替换为不可点击的文案
    - template_card: 用新卡片替换旧卡片
    二者传一个即可。
    """
    body: dict[str, Any] = {
        "userids": userids,
        "agentid": agentid,
        "response_code": response_code,
    }
    if template_card:
        body["template_card"] = template_card
    elif replace_text:
        body["button"] = {"replace_name": replace_text}
    return await _post_json("message/update_template_card", body)


# ============ OA 审批 API ============
# 审批 API 使用审批应用独立 Secret（与自建应用 Secret 不同）

async def _get_approval_token() -> str:
    """获取审批应用的 access_token（独立于自建应用 token）。"""
    global _approval_token, _approval_token_expires_at
    now = time.time()
    if _approval_token and now < _approval_token_expires_at - 60:
        return _approval_token
    cid = settings.wecom_corp_id or ""
    secret = settings.wecom_approval_secret or ""
    if not cid or not secret:
        raise WecomNotConfiguredError("缺少 WECOM_CORP_ID / WECOM_APPROVAL_SECRET")

    async def _fetch():
        async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_proxy_headers()) as client:
            r = await client.get(
                f"{_get_base()}/gettoken",
                params={"corpid": cid, "corpsecret": secret},
            )
            r.raise_for_status()
            return r.json()

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"gettoken(approval) 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    _approval_token = data["access_token"]
    _approval_token_expires_at = now + float(data.get("expires_in", 7200))
    return _approval_token


async def _approval_post(path: str, body: dict[str, Any]) -> dict[str, Any]:
    """审批专用 POST，使用审批应用 token。"""
    token = await _get_approval_token()

    async def _fetch():
        async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_proxy_headers()) as client:
            r = await client.post(
                f"{_get_base()}/{path}",
                params={"access_token": token},
                json=body,
            )
            r.raise_for_status()
            return r.json()

    data = await _retry_request(_fetch)
    if data.get("errcode", 0) != 0:
        raise WecomApiError(
            f"{path} 失败: {data.get('errmsg', data)}",
            errcode=data.get("errcode"),
        )
    return data


async def oa_get_approval_info(
    start_time: int,
    end_time: int,
    *,
    template_id: list[str] | None = None,
    sp_status: int = 0,
    cursor: int = 0,
    size: int = 100,
) -> dict[str, Any]:
    """
    批量获取审批单号。
    sp_status: 0全部 1审批中 2已通过 3已驳回 4已撤销 6通过后撤销 7已删除 10已支付
    返回 {sp_no_list, next_cursor, total_count}
    """
    body: dict[str, Any] = {
        "starttime": str(start_time),
        "endtime": str(end_time),
        "cursor": cursor,
        "size": size,
        "filters": [],
    }
    if template_id:
        body["filters"].append({"key": "template_id", "value": "|".join(template_id)})
    if sp_status:
        body["filters"].append({"key": "sp_status", "value": str(sp_status)})
    return await _approval_post("oa/getapprovalinfo", body)


async def oa_get_approval_detail(sp_no: str) -> dict[str, Any]:
    """获取审批单详情。返回 {info: {sp_name, sp_status, apply_data, ...}}"""
    return await _approval_post("oa/getapprovaldetail", {"sp_no": sp_no})


async def oa_apply_event(
    *,
    creator_userid: str,
    template_id: str,
    use_template_approver: int = 1,
    apply_data: dict[str, Any],
    summary_list: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """提交审批申请。use_template_approver: 1=使用模板审批人 0=自定义"""
    body: dict[str, Any] = {
        "creator_userid": creator_userid,
        "template_id": template_id,
        "use_template_approver": use_template_approver,
        "apply_data": apply_data,
    }
    if summary_list:
        body["summary_list"] = summary_list
    return await _approval_post("oa/applyevent", body)


async def oa_get_template_detail(template_id: str) -> dict[str, Any]:
    """获取审批模板详情（控件结构）。"""
    return await _approval_post("oa/gettemplatedetail", {"template_id": template_id})


# ============ OA 日程 API ============

async def oa_schedule_add(schedule: dict[str, Any]) -> dict[str, Any]:
    return await _post_json("oa/schedule/add", {"schedule": schedule})


async def oa_schedule_update(schedule: dict[str, Any]) -> dict[str, Any]:
    return await _post_json("oa/schedule/update", {"schedule": schedule})


async def oa_schedule_get(schedule_id_list: list[str]) -> dict[str, Any]:
    return await _post_json("oa/schedule/get", {"schedule_id_list": schedule_id_list})


async def oa_schedule_del(schedule_id: str) -> dict[str, Any]:
    return await _post_json("oa/schedule/del", {"schedule_id": schedule_id})


async def oa_schedule_get_by_calendar(
    cal_id: str,
    offset: int = 0,
    limit: int = 500,
) -> dict[str, Any]:
    return await _post_json(
        "oa/schedule/get_by_calendar",
        {"cal_id": cal_id, "offset": offset, "limit": limit},
    )
