"""
飞书 API 异步封装。
流式方案：发送内联卡片消息 → 用 patch 逐步更新内容（比 cardkit 流式卡片更简单可靠）。
"""

import asyncio
import json
import os
import tempfile
import time
from typing import Optional

import httpx
import lark_oapi
import lark_oapi as lark
from lark_oapi.api.im.v1.model import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    PatchMessageRequest,
    PatchMessageRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)
from lark_oapi.core.http.transport import Transport, _build_url, _build_header, _merge_dicts


# ── Monkey-patch: 持久 httpx 连接池，避免每次请求重新建连 ──

_patched_client: Optional[httpx.AsyncClient] = None
_patched_original_aexecute = Transport.aexecute


@staticmethod
async def _patched_aexecute(conf, req, option=None):
    global _patched_client
    if _patched_client is None or _patched_client.is_closed:
        _patched_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
    if option is None:
        option = lark_oapi.core.model.RequestOption()

    url = _build_url(conf.domain, req.uri, req.paths)
    headers = _build_header(req, option, conf)

    json_, files, data = None, None, None
    if req.files:
        files = req.files
        if req.body is not None:
            data = json.loads(lark_oapi.core.json.JSON.marshal(req.body))
    elif req.body is not None:
        json_ = json.loads(lark_oapi.core.json.JSON.marshal(req.body))

    client = _patched_client
    response = await client.request(
        str(req.http_method.name),
        url,
        headers=req.headers,
        params=req.queries,
        json=json_,
        data=data,
        files=files,
        timeout=conf.timeout,
    )

    resp = lark_oapi.core.model.RawResponse()
    resp.status_code = response.status_code
    resp.headers = dict(response.headers)
    resp.content = response.content
    return resp


Transport.aexecute = _patched_aexecute


def _card_json(content: str, loading: bool = False) -> str:
    """
    生成卡片 JSON 字符串（Card JSON 2.0）

    飞书卡片 markdown 元素有长度限制（约 3000 字符），
    超过限制时自动分段为多个 markdown 元素。
    """
    elements = []
    if loading:
        elements.append({"tag": "markdown", "content": "⏳ 思考中..."})
    else:
        MAX_CHUNK_SIZE = 2800

        if len(content) <= MAX_CHUNK_SIZE:
            elements.append({"tag": "markdown", "content": content})
        else:
            chunks = []
            current_chunk = ""

            lines = content.split('\n')

            for line in lines:
                if len(line) > MAX_CHUNK_SIZE:
                    if current_chunk:
                        chunks.append(current_chunk)
                        current_chunk = ""

                    for i in range(0, len(line), MAX_CHUNK_SIZE):
                        chunks.append(line[i:i + MAX_CHUNK_SIZE])
                    continue

                if len(current_chunk) + len(line) + 1 > MAX_CHUNK_SIZE:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = line
                else:
                    if current_chunk:
                        current_chunk += '\n' + line
                    else:
                        current_chunk = line

            if current_chunk:
                chunks.append(current_chunk)

            for i, chunk in enumerate(chunks):
                if i > 0:
                    chunk = f"**（续 {i}）**\n\n{chunk}"
                elements.append({"tag": "markdown", "content": chunk})

    return json.dumps({
        "schema": "2.0",
        "body": {"elements": elements},
    }, ensure_ascii=False)


class FeishuClient:
    def __init__(self, client: lark.Client, app_id: str = "", app_secret: str = ""):
        self.client = client
        self._app_id = app_id
        self._app_secret = app_secret

    async def _retry_with_backoff(self, coro_func, max_retries: int = 3, initial_delay: float = 0.5):
        """
        执行异步操作，失败时指数退避重试。

        Args:
            coro_func: 返回 coroutine 的可调用对象
            max_retries: 最多重试次数（不包括首次尝试）
            initial_delay: 初始延迟秒数

        Returns:
            操作结果

        Raises:
            最后一次尝试的异常
        """
        delay = initial_delay
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                return await coro_func()
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    print(f"[retry] 第 {attempt + 1} 次失败，{delay:.1f}s 后重试: {type(e).__name__}: {e}", flush=True)
                    await asyncio.sleep(delay)
                    delay *= 2  # 指数退避
                else:
                    print(f"[retry] 已达最大重试次数 {max_retries + 1}，放弃", flush=True)

        raise last_error

    # ── 发送消息 ──────────────────────────────────────────────

    async def send_card_to_user(self, open_id: str, content: str = "", loading: bool = True) -> str:
        """向用户发送卡片消息，返回 message_id（带重试）"""
        async def _send():
            req = (
                CreateMessageRequest.builder()
                .receive_id_type("open_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(open_id)
                    .msg_type("interactive")
                    .content(_card_json(content, loading=loading))
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.acreate(req)
            if not resp.success():
                raise RuntimeError(f"发送卡片消息失败: {resp.code} {resp.msg}")
            return resp.data.message_id

        return await self._retry_with_backoff(_send, max_retries=3)

    async def reply_card(self, message_id: str, content: str = "", loading: bool = True) -> str:
        """回复用户消息（卡片形式），触发通知。返回回复消息的 message_id（带重试）"""
        async def _reply():
            req = (
                ReplyMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    ReplyMessageRequestBody.builder()
                    .msg_type("interactive")
                    .content(_card_json(content, loading=loading))
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.areply(req)
            if not resp.success():
                raise RuntimeError(f"回复卡片消息失败: {resp.code} {resp.msg}")
            return resp.data.message_id

        return await self._retry_with_backoff(_reply, max_retries=3)

    async def update_card(self, message_id: str, content: str):
        """用 patch 更新已发送的卡片内容（带重试）"""
        async def _update():
            req = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(_card_json(content, loading=False))
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.apatch(req)
            if not resp.success():
                raise RuntimeError(f"patch 卡片失败: {resp.code} {resp.msg}")

        await self._retry_with_backoff(_update, max_retries=3)

    async def update_card_fast(self, message_id: str, content: str) -> bool:
        """流式推送用：失败不重试，直接跳过等下一帧。返回是否成功"""
        try:
            req = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(_card_json(content, loading=False))
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.apatch(req)
            if not resp.success():
                return False
            return True
        except Exception:
            return False

    async def download_image(self, message_id: str, image_key: str) -> str:
        """下载飞书图片到临时文件，返回本地路径（不阻塞事件循环）"""
        return await asyncio.to_thread(
            self._download_image_sync, message_id, image_key
        )

    def _download_image_sync(self, message_id: str, image_key: str) -> str:
        """同步下载逻辑，在线程池中执行"""
        import ssl
        import urllib.request
        import uuid

        ctx = ssl.create_default_context()

        token_body = json.dumps({"app_id": self._app_id, "app_secret": self._app_secret}).encode()
        token_req = urllib.request.Request(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            data=token_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(token_req, context=ctx, timeout=10) as r:
            token = json.loads(r.read())["tenant_access_token"]

        url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/resources/{image_key}?type=image"
        img_req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        tmp_path = os.path.join(tempfile.gettempdir(), f"feishu-img-{uuid.uuid4().hex[:8]}.jpg")
        with urllib.request.urlopen(img_req, context=ctx, timeout=15) as r:
            ct = r.headers.get("Content-Type", "")
            if "png" in ct:
                tmp_path = tmp_path.replace(".jpg", ".png")
            elif "gif" in ct:
                tmp_path = tmp_path.replace(".jpg", ".gif")
            with open(tmp_path, "wb") as f:
                f.write(r.read())

        return tmp_path

    async def update_card_with_buttons(self, message_id: str, content: str, buttons: list[dict]):
        """更新卡片内容并附加操作按钮（Card JSON 2.0: 按钮直接放 elements + behaviors 回调）"""
        base = json.loads(_card_json(content))
        for i, btn in enumerate(buttons):
            base["body"]["elements"].append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": btn["text"]},
                "type": "primary",
                "size": "small",
                "name": f"btn_{i}",
                "value": btn["value"],
                "behaviors": [{"type": "callback", "value": btn["value"]}],
            })
        card_content = json.dumps(base, ensure_ascii=False)

        async def _update():
            req = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(card_content)
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.apatch(req)
            if not resp.success():
                raise RuntimeError(f"patch 卡片失败: {resp.code} {resp.msg}")

        await self._retry_with_backoff(_update, max_retries=3)

    async def reply_text(self, message_id: str, text: str) -> str:
        """回复纯文本消息（触发通知）"""
        async def _reply():
            req = (
                ReplyMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    ReplyMessageRequestBody.builder()
                    .msg_type("text")
                    .content(json.dumps({"text": text}))
                    .build()
                )
                .build()
            )
            resp = await self.client.im.v1.message.areply(req)
            if not resp.success():
                raise RuntimeError(f"回复文本消息失败: {resp.code} {resp.msg}")
            return resp.data.message_id

        return await self._retry_with_backoff(_reply, max_retries=2)

    async def send_text_to_user(self, open_id: str, text: str) -> str:
        """发送纯文本消息"""
        req = (
            CreateMessageRequest.builder()
            .receive_id_type("open_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(open_id)
                .msg_type("text")
                .content(json.dumps({"text": text}))
                .build()
            )
            .build()
        )
        resp = await self.client.im.v1.message.acreate(req)
        if not resp.success():
            raise RuntimeError(f"发送文本消息失败: {resp.code} {resp.msg}")
        return resp.data.message_id
