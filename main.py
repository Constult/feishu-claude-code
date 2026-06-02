"""
飞书 × Claude Code Bot
通过 lark-cli WebSocket 长连接接收私聊/群聊消息和卡片回调，
调用本机 claude CLI 回复，支持流式卡片输出。

启动：python main.py
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid

# 确保项目目录在 sys.path 最前面
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lark_oapi as lark

import bot_config as config
from feishu_client import FeishuClient
from session_store import SessionStore
from commands import parse_command, handle_command
from claude_runner import run_claude
from run_control import ActiveRun, ActiveRunRegistry, stop_run

# ── 看门狗：检测休眠唤醒 + 健康日志 ─────────────────────────

_start_time = time.time()
_last_event = time.time()
_wake_event = threading.Event()  # 被设置时，事件循环立即杀掉 WebSocket 连接触发重连
_reconnect_pending = False  # 任务运行期间有重连需求时置为 True，任务结束后 flush


def _watchdog():
    """后台线程：
    1. 定期打印运行状态。
    2. 检测 wall-clock 跳变（笔记本合盖休眠后再打开）。睡眠期间 wall-clock 继续走，
       但调度器不跑；如果相邻两次检查之间流逝的时间远大于 sleep 时长，说明发生了休眠。
       这时直接设置 _wake_event，让事件循环立即重连 WebSocket（原连接已经随网卡断掉）。
    3. launchd 的 KeepAlive 会在进程退出后自动拉起，所以万一卡死也有兜底。
    """
    check_interval = 30  # 每 30 秒检查一次
    last_tick = time.time()

    while True:
        time.sleep(check_interval)
        now = time.time()
        drift = now - last_tick - check_interval
        last_tick = now

        if drift > 60:
            if _has_active_runs():
                global _reconnect_pending
                _reconnect_pending = True
                print(f"[watchdog] 🌅 休眠唤醒 (drift={drift:.0f}s)，任务活跃，延迟重连", flush=True)
            else:
                print(f"[watchdog] 🌅 检测到休眠唤醒 (drift={drift:.0f}s)，强制重连 WebSocket", flush=True)
                _wake_event.set()

        uptime = now - _start_time
        idle = now - _last_event
        if idle > 300:  # 只在空闲 >5min 时打印，避免刷屏
            print(f"[watchdog] uptime={uptime/3600:.1f}h idle={idle/60:.0f}min", flush=True)


# ── 全局单例 ──────────────────────────────────────────────────

_event_loop = None  # 主 asyncio 事件循环

lark_client = lark.Client.builder() \
    .app_id(config.FEISHU_APP_ID) \
    .app_secret(config.FEISHU_APP_SECRET) \
    .log_level(lark.LogLevel.INFO) \
    .build()

feishu = FeishuClient(lark_client, app_id=config.FEISHU_APP_ID, app_secret=config.FEISHU_APP_SECRET)
store = SessionStore()
_active_runs = ActiveRunRegistry()


def _has_active_runs() -> bool:
    """判断是否还有任务在跑。失败时保守返回 True，避免误杀订阅。"""
    try:
        return _active_runs.count_active() > 0
    except Exception:
        return True


# per-chat 消息队列锁，保证同一群组的消息串行处理，允许不同群组并发处理
_chat_locks: dict[str, asyncio.Lock] = {}
_MAX_CHAT_LOCKS = 200  # 防止无界增长


# ── /stop 命令处理 ───────────────────────────────────────────

async def _announce_stopped_run(active_run: ActiveRun):
    try:
        await feishu.update_card(active_run.card_msg_id, "⏹ 已停止当前任务")
    except Exception as exc:
        print(f"[warn] update stopped card failed: {exc}", flush=True)


async def _announce_interrupted(active_run: ActiveRun):
    try:
        await feishu.update_card(active_run.card_msg_id, "⏹ 已被新消息打断")
    except Exception:
        pass


async def _handle_stop_command(sender_open_id: str, chat_id: str = "") -> str:
    active_run = _active_runs.get_run(sender_open_id, chat_id=chat_id)
    if active_run is None:
        return "当前没有正在运行的任务"
    if active_run.stop_requested:
        return "正在停止当前任务，请稍候"
    stopped = await stop_run(
        _active_runs,
        sender_open_id,
        on_stopped=_announce_stopped_run,
        chat_id=chat_id,
    )
    if not stopped:
        return "当前没有正在运行的任务"
    return "已发送停止请求"


# ── 核心消息处理（async）─────────────────────────────────────

_seen_message_ids: set = set()
_MAX_SEEN = 500
_MAX_MESSAGE_AGE = 30  # 忽略超过 30 秒前的消息

async def handle_message_from_cli(evt: dict):
    """处理从 lark-cli 收到的消息事件（NDJSON 格式）"""
    global _last_event
    _last_event = time.time()

    # 兼容 raw 和 compact 两种格式
    if "event" in evt and "message" in evt.get("event", {}):
        # raw 格式：{schema, header, event: {message: {...}, sender: {...}}}
        msg = evt["event"]["message"]
        sender = evt["event"].get("sender", {}).get("sender_id", {})
        msg_type = msg.get("message_type", "")
        chat_type = msg.get("chat_type", "")
        user_id = sender.get("open_id", "")
        chat_id = msg.get("chat_id", "")
        message_id = msg.get("message_id", "")
        content = msg.get("content", "")
        mentions = msg.get("mentions", [])
        root_id = msg.get("root_id", msg.get("parent_id", ""))
        thread_id = msg.get("thread_id", "")
    else:
        # compact 格式
        msg_type = evt.get("message_type", "")
        chat_type = evt.get("chat_type", "")
        user_id = evt.get("user_id", evt.get("sender_id", ""))
        chat_id = evt.get("chat_id", "")
        message_id = evt.get("message_id", "")
        content = evt.get("content", "")
        mentions = evt.get("mentions", [])
        root_id = evt.get("root_id", evt.get("parent_id", ""))
        thread_id = evt.get("thread_id", "")

    if not user_id or not message_id:
        return

    # 去重：忽略已处理过的消息（防止重连后重放）
    if message_id in _seen_message_ids:
        print(f"[去重] 跳过已处理消息 {message_id}", flush=True)
        return
    _seen_message_ids.add(message_id)
    if len(_seen_message_ids) > _MAX_SEEN:
        # 简单清理：丢掉一半
        to_remove = list(_seen_message_ids)[:_MAX_SEEN // 2]
        for mid in to_remove:
            _seen_message_ids.discard(mid)

    # 忽略太旧的消息（防止重连后处理积压的旧事件）
    create_time = evt.get("create_time", evt.get("timestamp", ""))
    if create_time:
        try:
            msg_ts = int(create_time)
            if msg_ts > 1e12:  # 毫秒时间戳
                msg_ts = msg_ts / 1000
            age = time.time() - msg_ts
            if age > _MAX_MESSAGE_AGE:
                print(f"[过期] 跳过 {age:.0f}s 前的消息 {message_id[:16]}...", flush=True)
                return
        except (ValueError, TypeError):
            pass

    is_group = (chat_type == "group")
    raw_oc_chat_id = chat_id  # 保存原始的 oc_ chat_id（用于 API 调用）
    if not is_group:
        chat_id = user_id

    # 话题群：用 thread_id 或 root_id 区分话题，实现一个话题一个 session
    topic_id = thread_id or root_id
    if is_group and topic_id:
        chat_id = f"{chat_id}:{topic_id}"

    print(f"[收到消息] type={msg_type} chat={chat_type}" + (f" topic={topic_id[:16]}" if topic_id else ""), flush=True)
    print(f"[Chat Info] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)

    # 解析 content（可能是 JSON 字符串）
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            content = {"text": content}

    # /stop 命令在锁外处理
    text = ""
    if msg_type == "text":
        text = content.get("text", "").strip() if isinstance(content, dict) else str(content).strip()
        if text.lower() in ("/stop", "@_user_1 /stop") or text.strip().endswith("/stop"):
            reply = await _handle_stop_command(user_id, chat_id=chat_id)
            await feishu.reply_card(message_id, content=reply, loading=False)
            return

    # 自动打断：新消息到达时，停止该 chat 的活跃任务
    active = _active_runs.get_run(user_id, chat_id=chat_id)
    if active and not active.stop_requested:
        print(f"[打断] 新消息到达，自动停止当前任务 (chat={chat_id[:8]}...)", flush=True)
        await stop_run(_active_runs, user_id, on_stopped=_announce_interrupted, chat_id=chat_id)

    # 获取 per-chat 锁
    if chat_id not in _chat_locks:
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[chat_id] = asyncio.Lock()
    lock = _chat_locks[chat_id]

    async with lock:
        try:
            await _process_message_cli(user_id, chat_id, is_group, msg_type, content, message_id, mentions, raw_oc_chat_id)
        except Exception as e:
            print(f"[error] 消息处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()


# ── 飞书文档链接解析 ───────────────────────────────────────────
_FEISHU_DOC_URL_PATTERN = re.compile(
    r'https?://[a-zA-Z0-9.-]*feishu\.cn/'
    r'(docx|wiki|docs|sheets|base|mindnotes|minutes|file)/'
    r'[a-zA-Z0-9_-]+',
    re.IGNORECASE
)


async def _resolve_feishu_doc_urls(text: str, is_group: bool = False) -> str:
    """检测文本中的飞书文档链接，通过 lark-cli（用户 token）获取内容并附在文本末尾。群聊不自动读。"""
    if is_group:
        return text
    urls = _FEISHU_DOC_URL_PATTERN.findall(text)
    if not urls:
        return text

    # 去重
    seen = set()
    unique_urls = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    lark_cli = shutil.which("lark-cli") or "lark-cli"
    doc_parts = []

    for full_url in unique_urls[:3]:
        try:
            # 妙记类型走 vc +notes，不走废弃的 docs +fetch
            if "/minutes/" in full_url:
                doc_parts.append(await _resolve_minutes_url(lark_cli, full_url))
            else:
                doc_parts.append(await _resolve_doc_url(lark_cli, full_url))
        except Exception as e:
            print(f"[文档解析] 异常 {full_url}: {e}", flush=True)

    doc_parts = [p for p in doc_parts if p]
    if doc_parts:
        return text + "\n\n---\n" + "\n---\n".join(doc_parts)
    return text


async def _resolve_minutes_url(lark_cli: str, full_url: str) -> str | None:
    """通过 vc +notes 获取妙记的文字内容（逐字稿、总结、章节）"""
    token = full_url.rstrip("/").split("/")[-1]
    try:
        proc = await asyncio.create_subprocess_exec(
            lark_cli, "vc", "+notes",
            "--minute-tokens", token,
            "--as", "user",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0 and stdout:
            data = json.loads(stdout.decode())
            notes = data.get("data", {}).get("notes", [])
            if notes and "error" not in notes[0]:
                note = notes[0]
                title = note.get("title", "")
                artifacts = note.get("artifacts", {})
                parts = []
                if title:
                    parts.append(f"[妙记《{title}》]")
                summary = artifacts.get("summary", "")
                if summary:
                    parts.append(f"【AI 总结】\n{summary}")
                chapters = artifacts.get("chapters", [])
                if chapters:
                    chapter_lines = ["【章节】"]
                    for ch in chapters:
                        ts = int(ch.get("start_ms", 0)) / 1000
                        mins, secs = int(ts // 60), int(ts % 60)
                        chapter_lines.append(f"  {mins}:{secs:02d}  {ch.get('title', '')}")
                    parts.append("\n".join(chapter_lines))
                transcript_file = artifacts.get("transcript_file", "")
                if transcript_file:
                    try:
                        # transcript_file 是相对路径，如 minutes/<token>/transcript.txt
                        local_path = os.path.join(os.path.dirname(__file__), transcript_file)
                        with open(local_path, "r", encoding="utf-8") as f:
                            transcript = f.read()
                        # 取逐字稿正文（去掉头部元信息行）
                        lines = transcript.strip().split("\n")
                        body_start = 0
                        for i, line in enumerate(lines):
                            if line.startswith("说话人"):
                                body_start = i
                                break
                        if body_start > 0:
                            parts.append("【逐字稿】\n" + "\n".join(lines[body_start:]))
                    except Exception as e:
                        print(f"[文档解析] 读取妙记逐字稿失败: {e}", flush=True)
                return "\n\n".join(parts) if parts else None
            else:
                err = notes[0].get("error", "unknown") if notes else "no notes"
                print(f"[文档解析] 获取妙记失败 {full_url}: {err}", flush=True)
        else:
            err_msg = stderr.decode()[:200] if stderr else f"exit={proc.returncode}"
            print(f"[文档解析] 获取妙记失败 {full_url}: {err_msg}", flush=True)
    except Exception as e:
        print(f"[文档解析] 妙记异常 {full_url}: {e}", flush=True)
    return None


async def _resolve_doc_url(lark_cli: str, full_url: str) -> str | None:
    """通过 docs +fetch 获取文档内容"""
    try:
        proc = await asyncio.create_subprocess_exec(
            lark_cli, "docs", "+fetch",
            "--doc", full_url,
            "--as", "user",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0 and stdout:
            data = json.loads(stdout.decode())
            raw_content = data.get("data", data)
            title = raw_content.get("title", "") if isinstance(raw_content, dict) else ""
            body = raw_content.get("content", "") if isinstance(raw_content, dict) else str(raw_content)
            if title and body:
                return f"[文档《{title}》]\n{body}"
            elif body:
                return f"[文档内容]\n{body}"
        else:
            err_msg = stderr.decode()[:200] if stderr else f"exit={proc.returncode}"
            print(f"[文档解析] 获取失败 {full_url}: {err_msg}", flush=True)
    except Exception as e:
        print(f"[文档解析] 异常 {full_url}: {e}", flush=True)
    return None


async def _process_message_cli(user_id, chat_id, is_group, msg_type, content, message_id, mentions, raw_oc_chat_id=""):
    """处理消息内容"""
    print(f"[处理消息] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)
    text = ""

    if msg_type == "text":
        text = content.get("text", "").strip() if isinstance(content, dict) else str(content).strip()
        if not text:
            return

        # 群聊去掉 @mention 占位符
        if is_group and mentions:
            for m in mentions:
                key = m.get("key", "")
                if key:
                    text = text.replace(key, "").strip()
            if not text:
                return

        print(f"[文本] {text[:50]}", flush=True)

    elif msg_type == "post":
        # 富文本消息：提取文本和图片
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except Exception:
                pass
        print(f"[post debug] type={type(content).__name__} keys={list(content.keys()) if isinstance(content, dict) else 'N/A'}", flush=True)

        parts = []
        image_keys = []
        if isinstance(content, dict):
            # compact 模式下话题群 post 可能只有 {"text": "..."}
            if "text" in content and "content" not in content and len(content) <= 2:
                parts.append(content["text"])

            # 找到 content 数组：可能直接在顶层，或在 zh_cn/en_us 下面
            body = content.get("content", None)
            if not parts and not isinstance(body, list):
                for lang_key in ("zh_cn", "en_us", "ja_jp"):
                    lang_body = content.get(lang_key, None)
                    if isinstance(lang_body, dict):
                        body = lang_body.get("content", [])
                        if isinstance(body, list) and body:
                            break

            if not parts and isinstance(body, list):
                for paragraph in body:
                    if isinstance(paragraph, list):
                        for node in paragraph:
                            if isinstance(node, dict):
                                tag = node.get("tag", "")
                                if tag == "text":
                                    parts.append(node.get("text", ""))
                                elif tag == "a":
                                    # 链接：提取文字和 URL
                                    link_text = node.get("text", "")
                                    link_href = node.get("href", "")
                                    if link_href:
                                        parts.append(f"{link_text} {link_href}" if link_text else link_href)
                                    elif link_text:
                                        parts.append(link_text)
                                elif tag == "img":
                                    ik = node.get("image_key", "")
                                    if ik:
                                        image_keys.append(ik)
                                elif tag == "media":
                                    # 文件附件
                                    parts.append(f"[文件: {node.get('file_name', node.get('file_key', ''))}]")

        text = " ".join(parts).strip()

        # 群聊去掉 @mention
        if is_group and mentions:
            for m in mentions:
                key = m.get("key", "")
                if key:
                    text = text.replace(key, "").strip()

        # 富文本中的图片
        if image_keys:
            try:
                img_path = await feishu.download_image(message_id, image_keys[0])
                img_desc = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片]"
                text = f"{text}\n{img_desc}" if text else img_desc
            except Exception as e:
                print(f"[error] 富文本图片下载失败: {e}", flush=True)

        # Fallback：如果 post 解析不到文字（比如嵌入式文档卡片），用 API 获取纯文本版本
        if not text:
            try:
                lark_cli = shutil.which("lark-cli") or "lark-cli"
                api_cid = raw_oc_chat_id or chat_id.split(":")[0]
                if "oc_" in api_cid:
                    list_args = ["--chat-id", api_cid.split(":")[0], "--as", "bot"]
                else:
                    list_args = ["--user-id", user_id, "--as", "user"]
                result = await asyncio.create_subprocess_exec(
                    lark_cli, "im", "+chat-messages-list", *list_args, "--page-size", "5",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await result.communicate()
                if stdout:
                    list_data = json.loads(stdout.decode())
                    for msg in list_data.get("data", {}).get("messages", []):
                        if msg.get("message_id") == message_id:
                            text = msg.get("content", "")
                            print(f"[post fallback] 从 API 获取文本: {text[:80]}", flush=True)
                            break
            except Exception as e:
                print(f"[post fallback] 失败: {e}", flush=True)

        if not text:
            return
        print(f"[富文本] {text[:80]}", flush=True)

    elif msg_type == "image":
        image_key = content.get("image_key", "") if isinstance(content, dict) else ""
        if not image_key:
            return
        try:
            img_path = await feishu.download_image(message_id, image_key)
            text = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片，直接回复用中文]"
        except Exception as e:
            print(f"[error] 下载图片失败: {e}", flush=True)
            if is_group:
                try:
                    await feishu.reply_card(message_id, content=f"❌ 下载图片失败：{e}", loading=False)
                except Exception:
                    pass
            else:
                await feishu.send_text_to_user(user_id, f"❌ 下载图片失败：{e}")
            return
    elif msg_type == "merge_forward":
        # 转发的聊天记录：通过 chat-messages-list 拉取完整内容
        # （messages-mget 对 merge_forward 会超时，chat-messages-list 能正确展开）
        try:
            lark_cli = shutil.which("lark-cli") or "lark-cli"
            api_chat_id = raw_oc_chat_id or chat_id.split(":")[0]
            print(f"[转发debug] raw_oc_chat_id={raw_oc_chat_id} chat_id={chat_id} api_chat_id={api_chat_id} message_id={message_id}", flush=True)

            if api_chat_id and "oc_" in api_chat_id:
                list_args = ["--chat-id", api_chat_id.split(":")[0], "--as", "bot"]
            else:
                list_args = ["--user-id", user_id, "--as", "user"]

            print(f"[转发debug] list_args={list_args}", flush=True)

            result = await asyncio.create_subprocess_exec(
                lark_cli, "im", "+chat-messages-list",
                *list_args,
                "--page-size", "5",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await result.communicate()
            fwd_content = ""

            if stderr:
                print(f"[转发debug] stderr={stderr.decode()[:200]}", flush=True)

            if stdout:
                raw_out = stdout.decode()
                try:
                    list_data = json.loads(raw_out)
                    msgs = list_data.get("data", {}).get("messages", [])
                    print(f"[转发debug] 找到 {len(msgs)} 条消息", flush=True)
                    for msg in msgs:
                        mid = msg.get("message_id", "")
                        mt = msg.get("msg_type", "")
                        print(f"[转发debug]   msg: id={mid[:20]} type={mt}", flush=True)
                        if mid == message_id:
                            fwd_content = msg.get("content", "")
                            print(f"[转发debug] 匹配！content长度={len(fwd_content)}", flush=True)
                            break
                except Exception as e:
                    print(f"[转发debug] JSON解析失败: {e}, raw={raw_out[:200]}", flush=True)
            else:
                print(f"[转发debug] stdout为空", flush=True)

            if not fwd_content or "Merged and Forwarded" in fwd_content:
                if isinstance(content, dict):
                    fwd_content = json.dumps(content, ensure_ascii=False)
                elif isinstance(content, str):
                    fwd_content = content

            text = f"[用户转发了一段聊天记录，内容如下：]\n{fwd_content}"
            print(f"[转发] {text[:200]}", flush=True)
        except Exception as e:
            print(f"[error] 处理转发消息失败: {e}", flush=True)
            text = f"[用户转发了一段聊天记录，但无法读取内容: {e}]"

    elif msg_type == "media":
        # 文件/文档附件：提取 file_key 并尝试下载/导出
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except Exception:
                pass
        file_key = content.get("file_key", "") if isinstance(content, dict) else ""
        file_name = content.get("file_name", "") if isinstance(content, dict) else ""
        image_key = content.get("image_key", "") if isinstance(content, dict) else ""

        if image_key:
            # 图片文件
            try:
                img_path = await feishu.download_image(message_id, image_key)
                text = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片，直接回复用中文]"
            except Exception as e:
                print(f"[error] 媒体图片下载失败: {e}", flush=True)
                text = f"[用户发送了一张图片但下载失败: {e}]"
        elif file_key and file_name:
            # 尝试通过 lark-cli 导出文档内容
            print(f"[媒体] file_key={file_key} file_name={file_name}", flush=True)
            lark_cli = shutil.which("lark-cli") or "lark-cli"
            doc_content = ""
            try:
                # 先尝试用 docs +fetch（传 token 即可，无需拼接完整 URL）
                proc = await asyncio.create_subprocess_exec(
                    lark_cli, "docs", "+fetch",
                    "--doc", file_key,
                    "--as", "user",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await proc.communicate()
                if proc.returncode == 0 and stdout:
                    data = json.loads(stdout.decode())
                    raw_content = data.get("data", data)
                    title = raw_content.get("title", "") if isinstance(raw_content, dict) else ""
                    body = raw_content.get("content", "") if isinstance(raw_content, dict) else str(raw_content)
                    doc_content = body
                    if title:
                        text = f"[用户分享了文档《{title}》，以下是文档内容：]\n{doc_content}"
                    else:
                        text = f"[用户分享了文档，以下是文档内容：]\n{doc_content}"
            except Exception as e:
                print(f"[媒体] docs fetch 失败: {e}", flush=True)

            if not doc_content:
                # 尝试用 drive export
                try:
                    proc = await asyncio.create_subprocess_exec(
                        lark_cli, "drive", "+export",
                        "--token", file_key,
                        "--doc-type", "docx",
                        "--file-extension", "markdown",
                        "--as", "user",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    stdout, _ = await proc.communicate()
                    if proc.returncode == 0:
                        # export 返回文件路径信息
                        text = f"[用户分享了文件「{file_name}」，请使用 lark-cli drive +export --token {file_key} --doc-type docx --file-extension markdown 导出后读取]"
                    else:
                        text = f"[用户分享了文件「{file_name}」(file_key={file_key})，但无法自动获取内容]"
                except Exception as e:
                    print(f"[媒体] drive export 失败: {e}", flush=True)
                    text = f"[用户分享了文件「{file_name}」(file_key={file_key})]"
        elif file_key:
            text = f"[用户分享了一个文件 (file_key={file_key})]"
        else:
            print(f"[跳过] media 消息无可用 file_key/image_key", flush=True)
            return

    elif msg_type in ("file", "audio"):
        # 文件消息（PDF、Word 等）和语音消息：下载到本地后交给 Claude
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except Exception:
                pass
        file_key = content.get("file_key", "") if isinstance(content, dict) else ""
        file_name = content.get("file_name", "") if isinstance(content, dict) else ""
        duration = content.get("duration", 0) if isinstance(content, dict) else 0

        if not file_key:
            print(f"[跳过] {msg_type} 消息无 file_key", flush=True)
            return

        label = "语音" if msg_type == "audio" else "文件"
        print(f"[{label}] file_key={file_key} file_name={file_name}" + (f" duration={duration}s" if duration else ""), flush=True)

        # 用 im +messages-resources-download 下载（output 需相对路径）
        ext = os.path.splitext(file_name)[1] or (".ogg" if msg_type == "audio" else ".bin")
        safe_name = f"feishu-{msg_type}-{uuid.uuid4().hex[:8]}{ext}"
        download_to = os.path.join(tempfile.gettempdir(), safe_name)

        try:
            lark_cli = shutil.which("lark-cli") or "lark-cli"
            args = [
                lark_cli, "im", "+messages-resources-download",
                "--message-id", message_id,
                "--file-key", file_key,
                "--type", "file",
                "--output", safe_name,
                "--as", "user",
            ]
            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd=tempfile.gettempdir(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()

            # 检查是否实际下载成功
            actual_path = download_to
            if proc.returncode == 0:
                # 可能文件名被修改（server Content-Disposition），尝试找到刚下载的文件
                out_text = stdout.decode() if stdout else ""
                if os.path.exists(download_to):
                    pass  # 用我们预期的路径
                else:
                    # 可能在当前目录下有不同的扩展名
                    import glob as _glob
                    candidates = _glob.glob(os.path.join(tempfile.gettempdir(), f"feishu-{msg_type}-*"))
                    if candidates:
                        # 取最新的
                        actual_path = max(candidates, key=os.path.getmtime)

            if os.path.exists(actual_path):
                size_kb = os.path.getsize(actual_path) / 1024
                print(f"[{label}] 下载成功 → {actual_path} ({size_kb:.0f}KB)", flush=True)
                if msg_type == "audio":
                    text = f"[用户发来一段语音（时长 {duration} 秒），已下载到：{actual_path}。当前无法直接转写语音，请告知用户。]"
                else:
                    text = f"[用户发送了文件「{file_name}」，已下载到：{actual_path}，请读取并分析这个文件]"
            else:
                err = stderr.decode()[:300] if stderr else f"exit={proc.returncode}"
                print(f"[{label}] 下载后找不到文件: {err}", flush=True)
                text = f"[用户发送了文件「{file_name}」(file_key={file_key})，但下载后找不到文件：{err}]"
        except Exception as e:
            print(f"[{label}] 下载异常: {e}", flush=True)
            text = f"[用户发送了文件「{file_name}」(file_key={file_key})，但下载异常：{e}]"

    else:
        print(f"[跳过] 不支持的消息类型: {msg_type}", flush=True)
        return

    # ── 飞书文档链接预解析 → 用 lark-cli 取内容嵌入上下文 ─────
    if text:
        text = await _resolve_feishu_doc_urls(text, is_group=is_group)

    # ── 斜杠命令 ──────────────────────────────────────────────
    parsed = parse_command(text)
    if parsed:
        cmd, args = parsed
        print(f"[cmd] 执行命令 {cmd}", flush=True)
        reply = await handle_command(cmd, args, user_id, chat_id, store)
        print(f"[cmd] 命令返回 type={type(reply).__name__}", flush=True)
        if reply is not None:
            if isinstance(reply, dict):
                reply_text, reply_buttons = reply["text"], reply.get("buttons", [])
            else:
                reply_text, reply_buttons = reply, []

            if cmd == "resume" and not args:
                await feishu.reply_card(message_id, content=reply_text, loading=False)
            elif reply_buttons:
                card_id = await feishu.reply_card(message_id, content=reply_text, loading=False)
                print(f"[按钮] 卡片已发送 card_id={card_id}, 准备添加 {len(reply_buttons)} 个按钮", flush=True)
                try:
                    await feishu.update_card_with_buttons(card_id, reply_text, reply_buttons)
                    print(f"[按钮] 按钮添加成功", flush=True)
                except Exception as btn_err:
                    print(f"[按钮] 按钮添加失败: {btn_err}", flush=True)
            else:
                await feishu.reply_card(message_id, content=reply_text, loading=False)
            return

    # ── 普通消息 → 先 reaction 再调用 Claude ──────────────────
    # 第一反应：根据用户说的话，本能地回一个表情
    try:
        instinct = _pick_instinct_reaction(text)
        await _add_reaction(message_id, instinct)
    except Exception:
        pass

    session = await store.get_current(user_id, chat_id)
    print(f"[Claude] session={session.session_id} model={session.model}", flush=True)

    try:
        card_msg_id = await feishu.reply_card(message_id, loading=True)
        print(f"[卡片] card_msg_id={card_msg_id}", flush=True)
    except Exception as e:
        print(f"[error] 发送占位卡片失败: {e}", flush=True)
        try:
            await feishu.reply_card(message_id, content=f"❌ 发送消息失败：{e}", loading=False)
        except Exception:
            pass
        return

    await _run_and_display(user_id, chat_id, is_group, text, card_msg_id, session, message_id, raw_oc_chat_id)


# ── 卡片按钮回调处理 ─────────────────────────────────────────

_handled_comment_ids: set = set()

async def handle_doc_comment_from_cli(evt: dict):
    """处理文档评论事件：有人在文档评论里 @了 bot，自动回复评论"""
    global _last_event
    _last_event = time.time()

    # 从 raw 格式提取信息
    event = evt.get("event", evt)
    comment_id = event.get("comment_id", "")
    reply_id = event.get("reply_id", "")
    is_mentioned = event.get("is_mentioned", False)

    # file_token 等字段藏在 notice_meta 里
    notice_meta = event.get("notice_meta", {})
    file_token = notice_meta.get("file_token", event.get("file_token", ""))
    file_type = notice_meta.get("file_type", event.get("file_type", ""))
    from_user = notice_meta.get("from_user_id", {})
    user_id = from_user.get("open_id", "") if isinstance(from_user, dict) else ""

    print(f"[文档评论] file={file_token[:12]}... comment={comment_id} user={user_id[:8] if user_id else 'N/A'}", flush=True)
    print(f"[文档评论] raw keys: {list(event.keys())}", flush=True)
    print(f"[文档评论] raw event: {json.dumps(evt, ensure_ascii=False)[:500]}", flush=True)

    if not file_token or not comment_id:
        print(f"[文档评论] 缺少 file_token 或 comment_id，跳过", flush=True)
        return

    # 去重：同一个评论只处理一次（飞书可能推送多次）
    dedup_key = f"{file_token}:{comment_id}"
    if dedup_key in _handled_comment_ids:
        print(f"[文档评论] 重复事件，跳过 {dedup_key}", flush=True)
        return
    _handled_comment_ids.add(dedup_key)
    if len(_handled_comment_ids) > 200:
        _handled_comment_ids.clear()

    try:
        lark_cli = shutil.which("lark-cli") or "lark-cli"

        # 1. 获取评论内容（包括划词内容）
        proc = await asyncio.create_subprocess_exec(
            lark_cli, "drive", "file.comments", "list",
            "--params", json.dumps({"file_token": file_token, "file_type": file_type or "docx"}),
            "--as", "user",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        comments_data = json.loads(stdout.decode()) if stdout else {}

        # 找到具体的评论
        target_comment = None
        all_comments = comments_data.get("data", {}).get("items", [])
        for c in all_comments:
            if c.get("comment_id") == comment_id:
                target_comment = c
                break

        if not target_comment:
            print(f"[文档评论] 未找到评论 {comment_id}", flush=True)
            return

        # 提取划词内容和评论文本
        quote = target_comment.get("quote", "")
        reply_list = target_comment.get("reply_list", {}).get("replies", [])
        comment_text = ""
        for reply in reply_list:
            elements = reply.get("content", {}).get("elements", [])
            for elem in elements:
                if elem.get("type") == "text_run":
                    comment_text += elem.get("text", "")

        prompt = ""
        if quote:
            prompt += f"[文档中划词的内容：]{quote}\n"
        prompt += f"[用户在文档评论中说：]{comment_text}\n"
        prompt += "请根据划词内容和评论回复用户，回复会显示在文档评论区。简洁回复，不要太长。"

        print(f"[文档评论] quote={quote[:50]}... text={comment_text[:50]}...", flush=True)

        # 2. 调用 Claude 生成回复
        from claude_runner import run_claude
        full_text, _, _ = await run_claude(
            message=prompt,
            session_id=None,
            model=config.DEFAULT_MODEL,
            cwd=config.DEFAULT_CWD,
            permission_mode="bypassPermissions",
        )

        if not full_text:
            full_text = "（无法生成回复）"

        # 3. 在评论下面回复
        reply_content = json.dumps([{"type": "text", "text": full_text[:800]}])
        proc = await asyncio.create_subprocess_exec(
            lark_cli, "drive", "file.comment.replys", "create",
            "--params", json.dumps({
                "file_token": file_token,
                "comment_id": comment_id,
                "file_type": file_type or "docx",
            }),
            "--data", json.dumps({
                "content": {"elements": [{"type": "text_run", "text": full_text[:800]}]},
            }),
            "--as", "user",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        print(f"[文档评论] 已回复评论 {comment_id}", flush=True)

        # 4. 启动后台轮询：监听这条评论的后续回复（无需 @）
        _safe_create_task(_poll_comment_replies(
            lark_cli, file_token, file_type or "docx", comment_id, user_id
        ))

    except Exception as e:
        print(f"[文档评论] 处理失败: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)


_active_comment_polls: set = set()  # 正在轮询的评论 ID

async def _poll_comment_replies(lark_cli, file_token, file_type, comment_id, user_id):
    """轮询评论线程的后续回复，持续 10 分钟，每 15 秒查一次"""
    if comment_id in _active_comment_polls:
        return
    _active_comment_polls.add(comment_id)

    bot_app_id = config.FEISHU_APP_ID
    known_reply_ids = set()
    # 先记录当前已有的回复
    try:
        proc = await asyncio.create_subprocess_exec(
            lark_cli, "drive", "file.comments", "list",
            "--params", json.dumps({"file_token": file_token, "file_type": file_type}),
            "--as", "user",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        data = json.loads(stdout.decode()) if stdout else {}
        for c in data.get("data", {}).get("items", []):
            if c.get("comment_id") == comment_id:
                for r in c.get("reply_list", {}).get("replies", []):
                    known_reply_ids.add(r.get("reply_id", ""))
                break
    except Exception:
        pass

    print(f"[评论轮询] 开始监听 comment={comment_id}，已有 {len(known_reply_ids)} 条回复", flush=True)

    try:
        for _ in range(40):  # 40 × 15s = 10 分钟
            await asyncio.sleep(15)

            try:
                proc = await asyncio.create_subprocess_exec(
                    lark_cli, "drive", "file.comments", "list",
                    "--params", json.dumps({"file_token": file_token, "file_type": file_type}),
                    "--as", "user",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await proc.communicate()
                data = json.loads(stdout.decode()) if stdout else {}
            except Exception:
                continue

            target = None
            for c in data.get("data", {}).get("items", []):
                if c.get("comment_id") == comment_id:
                    target = c
                    break
            if not target:
                continue

            replies = target.get("reply_list", {}).get("replies", [])
            for reply in replies:
                rid = reply.get("reply_id", "")
                if rid in known_reply_ids:
                    continue
                known_reply_ids.add(rid)

                # 跳过 bot 自己发的回复
                sender = reply.get("user_id", "")
                if sender == bot_app_id or not sender:
                    continue

                # 提取回复文本
                elements = reply.get("content", {}).get("elements", [])
                reply_text = ""
                for elem in elements:
                    if elem.get("type") == "text_run":
                        reply_text += elem.get("text", "")
                if not reply_text:
                    continue

                print(f"[评论轮询] 新回复: {reply_text[:50]}...", flush=True)

                # 获取划词内容
                quote = target.get("quote", "")

                # 构建 prompt
                # 收集整个对话历史
                history = []
                for r in replies:
                    r_elements = r.get("content", {}).get("elements", [])
                    r_text = "".join(e.get("text", "") for e in r_elements if e.get("type") == "text_run")
                    r_sender = r.get("user_id", "")
                    role = "Bot" if r_sender == bot_app_id else "用户"
                    if r_text:
                        history.append(f"{role}: {r_text}")

                prompt = ""
                if quote:
                    prompt += f"[文档中划词的内容：]{quote}\n"
                prompt += f"[评论区对话历史：]\n" + "\n".join(history)
                prompt += f"\n\n用户刚回复了：{reply_text}\n请继续对话，简洁回复。"

                # 调用 Claude
                from claude_runner import run_claude
                try:
                    full_text, _, _ = await run_claude(
                        message=prompt,
                        session_id=None,
                        model=config.DEFAULT_MODEL,
                        cwd=config.DEFAULT_CWD,
                        permission_mode="bypassPermissions",
                    )
                except Exception as e:
                    print(f"[评论轮询] Claude 调用失败: {e}", flush=True)
                    continue

                if not full_text:
                    continue

                # 回复
                try:
                    proc = await asyncio.create_subprocess_exec(
                        lark_cli, "drive", "file.comment.replys", "create",
                        "--params", json.dumps({
                            "file_token": file_token,
                            "comment_id": comment_id,
                            "file_type": file_type,
                        }),
                        "--data", json.dumps({
                            "content": {"elements": [{"type": "text_run", "text": full_text[:800]}]},
                        }),
                        "--as", "user",
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc.wait()
                    # 记录自己的回复 ID 以避免重复
                    print(f"[评论轮询] 已回复", flush=True)
                except Exception as e:
                    print(f"[评论轮询] 回复失败: {e}", flush=True)
    finally:
        _active_comment_polls.discard(comment_id)
        print(f"[评论轮询] 结束监听 comment={comment_id}", flush=True)


async def handle_card_action_from_cli(evt: dict):
    """处理从 lark-cli 收到的卡片回调事件"""
    global _last_event
    _last_event = time.time()

    user_id = evt.get("operator_id", evt.get("user_id", ""))
    action = evt.get("action", {})
    value = action.get("value", {})
    msg_id = evt.get("open_message_id", "")

    if not user_id:
        return

    action_type = value.get("action", "")
    chat_id = value.get("cid", user_id)

    print(f"[卡片回调] user={user_id[:8]}... action={action_type or 'reply'}", flush=True)

    if action_type == "set_mode":
        mode = value.get("mode", "")
        if mode:
            await _handle_set_mode(user_id, chat_id, mode, msg_id)
    else:
        reply_text = value.get("reply", "")
        if reply_text:
            print(f"[按钮] user={user_id[:8]}... reply={reply_text}", flush=True)
            await _handle_button_reply(user_id, chat_id, reply_text, msg_id)


# ── Claude 运行与展示 ────────────────────────────────────────

async def _run_and_display(
    user_id: str, chat_id: str, is_group: bool,
    text: str, card_msg_id: str, session, notify_msg_id: str,
    raw_oc_chat_id: str = "",
):
    """调用 Claude 并流式展示结果。超过阈值自动切换为文档输出。支持 [[SEND_FILE]] 文件回传。"""
    active_run = _active_runs.start_run(user_id, card_msg_id, chat_id=chat_id)

    accumulated = ""
    last_pushed_len = 0
    tool_history: list[str] = []
    ask_options: list[tuple[str, str]] = []
    plan_exited = False
    last_push_time = 0.0
    push_failures = 0
    switched_to_doc = False
    _DOC_SWITCH_THRESHOLD = 30    # 超过此字数自动切文档（长文直接出飞书文档，比流式卡片快）
    _MAX_STREAM_DISPLAY = 2500

    async def push(content: str):
        nonlocal push_failures
        if push_failures >= 3:
            return
        ok = await feishu.update_card_fast(card_msg_id, content)
        if ok:
            push_failures = 0
        else:
            push_failures += 1

    def _build_display() -> str:
        parts = []
        if tool_history:
            parts.append("\n".join(tool_history[-5:]))
        if accumulated and not switched_to_doc:
            if parts:
                parts.append("")
            d = accumulated
            if len(d) > _MAX_STREAM_DISPLAY:
                d = "...\n\n" + d[-_MAX_STREAM_DISPLAY:]
            parts.append(d)
        return "\n".join(parts) if parts else "⏳ 思考中..."

    async def on_tool_use(name: str, inp: dict):
        nonlocal accumulated, last_push_time, plan_exited
        if name.lower() == "exitplanmode":
            plan_exited = True
            return
        if name.lower() == "enterplanmode":
            if session.permission_mode != "plan":
                print(f"[Plan] EnterPlanMode 检测到，切换为 plan", flush=True)
                await store.set_permission_mode(user_id, chat_id, "plan")
            return
        if name.lower() == "enterworktree" and inp:
            wt_name = inp.get("name", "")
            if wt_name:
                print(f"[Worktree] 进入 worktree: {wt_name}", flush=True)
            return
        if name.lower() == "exitworktree":
            print(f"[Worktree] 退出 worktree", flush=True)
            return
        if name.lower() == "askuserquestion":
            question = inp.get("question", inp.get("text", ""))
            if question:
                accumulated += f"\n\n❓ **等待回复：**\n{question}"
                detected = _extract_options(question)
                if detected:
                    ask_options.clear()
                    ask_options.extend(detected)
                await push(_build_display())
                return
        tool_line = _format_tool(name, inp)
        if inp and tool_history:
            tool_history[-1] = tool_line
        else:
            tool_history.append(tool_line)
        if switched_to_doc:
            await push(f"📄 正在生成文档...\n{tool_line}")
        else:
            await push(_build_display())
        last_push_time = time.time()

    async def on_text_chunk(chunk: str):
        nonlocal accumulated, last_push_time, last_pushed_len, switched_to_doc
        accumulated += chunk
        now = time.time()
        elapsed = now - last_push_time

        if not switched_to_doc and len(accumulated) >= _DOC_SWITCH_THRESHOLD:
            switched_to_doc = True
            await push(f"📄 内容较长（{len(accumulated)} 字），正在生成文档...")
            last_push_time = now
            last_pushed_len = len(accumulated)
            return

        if switched_to_doc:
            if elapsed >= 3.0:
                await push(f"📄 正在生成文档...（已 {len(accumulated)} 字）")
                last_push_time = now
            return

        new_chars = len(accumulated) - last_pushed_len
        # 预文档阶段（<30字）：0.5s 快推，保证流式体验不卡顿
        if elapsed >= 0.5 and new_chars > 0:
            await push(_build_display())
            last_push_time = now
            last_pushed_len = len(accumulated)

    try:
        print(f"[run_claude] 开始调用...", flush=True)
        full_text, new_session_id, used_fresh_session_fallback = await run_claude(
            message=text,
            session_id=session.session_id,
            model=session.model,
            cwd=session.cwd,
            permission_mode=session.permission_mode,
            on_text_chunk=on_text_chunk,
            on_tool_use=on_tool_use,
            on_process_start=lambda proc: _active_runs.attach_process(user_id, proc, chat_id=chat_id),
        )
        print(f"[run_claude] 完成, session={new_session_id}", flush=True)
    except Exception as e:
        if active_run.stop_requested:
            return
        print(f"[error] Claude 运行失败: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        try:
            await feishu.update_card(card_msg_id, f"❌ Claude 执行出错：{type(e).__name__}: {e}")
        except Exception:
            pass
        return
    finally:
        _active_runs.clear_run(user_id, active_run, chat_id=chat_id)
        # 任务结束后，如果有待处理的重连需求则触发
        global _reconnect_pending
        if _reconnect_pending and not _has_active_runs():
            _reconnect_pending = False
            print("[reconnect] 任务结束，执行延迟重连", flush=True)
            _wake_event.set()

    final = full_text or accumulated or "（无输出）"
    if used_fresh_session_fallback:
        final = "⚠️ 检测到工作目录已变化，旧会话无法继续。本次已自动切换到新 session。\n\n" + final

    # ── [[SEND_FILE]] 文件回传 ───────────────────────────────
    _SEND_FILE_RE = re.compile(r'\[\[SEND_FILE:(.+?)\]\]')
    send_matches = list(_SEND_FILE_RE.finditer(final))
    if send_matches:
        lark_cli = shutil.which("lark-cli") or "lark-cli"
        for m in reversed(send_matches):
            filepath = m.group(1).strip()
            full_path = os.path.expanduser(filepath)
            if not os.path.isabs(full_path):
                full_path = os.path.abspath(full_path)
            if not os.path.exists(full_path):
                final = final[:m.start()] + f"❌ 文件不存在：{filepath}" + final[m.end():]
                continue
            try:
                proc = await asyncio.create_subprocess_exec(
                    lark_cli, "drive", "+upload",
                    "--file", full_path,
                    "--as", "user",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    result = json.loads(stdout.decode())
                    file_data = result.get("data", result)
                    file_url = file_data.get("url", "") or file_data.get("share_link", "")
                    file_name = os.path.basename(full_path)
                    if file_url:
                        final = final[:m.start()] + f"📎 [{file_name}]({file_url})" + final[m.end():]
                    else:
                        final = final[:m.start()] + f"✅ {file_name} 已上传" + final[m.end():]
                else:
                    err = stderr.decode()[:100] if stderr else f"exit={proc.returncode}"
                    final = final[:m.start()] + f"❌ 上传失败：{err}" + final[m.end():]
            except Exception as e:
                final = final[:m.start()] + f"❌ 上传异常：{e}" + final[m.end():]

    # ── 文档模式：内容较长，自动写入飞书文档 ────────────────
    if switched_to_doc and final and len(final) > _DOC_SWITCH_THRESHOLD:
        doc_url = None
        try:
            title_src = text.strip().split("\n")[0][:50]
            lark_cli = shutil.which("lark-cli") or "lark-cli"

            # Step 1: 创建文档壳
            proc = await asyncio.create_subprocess_exec(
                lark_cli, "docs", "+create",
                "--api-version", "v2",
                "--content", f"# {title_src}",
                "--doc-format", "markdown",
                "--as", "user",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                print(f"[doc] 创建壳失败: {stderr.decode()[:300]}", flush=True)
                raise RuntimeError("create shell failed")

            result = json.loads(stdout.decode())
            doc_url = result.get("data", {}).get("document", {}).get("url", "")
            doc_token = result.get("data", {}).get("document", {}).get("document_id", "")
            if not doc_token:
                raise RuntimeError("no document_id in response")

            # Step 2: 写入内容（临时文件，避免 shell 截断特殊字符）
            tmp_name = f"_feishu_doc_{uuid.uuid4().hex[:8]}.md"
            tmp_path = os.path.join(os.getcwd(), tmp_name)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(final)
            try:
                proc = await asyncio.create_subprocess_exec(
                    lark_cli, "docs", "+update",
                    "--api-version", "v2",
                    "--command", "overwrite",
                    "--doc-format", "markdown",
                    "--doc", doc_token,
                    "--content", f"@{tmp_name}",
                    "--as", "user",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate()
                if proc.returncode != 0:
                    print(f"[doc] 写入内容失败: {stderr.decode()[:300]}", flush=True)
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            print(f"[doc] 文档已生成: {doc_url}", flush=True)
        except Exception as e:
            doc_url = None
            print(f"[doc] 创建异常: {type(e).__name__}: {e}", flush=True)

        if doc_url:
            summary = f"📄 文档已生成：\n\n[点击查看]({doc_url})\n\n（共 {len(final)} 字符）"
            try:
                await feishu.update_card(card_msg_id, summary)
            except Exception:
                pass
            if new_session_id:
                await store.on_claude_response(user_id, chat_id, new_session_id, text)
            return
        print(f"[doc] 回退为卡片显示", flush=True)

    # ── 正常卡片显示（短回复 / doc 创建失败回退）───────────
    options = _extract_options(final) or ask_options
    try:
        if options:
            buttons = [
                {"text": display, "value": {"reply": value, "cid": chat_id}}
                for display, value in options
            ]
            await feishu.update_card_with_buttons(card_msg_id, final, buttons)
        else:
            await feishu.update_card(card_msg_id, final)
    except Exception as e:
        print(f"[error] 卡片更新失败，回退发文本: {e}", flush=True)
        try:
            if is_group and notify_msg_id:
                await feishu.reply_card(notify_msg_id, content=final, loading=False)
            else:
                await feishu.send_text_to_user(user_id, final)
        except Exception as fallback_err:
            print(f"[error] 文本回退也失败: {fallback_err}", flush=True)

    if new_session_id:
        await store.on_claude_response(user_id, chat_id, new_session_id, text)

    if plan_exited and (await store.get_current(user_id, chat_id)).permission_mode == "plan":
        print(f"[Plan] ExitPlanMode 检测到，切换为 bypassPermissions", flush=True)
        await store.set_permission_mode(user_id, chat_id, "bypassPermissions")
        try:
            notice = "🚀 已退出规划模式，发送任意消息开始执行。"
            if is_group and notify_msg_id:
                await feishu.reply_text(notify_msg_id, notice)
            else:
                await feishu.send_text_to_user(user_id, notice)
        except Exception:
            pass


# ── 模式切换与按钮回复 ───────────────────────────────────────

async def _handle_set_mode(user_id: str, chat_id: str, mode: str, card_msg_id: str):
    from commands import VALID_MODES
    await store.set_permission_mode(user_id, chat_id, mode)
    desc = VALID_MODES.get(mode, "")
    print(f"[模式切换] user={user_id[:8]}... mode={mode}", flush=True)
    if card_msg_id:
        try:
            await feishu.update_card(card_msg_id, f"✅ 已切换为 **{mode}**\n{desc}")
        except Exception:
            pass


async def _handle_button_reply(user_id: str, chat_id: str, text: str, clicked_msg_id: str):
    is_group = (chat_id != user_id)

    active = _active_runs.get_run(user_id, chat_id=chat_id)
    if active and not active.stop_requested:
        await stop_run(_active_runs, user_id, on_stopped=_announce_interrupted, chat_id=chat_id)

    if chat_id not in _chat_locks:
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[chat_id] = asyncio.Lock()
    lock = _chat_locks[chat_id]

    async with lock:
        try:
            session = await store.get_current(user_id, chat_id)
            try:
                if clicked_msg_id:
                    card_msg_id = await feishu.reply_card(clicked_msg_id, loading=True)
                else:
                    # 没有 clicked_msg_id 时 fallback 到发新消息
                    card_msg_id = await feishu.reply_card(clicked_msg_id or user_id, loading=True)
            except Exception as e:
                print(f"[error] 按钮回复占位卡片失败: {e}", flush=True)
                return
            await _run_and_display(
                user_id, chat_id, is_group, text,
                card_msg_id, session, clicked_msg_id or "",
            )
        except Exception as e:
            print(f"[error] 按钮回复处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)


# ── 工具函数 ─────────────────────────────────────────────────

def _pick_instinct_reaction(user_text: str) -> str:
    """听到用户说的话后的第一反应——本能的、情绪化的，像人一样"""
    import random
    t = user_text.lower()

    # ── 用户在抱怨/骂 ──
    if any(k in t for k in ("什么鬼", "wtf", "垃圾", "坑", "又挂了", "又出问题")):
        return random.choice(["SPITBLOOD", "FACEPALM", "ColdSweat", "TERROR"])
    if any(k in t for k in ("bug", "报错", "挂了", "崩了", "不work", "坏了", "出问题")):
        return random.choice(["SHOCKED", "EYES", "PETRIFIED", "ColdSweat"])
    if any(k in t for k in ("为什么", "怎么回事", "啥情况", "why")):
        return random.choice(["THINKING", "EYES", "SMART", "GLANCE"])

    # ── 用户在求助 ──
    if any(k in t for k in ("帮我", "help", "救命", "搞不定", "怎么办")):
        return random.choice(["SALUTE", "MUSCLE", "RoarForYou", "OnIt", "GoGoGo"])
    if any(k in t for k in ("能不能", "可以吗", "行不行", "有没有办法")):
        return random.choice(["THINKING", "SMART", "WITTY", "OK"])

    # ── 用户发了个任务/指令 ──
    if any(k in t for k in ("做一个", "写一个", "创建", "生成", "build", "make", "create")):
        return random.choice(["MUSCLE", "GoGoGo", "Fire", "STRIVE", "RoarForYou"])
    if any(k in t for k in ("改一下", "修一下", "fix", "调整", "优化", "更新")):
        return random.choice(["OnIt", "THUMBSUP", "SALUTE", "FISTBUMP"])
    if any(k in t for k in ("删掉", "去掉", "remove", "delete", "砍掉")):
        return random.choice(["CLEAVER", "OK", "CheckMark"])
    if any(k in t for k in ("查一下", "看看", "check", "找", "搜索")):
        return random.choice(["EYES", "StatusReading", "SMART", "THINKING"])
    if any(k in t for k in ("发送", "发给", "send", "推送", "通知")):
        return random.choice(["OnIt", "SALUTE", "GoGoGo"])

    # ── 用户在分享好消息 ──
    if any(k in t for k in ("成功了", "搞定了", "done", "完成了", "work了", "好了")):
        return random.choice(["YEAH", "Partying", "FIREWORKS", "CLAP", "APPLAUSE", "Hundred"])
    if any(k in t for k in ("太棒了", "amazing", "awesome", "牛", "厉害", "666", "nice")):
        return random.choice(["WOW", "Fire", "PROUD", "Partying", "APPLAUSE"])
    if any(k in t for k in ("哈哈", "笑死", "lol", "haha", "好笑", "绝了")):
        return random.choice(["LAUGH", "LOL", "CHUCKLE", "BeamingFace", "TRICK"])

    # ── 用户在抒发情绪 ──
    if any(k in t for k in ("烦", "累", "难", "头疼", "无语", "服了")):
        return random.choice(["HUG", "COMFORT", "Sigh", "HEART"])
    if any(k in t for k in ("谢谢", "感谢", "thanks", "thx", "辛苦")):
        return random.choice(["BLUSH", "FINGERHEART", "LOVE", "INNOCENTSMILE", "SHY"])
    if any(k in t for k in ("不要", "别", "stop", "算了", "不用了")):
        return random.choice(["OK", "THUMBSUP", "CheckMark", "Shrug"])

    # ── 用户发图片 ──
    if any(k in t for k in ("图片", "截图", "screenshot", "看这个", "image")):
        return random.choice(["EYES", "SMART", "StatusReading", "THINKING"])

    # ── 用户在聊天/闲聊 ──
    if any(k in t for k in ("你觉得", "what do you think", "怎么看", "你的意见")):
        return random.choice(["THINKING", "SMART", "WITTY", "SMIRK"])
    if any(k in t for k in ("在吗", "hello", "hi", "你好", "嗨")):
        return random.choice(["WAVE", "SMILE", "BeamingFace", "Delighted", "JOYFUL"])
    if any(k in t for k in ("晚安", "good night", "睡了")):
        return random.choice(["GeneralMoonRest", "WAVE", "HEART"])
    if any(k in t for k in ("早", "morning", "早上好")):
        return random.choice(["GeneralSun", "Coffee", "WAVE", "BeamingFace"])

    # ── 默认：收到了，马上处理 ──
    return random.choice([
        "OnIt", "OK", "THUMBSUP", "SMILE", "JIAYI",
        "SALUTE", "BeamingFace", "WINK",
    ])


async def _add_reaction(message_id: str, emoji_type: str):
    """给消息添加表情 reaction"""
    proc = await asyncio.create_subprocess_exec(
        shutil.which("lark-cli") or "lark-cli",
        "im", "reactions", "create",
        "--params", json.dumps({"message_id": message_id}),
        "--data", json.dumps({"reaction_type": {"emoji_type": emoji_type}}),
        "--as", "bot",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    if proc.returncode == 0:
        print(f"[reaction] {emoji_type} → {message_id[:16]}...", flush=True)


def _extract_options(text: str) -> list[tuple[str, str]]:
    lines = text.strip().split('\n')
    option_lines = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            if option_lines:
                break
            continue
        m = re.match(r'^(\d+|[a-zA-Z])[.）\)、]\s*(.+)', line)
        if m:
            option_lines.append((m.group(1), m.group(2).strip()))
        elif option_lines:
            break
        else:
            break
    option_lines.reverse()
    if len(option_lines) >= 2:
        return [
            (f"{key}. {desc}" if len(desc) <= 18 else f"{key}. {desc[:16]}..", key)
            for key, desc in option_lines
        ]
    tail = "\n".join(lines[-3:]) if len(lines) >= 3 else text
    if re.search(r'\by\b.*\bn\b|Y/N|yes.*no|是/否|确认/取消', tail, re.IGNORECASE):
        return [("Yes", "yes"), ("No", "no")]
    return []


def _format_tool(name: str, inp: dict) -> str:
    n = name.lower()
    if n == "bash":
        cmd = inp.get("command", "")
        if len(cmd) > 80:
            cmd = cmd[:77] + "..."
        return f"🔧 **执行命令：** `{cmd}`" if cmd else f"🔧 **执行命令...**"
    elif n in ("read_file", "read"):
        return f"📄 **读取：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("write_file", "write"):
        return f"✏️ **写入：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("edit_file", "edit"):
        return f"✂️ **编辑：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("glob",):
        return f"🔍 **搜索文件：** `{inp.get('pattern', '')}`"
    elif n in ("grep",):
        return f"🔎 **搜索内容：** `{inp.get('pattern', '')}`"
    elif n == "task":
        return f"🤖 **子任务：** {inp.get('description', inp.get('prompt', '')[:40])}"
    elif n == "webfetch":
        return f"🌐 **抓取网页...**"
    elif n == "websearch":
        return f"🔍 **搜索：** {inp.get('query', '')}"
    else:
        return f"⚙️ **{name}**"


# ── 安全的 task 创建（防止异常被静默吞掉）─────────────────────

def _safe_create_task(coro):
    """创建 asyncio task 并捕获未处理的异常"""
    task = asyncio.create_task(coro)
    task.add_done_callback(_handle_task_exception)
    return task

def _handle_task_exception(task):
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        print(f"[error] 未处理的 task 异常: {type(exc).__name__}: {exc}", flush=True)


# ── lark-cli 事件循环 ────────────────────────────────────────

async def _event_reader(proc, failures=None):
    """从 lark-cli 的 stdout 读取 NDJSON 事件并分发处理

    Returns one of: "idle" (no events for IDLE_TIMEOUT 600s), "wake" (sleep resume),
    "eof" (stdout closed).
    """
    global _reconnect_pending
    queue = asyncio.Queue()
    main_loop = asyncio.get_event_loop()

    def _reader_thread():
        """专用线程：阻塞读 stdout，逐行放入 queue"""
        try:
            for line in proc.stdout:
                line = line.strip()
                if line:
                    main_loop.call_soon_threadsafe(queue.put_nowait, line)
        except Exception as e:
            print(f"[lark-cli reader] thread error: {type(e).__name__}: {e}", flush=True)
        # stdout closed (process exited or pipe broken) → send sentinel
        main_loop.call_soon_threadsafe(queue.put_nowait, None)

    reader = threading.Thread(target=_reader_thread, daemon=True)
    reader.start()

    _last_event_in_reader = time.time()
    IDLE_TIMEOUT = 600

    while True:
        try:
            line = await asyncio.wait_for(queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            # 先看 watchdog 是否提示机器刚醒
            if _wake_event.is_set():
                _wake_event.clear()
                if _has_active_runs():
                    _reconnect_pending = True
                    _last_event_in_reader = time.time()
                    print("[lark-cli] wake 信号延迟（任务活跃），待任务结束后重连", flush=True)
                    continue
                print(f"[lark-cli] 收到唤醒信号，主动断开重连", flush=True)
                _kill_process_tree(proc)
                return "wake"
            idle_seconds = time.time() - _last_event_in_reader
            if idle_seconds > IDLE_TIMEOUT:
                if _has_active_runs():
                    _reconnect_pending = True
                    _last_event_in_reader = time.time()
                    print(f"[lark-cli] {idle_seconds/60:.0f}分钟无事件，任务活跃，延迟重连", flush=True)
                    continue
                print(f"[lark-cli] {idle_seconds/60:.0f}分钟无事件(idle)，重启连接", flush=True)
                _kill_process_tree(proc)
                return "idle"
            continue

        if line is None:
            exit_code = proc.poll()
            print(f"[lark-cli] stdout EOF, exit_code={exit_code}, 准备重连...", flush=True)
            return "eof"

        _last_event_in_reader = time.time()

        try:
            evt = json.loads(line)
        except Exception:
            print(f"[lark-cli] 非JSON行: {line[:100]}", flush=True)
            continue

        # 判断事件类型（兼容 raw 和 compact 格式）
        header = evt.get("header", {})
        event_type = header.get("event_type", "") or evt.get("event_type", evt.get("type", ""))

        if "card.action" in event_type or evt.get("action"):
            _safe_create_task(handle_card_action_from_cli(evt))
        elif "drive.notice.comment" in event_type:
            _safe_create_task(handle_doc_comment_from_cli(evt))
        elif "im.message" in event_type or evt.get("message_id") or (evt.get("event", {}).get("message")):
            _safe_create_task(handle_message_from_cli(evt))
        else:
            print(f"[lark-cli] 跳过事件: {json.dumps(evt, ensure_ascii=False)[:200]}", flush=True)


def _kill_process_tree(proc):
    """Kill a subprocess and ALL its children.

    Tries graceful shutdown first (close stdin → lark-cli sends unsubscribe →
    closes WebSocket cleanly), then falls back to force kill after a timeout.
    This prevents server-side subscription leakage that happens when we
    force-kill without giving the process a chance to unsubscribe.

    Safe to call with proc=None (no-op).
    """
    if proc is None:
        return
    import platform

    # Step 1: close stdin to signal graceful shutdown (lark-cli treats stdin EOF as shutdown)
    try:
        if proc.stdin:
            proc.stdin.close()
    except Exception:
        pass

    # Step 2: wait briefly for graceful exit
    try:
        proc.wait(timeout=3)
        return  # exited cleanly
    except subprocess.TimeoutExpired:
        pass

    # Step 3: force kill the process tree
    if platform.system() == "Windows":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
        )
    else:
        import signal
        pgid = None
        try:
            pgid = os.getpgid(proc.pid)
        except (ProcessLookupError, OSError, AttributeError):
            pass

        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, OSError):
                pass

        try:
            proc.wait(timeout=2)
            return
        except subprocess.TimeoutExpired:
            pass

        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
        else:
            try:
                proc.kill()
            except (ProcessLookupError, OSError):
                pass

        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


async def run_lark_cli_loop():
    """持续运行 lark-cli event +subscribe，断线自动重连

    Key design decisions:
    - Uses --force so orphan connections from a previous crash don't block us
    - Spawns lark-cli in its own process group (start_new_session=True) so we
      can kill the entire tree (Node.js parent + children) cleanly
    - Cleanup uses process group kill instead of fragile pgrep pattern matching
    - Exponential backoff on repeated failures (10s → 20s → 40s → ... → max 120s)
    """
    import random
    lark_cli = shutil.which("lark-cli") or "lark-cli"
    cmd = [
        lark_cli, "event", "+subscribe",
        "--as", "bot",
        "--force",
        "--event-types", "im.message.receive_v1,card.action.trigger,drive.notice.comment_add_v1",
    ]

    failures = [0]  # mutable so _event_reader / stderr_reader can increment via closure

    while True:
        await asyncio.to_thread(_cleanup_stale_processes)

        print("[lark-cli] 启动事件订阅...", flush=True)
        proc = None
        reason = "eof"  # default if Popen itself fails
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )

            # ── stderr reader：日志 + 断开检测 ──────────────
            _disconnect_keywords = (
                "connection reset", "broken pipe", "connection was aborted",
                "wsarecv", "no such host", "i/o timeout", "unexpected eof",
                "use of closed network connection",
            )
            def _stderr_reader():
                try:
                    for line in proc.stderr:
                        line = line.strip()
                        if not line or "SDK Info" in line or "not found handler" in line:
                            continue
                        print(f"[lark-cli stderr] {line}", flush=True)
                        lower = line.lower()
                        if any(kw in lower for kw in _disconnect_keywords):
                            print("[lark-cli] ⚠️ 检测到连接断开，立即重启", flush=True)
                            failures[0] += 1
                            _kill_process_tree(proc)
                            return
                except Exception as e:
                    print(f"[lark-cli stderr reader] error: {type(e).__name__}: {e}", flush=True)
            threading.Thread(target=_stderr_reader, daemon=True).start()

            # 等一下看有没有立即报错（进程直接退出）
            await asyncio.sleep(3)
            if proc.poll() is not None:
                failures[0] += 1
                wait = min(10 * (2 ** min(failures[0], 4)), 120)
                wait += random.randint(0, int(wait * 0.3))
                print(f"[lark-cli] 启动失败 (exit={proc.returncode})，{wait}s 后重试 (第{failures[0]}次)", flush=True)
                await asyncio.sleep(wait)
                continue

            print("[lark-cli] ✅ 事件订阅已连接", flush=True)
            failures[0] = 0  # 成功连接后重置失败计数
            reason = await _event_reader(proc, failures)

        except Exception as e:
            failures[0] += 1
            print(f"[lark-cli] 异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()
            reason = "exception"

        _kill_process_tree(proc)

        # idle / wake 是正常连接维护，用短固定延迟
        # eof / exception / 启动失败 才是真正的异常，用指数退避
        if reason in ("idle", "wake"):
            wait = 10 + random.randint(0, 5)
        else:
            wait = min(10 * (2 ** min(failures[0], 4)), 120)
            wait += random.randint(0, int(wait * 0.3))
        print(f"[lark-cli] {wait}s 后重连... (reason={reason}, failures={failures[0]})", flush=True)
        await asyncio.sleep(wait)


# ── 启动 ──────────────────────────────────────────────────────

def _cleanup_stale_processes():
    """Kill any orphaned lark-cli subscribe processes from previous crashes.

    Uses SIGKILL to ensure Node.js processes don't ignore SIGTERM.
    Also tries process-group kill for each PID in case children survived.
    This is a safety net — the primary cleanup is _kill_process_tree()
    called in run_lark_cli_loop(). This function handles the case where
    the bridge itself crashed and restarted, leaving orphans with no
    parent to clean them up.
    """
    import platform
    try:
        if platform.system() == "Windows":
            result = subprocess.run(
                ["taskkill", "/F", "/IM", "lark-cli.exe"],
                capture_output=True, text=True,
            )
            killed_count = result.stdout.count("SUCCESS") + result.stdout.count("成功")
            if killed_count > 0:
                print(f"   清理旧进程  : 已杀掉 {killed_count} 个 lark-cli 残留进程", flush=True)
                time.sleep(3)
        else:
            import signal
            result = subprocess.run(
                ["pgrep", "-f", "lark-cli.*event.*subscribe"],
                capture_output=True, text=True
            )
            pids = [p.strip() for p in result.stdout.strip().split("\n") if p.strip()]
            if pids:
                for pid in pids:
                    try:
                        pid_int = int(pid)
                        try:
                            pgid = os.getpgid(pid_int)
                            os.killpg(pgid, signal.SIGKILL)
                        except (ProcessLookupError, OSError):
                            os.kill(pid_int, signal.SIGKILL)
                    except (ProcessLookupError, ValueError, OSError):
                        pass
                print(f"   清理旧进程  : 已杀掉 {len(pids)} 个 lark-cli 残留进程", flush=True)
                time.sleep(3)
    except Exception:
        pass


def main():
    # Reconfigure stdout for Windows GBK terminals that can't handle emoji
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("🚀 飞书 Claude Bot 启动中...")
    print(f"   App ID      : {config.FEISHU_APP_ID}")
    print(f"   默认模型    : {config.DEFAULT_MODEL}")
    print(f"   默认工作目录: {config.DEFAULT_CWD}")
    print(f"   权限模式    : {config.PERMISSION_MODE}")
    print(f"   事件接收    : lark-cli WebSocket (单连接)")

    # 清理旧的 lark-cli 残留进程
    _cleanup_stale_processes()

    # 启动看门狗线程
    t = threading.Thread(target=_watchdog, daemon=True)
    t.start()

    # 运行 asyncio 事件循环
    asyncio.run(run_lark_cli_loop())


if __name__ == "__main__":
    main()
