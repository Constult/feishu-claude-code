# 飞书 Bot 回复速度骤降（"一秒一个字"）

**日期**: 2026-06-02
**状态**: 已修复
**影响**: bot 流式回复极慢，用户体感 "一秒一个字"

---

## 症状

飞书 bot 回复文字以约 1 字/秒的速度出现，之前一直正常。代码自 5 月 18 日以来未改动。

## 排查过程

### 方向 1：流式推送参数（误判）

最初怀疑 `main.py:1097-1100` 的流式参数变化：

| 参数 | 改前 (5/18) | 改后 |
|------|------------|------|
| `_MIN_INTERVAL` | 0.3s | 0.8s |
| `_MAX_INTERVAL` | 0.8s | 2.0s |
| `_DOC_SWITCH_THRESHOLD` | 200 | 30 |

虽然改慢了，但不足以造成"一秒一个字"。而且参数 5 月 18 日就改了，用户直到今天才觉得慢 → **不是根因**。

但 `_MIN_CHUNK=80` 配合 `_MAX_INTERVAL=2.0` 确实有 drip-feed 问题：前 30 字内永远攒不到 80 字，只能靠 `_MAX_INTERVAL` 每 2 秒硬推，每次只推 2-4 个字。**最终修复：前 30 字改为每 0.5 秒有字就推。**

### 方向 2：Claude Code 版本（部分相关）

WinGet 自动升级了 Claude Code：2.1.138 → 2.1.152（5 月 29 日）。v2.1.152 有已知性能回归：
- 3 秒 thinking 显示保底延迟
- 孤儿 thinking 消息过滤触发额外重试
- stream-json 模式下 stdin 关闭 hang

**处理**：升级到 v2.1.158 修复这些回归。

### 方向 3：TCP 连接退化（部分相关）

`bridge.log` 累计 536 次重试、170 次 `ConnectionResetError(10054)`。桥进程连续跑了 108+ 小时，多次休眠唤醒导致 Windows TCP 栈退化。

**处理**：杀掉旧进程重启，清空 TCP 连接池。但重启后重试依旧 → **不是根因**。

### 方向 4：真正的根因 — httpx ConnectError

增强日志后发现每次 `update_card()` 都抛 `ConnectError`。第一个 `reply_card()` 总是成功，但后续流式推送全部失败。

**根因**：`lark-oapi` SDK 的 `Transport.aexecute()` 每次 API 调用都执行 `async with httpx.AsyncClient() as client` —— 新建 TCP 连接 → 发请求 → 关闭连接。短时间内对 `open.feishu.cn:443` 重复建连，飞书服务端或网络层以 TCP RST 拒绝后续连接。

这解释了为什么"以前正常"：之前 SDK 的短连接模式碰巧没触发限制，今天触发了。可能原因：飞书收紧连接频率限制、网络环境变化、或 Windows 更新影响 TCP 行为。

## 修复方案

### 主线：monkey-patch SDK transport，持久连接池

`feishu_client.py` 中 monkey-patch `Transport.aexecute`，将每次新建 `httpx.AsyncClient()` 改为复用全局持久客户端：

```python
_patched_client = httpx.AsyncClient(
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    timeout=httpx.Timeout(30.0, connect=10.0),
)
Transport.aexecute = _patched_aexecute
```

### 辅助改动

1. **流式推送不重试**：新增 `update_card_fast()` 方法，流式帧失败直接跳过等下一帧，不阻塞 3.5 秒重试
2. **前 30 字快推**：0.5 秒有字就推（替代攒 80 字/2 秒间隔）
3. **SDK 升级**：lark-oapi 1.5.5 → 1.6.8

## 踩坑记录

1. **`NameError: name 'lark_oapi' is not defined`**：monkey-patch 函数内用了 `lark_oapi.core.model.xxx`，但模块顶部 `import lark_oapi as lark` 没有注册 `lark_oapi` 为可导入名。修法：加一行 `import lark_oapi`（无别名），函数内改用 `lark_oapi.core.model.xxx`。

2. **`ModuleNotFoundError: No module named 'lark'`**：同上根因，函数内 `from lark.core.model import` 找不到 `lark` 包。修法同上。

3. **初始误判**：一开始把 `_DOC_SWITCH_THRESHOLD=30` 改成 500，用户指出长文走文档比流式卡片更快，改回 30。

## 涉及文件

- `feishu_client.py`：monkey-patch + `update_card_fast()` + 日志增强
- `main.py`：前 30 字 0.5s 快推 + 流式用 `update_card_fast`

## 验证

重启后三条消息（"hi"、wiki 链接、"分析录音"）全部零重试，卡片和文档正常生成。

## 经验教训

- 先确认根因再改代码，不要看到臭的参数就改
- "以前正常今天突然慢" → 找环境变化而非代码变化
- 第三方 SDK 的隐式行为（每次新建连接）是定时炸弹
- 先加日志再分析，空异常信息是最大的信息
