# feishu-bridge

飞书 ↔ Claude Code 桥接器。Python 进程通过 lark-cli WebSocket 接收飞书消息，调用本地 `claude` CLI 生成回复，以流式卡片返回到飞书。

> **状态：稳定运行** — 2026年5月起长期稳定，无已知 bug。历史踩坑记录见 memory。

## 架构

```
飞书服务器
  │ WebSocket (lark-cli event +subscribe --as bot)
  ▼
main.py ──事件分发──→ handle_message_from_cli()
  │                    ├─ 文本/图片/富文本/转发 → _process_message_cli()
  │                    ├─ 卡片按钮回调 → handle_card_action_from_cli()
  │                    └─ 文档评论 @ → handle_doc_comment_from_cli()
  │
  ├─ claude_runner.py    → subprocess: claude --print --output-format stream-json
  ├─ feishu_client.py    → 飞书 API（lark_oapi SDK）：发卡片、patch 流式更新、下载图片
  ├─ session_store.py    → ~/.feishu-claude/ 持久化 session
  ├─ commands.py         → /new /resume /model /mode 等斜杠命令
  └─ run_control.py      → ActiveRun 注册表，/stop 中断
```

## 关键约束

- **平台**：代码已适配 Windows 和 macOS。进程清理先尝试优雅关闭（关 stdin 等 3s 让 lark-cli 发 unsubscribe），超时才强杀（Windows: taskkill /F，macOS: os.killpg(SIGKILL)）
- **Secret 双份**：lark-cli 读全局 `~/.lark-cli/config.json`（收消息），Python SDK 读项目 `.env`（发消息）。两份 App ID 必须一致，Secret 分别存储在系统凭据管理器和 .env 中
- **文档操作走 lark-cli**：消息中包含飞书文档链接（docx/wiki/docs/sheets/base/mindnotes/file）时，bridge 自动用 `lark-cli docs +fetch --as user` 获取文档内容并嵌入上下文。**妙记（minutes）例外**：走 `lark-cli vc +notes --minute-tokens <token>` 获取逐字稿+AI 总结+章节。media 消息类型（直接分享文档附件）同理走 lark-cli。所需 scope：`vc:note:read` `minutes:minutes:readonly` `minutes:minutes.artifacts:read` `minutes:minutes.transcript:export`
- **文件回传 [[SEND_FILE]]**：Claude 在回复中输出 `[[SEND_FILE:路径]]` 时，bridge 自动将文件上传到飞书 Drive 并替换为下载链接，支持相对/绝对路径，任意文件类型
- **不要改 .env**：`.env` 在 `.gitignore` 里，不要提交
- **改代码后清缓存**：`rm -rf __pycache__`，否则 Python 可能跑旧的 .pyc
- **JSON 输出不乱码**：所有 `subprocess.Popen` 必须显式 `encoding="utf-8"`

## 启动 / 停止

```bash
# 手动启动（foreground）
python main.py

# 后台启动
# Windows: start_bridge.bat（已注册到 HKCU Run 键，登录自动启动）
# macOS: launchctl load ~/Library/LaunchAgents/com.zara.feishu-claude.plist

# 完全停止
# Windows: taskkill /F /IM python.exe && taskkill /F /IM lark-cli.exe
# macOS: launchctl unload ...
```

## 目录结构

```
.env              # 凭证（不入 git）
.env.example      # 凭证模板
main.py           # 主循环：事件订阅、分发、回复
feishu_client.py  # 飞书 API 封装（发卡片、patch 流式、下载图片）
claude_runner.py  # 调 claude CLI，解析 stream-json
commands.py       # 斜杠命令
session_store.py  # 会话持久化
run_control.py    # 运行中任务管理
bot_config.py     # 读 .env 配置
start_bridge.bat  # Windows 启动脚本
deploy/           # macOS launchd plist
TROUBLESHOOTING.md # 故障排查
```

