#!/bin/bash
# 飞书 bridge 健康检查 + 自动修复（带防抖）
# 用法：定期执行（cron / 计划任务 / 手动）

set -euo pipefail

BRIDGE_DIR="/d/claude-workspace/feishu-bridge"
BRIDGE_LOG="$BRIDGE_DIR/bridge.log"
COUNT_FILE="/tmp/feishu_bridge_restart_count.txt"
LARK_USER_ID="ou_f727fbca771cb083bba09202c63cf6ab"
TODAY=$(date +%Y-%m-%d)

# ── 1. 检查 python.exe 是否在运行 ──
python_running=false
if tasklist 2>/dev/null | grep -qi "python.exe"; then
    python_running=true
fi

# ── 2. 检查 bridge.log 最后 10 行是否有 "stdout EOF" 重连循环 ──
eof_loop=false
if [ -f "$BRIDGE_LOG" ]; then
    if tail -10 "$BRIDGE_LOG" 2>/dev/null | grep -q "stdout EOF"; then
        eof_loop=true
    fi
fi

# ── Bridge 正常则退出 ──
if $python_running && ! $eof_loop; then
    echo "[$(date '+%H:%M:%S')] bridge OK"
    exit 0
fi

echo "[$(date '+%H:%M:%S')] bridge 异常: python_running=$python_running eof_loop=$eof_loop"

# ── 3. 读取/更新今日重启计数 ──
if [ -f "$COUNT_FILE" ]; then
    read -r file_date file_count < "$COUNT_FILE" || { file_date=""; file_count=0; }
else
    file_date=""
    file_count=0
fi

if [ "$file_date" != "$TODAY" ]; then
    count=1
else
    count=$((file_count + 1))
fi

echo "$TODAY $count" > "$COUNT_FILE"

# ── 4. 按计数决策 ──
if [ "$count" -le 3 ]; then
    echo "[$(date '+%H:%M:%S')] 第 ${count} 次重启，执行修复..."

    taskkill /F /IM python.exe 2>/dev/null || true
    taskkill /F /IM lark-cli.exe 2>/dev/null || true
    sleep 2

    # 后台启动 bridge
    cd "$BRIDGE_DIR"
    PYTHONIOENCODING=utf-8 PYTHONLEGACYWINDOWSSTDIO=utf-8 nohup python main.py >> bridge.log 2>&1 &
    disown

    echo "[$(date '+%H:%M:%S')] bridge 已重启"

    lark-cli im +messages-send \
        --user-id "$LARK_USER_ID" \
        --text "🔧 飞书 bridge 已自动重启（今日第 ${count} 次）" \
        --as user

    echo "[$(date '+%H:%M:%S')] 已发送重启通知"
else
    echo "[$(date '+%H:%M:%S')] 24h 内崩溃 ${count} 次，超过阈值，停止自动修复"

    lark-cli im +messages-send \
        --user-id "$LARK_USER_ID" \
        --text "⚠️ 飞书 bridge 24h 内崩溃超过 3 次，已停止自动修复，请手动排查" \
        --as user

    echo "[$(date '+%H:%M:%S')] 已发送告警通知"
fi
