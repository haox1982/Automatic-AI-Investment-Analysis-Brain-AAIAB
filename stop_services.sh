#!/bin/bash

# 投资分析系统服务停止脚本

echo "=== 投资分析系统服务停止脚本 ==="
echo "当前时间: $(date)"

# 定义路径
BACKTRADER_DIR="/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader"
AKSHARE_DIR="/Volumes/ext-fx/Coding/6.Docker/19.Python_Flask/akshare_api"

# 停止定时任务调度器
if [ -f "$BACKTRADER_DIR/scheduler.pid" ]; then
    SCHEDULER_PID=$(cat "$BACKTRADER_DIR/scheduler.pid")
    if kill -0 $SCHEDULER_PID 2>/dev/null; then
        echo "停止定时任务调度器 (PID: $SCHEDULER_PID)..."
        kill $SCHEDULER_PID
        sleep 2
        if kill -0 $SCHEDULER_PID 2>/dev/null; then
            echo "强制停止定时任务调度器..."
            kill -9 $SCHEDULER_PID
        fi
        echo "定时任务调度器已停止"
    else
        echo "定时任务调度器未运行"
    fi
    rm -f "$BACKTRADER_DIR/scheduler.pid"
else
    echo "未找到定时任务调度器PID文件"
fi

# 停止AkShare API服务
echo "尝试通过进程名停止AkShare API服务..."
if pgrep -f "ak_api.py" > /dev/null; then
    echo "发现正在运行的AkShare API服务，正在停止..."
    pkill -f "ak_api.py"
    sleep 2
    if pgrep -f "ak_api.py" > /dev/null; then
        echo "强制停止AkShare API服务..."
        pkill -9 -f "ak_api.py"
    fi
    echo "AkShare API服务已停止。"
else
    echo "AkShare API服务未运行。"
fi

# 清理旧的PID文件（如果存在），以防万一
if [ -f "$AKSHARE_DIR/akshare_api.pid" ]; then
    rm -f "$AKSHARE_DIR/akshare_api.pid"
    echo "已清理旧的AkShare PID文件。"
fi

# 强制停止所有相关进程
echo "检查并强制停止所有相关进程..."
SCHEDULER_PIDS=$(pgrep -f "scheduler.py")
if [ -n "$SCHEDULER_PIDS" ]; then
    echo "发现以下scheduler.py进程正在运行，正在强制停止:"
    echo "$SCHEDULER_PIDS"
    for PID in $SCHEDULER_PIDS; do
        echo "强制停止进程 PID: $PID"
        kill -9 $PID 2>/dev/null
    done
fi

AKAPI_PIDS=$(pgrep -f "ak_api.py")
if [ -n "$AKAPI_PIDS" ]; then
    echo "发现以下ak_api.py进程正在运行，正在强制停止:"
    echo "$AKAPI_PIDS"
    for PID in $AKAPI_PIDS; do
        echo "强制停止进程 PID: $PID"
        kill -9 $PID 2>/dev/null
    done
fi

# 最后检查是否还有进程在运行
REMAINING_PROCESSES=$(ps aux | grep -E '(scheduler.py|ak_api.py)' | grep -v grep)
if [ -n "$REMAINING_PROCESSES" ]; then
    echo "警告: 仍有以下进程无法停止:"
    echo "$REMAINING_PROCESSES"
else
    echo "所有服务已成功停止"
fi

echo "=== 停止脚本执行完成 ==="