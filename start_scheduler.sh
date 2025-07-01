#!/bin/bash

# 投资分析系统启动脚本
# 包含定时任务调度器和AkShare API服务

echo "=== 投资分析系统启动脚本 ==="
echo "当前时间: $(date)"
echo "工作目录: $(pwd)"

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到python3命令"
    exit 1
fi

echo "Python版本: $(python3 --version)"

# 检查必要的Python包
echo "检查Python依赖包..."
python3 -c "import schedule, pandas, yfinance, flask, akshare" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "警告: 部分Python包可能未安装，尝试安装..."
    pip3 install schedule pandas yfinance plotly akshare flask
fi

# 定义路径
BACKTRADER_DIR="/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader"
AKSHARE_DIR="/Volumes/ext-fx/Coding/6.Docker/19.Python_Flask/akshare_api"

# 检查目录和脚本
if [ ! -d "$BACKTRADER_DIR" ]; then
    echo "错误: Backtrader目录不存在: $BACKTRADER_DIR"
    exit 1
fi

if [ ! -d "$AKSHARE_DIR" ]; then
    echo "错误: AkShare API目录不存在: $AKSHARE_DIR"
    exit 1
fi

if [ ! -f "$BACKTRADER_DIR/scheduler.py" ]; then
    echo "错误: 未找到scheduler.py文件"
    exit 1
fi

if [ ! -f "$AKSHARE_DIR/ak_api.py" ]; then
    echo "错误: 未找到ak_api.py文件"
    exit 1
fi

# 创建日志目录
mkdir -p "$BACKTRADER_DIR/logs"
mkdir -p "$AKSHARE_DIR/logs"

echo "=== 启动服务 ==="

# 检查是否有已运行的进程
echo "检查是否有已运行的进程..."
SCHEDULER_PIDS=$(pgrep -f "scheduler.py")
if [ -n "$SCHEDULER_PIDS" ]; then
    echo "发现以下scheduler.py进程正在运行，将先停止这些进程:"
    echo "$SCHEDULER_PIDS"
    for PID in $SCHEDULER_PIDS; do
        echo "停止进程 PID: $PID"
        kill -9 $PID 2>/dev/null
    done
    echo "已停止所有旧的scheduler.py进程"
    sleep 2
fi

AKAPI_PIDS=$(pgrep -f "ak_api.py")
if [ -n "$AKAPI_PIDS" ]; then
    echo "发现以下ak_api.py进程正在运行，将先停止这些进程:"
    echo "$AKAPI_PIDS"
    for PID in $AKAPI_PIDS; do
        echo "停止进程 PID: $PID"
        kill -9 $PID 2>/dev/null
    done
    echo "已停止所有旧的ak_api.py进程"
    sleep 2
fi

# 启动定时任务调度器（后台运行）
echo "启动定时任务调度器..."
cd "$BACKTRADER_DIR" || {
    echo "错误: 无法切换到目录 $BACKTRADER_DIR"
    exit 1
}
nohup python3 scheduler.py > logs/scheduler_output.log 2>&1 &
SCHEDULER_PID=$!
echo "定时任务调度器已启动，PID: $SCHEDULER_PID"
echo "日志文件: $BACKTRADER_DIR/scheduler.log"
echo "输出日志: $BACKTRADER_DIR/logs/scheduler_output.log"

# 等待一下确保调度器启动
sleep 2

# 启动AkShare API服务（后台运行）
echo "启动AkShare API服务..."
cd "$AKSHARE_DIR" || {
    echo "错误: 无法切换到目录 $AKSHARE_DIR"
    exit 1
}
nohup python3 ak_api.py > logs/akshare_api.log 2>&1 &
AKSHARE_PID=$!
echo "AkShare API服务已启动，PID: $AKSHARE_PID"
echo "日志文件: $AKSHARE_DIR/logs/akshare_api.log"

# 保存PID到文件，方便后续管理
echo $SCHEDULER_PID > "$BACKTRADER_DIR/scheduler.pid"
echo $AKSHARE_PID > "$AKSHARE_DIR/akshare_api.pid"

echo ""
echo "=== 系统启动完成 ==="
echo "定时任务调度器功能:"
echo "- 每日10:00: 宏观数据更新"
echo "- 每日10:30: 技术分析图表生成"
echo "- 每日10:40: 宏观数据文本分析"
echo "- 每周一10:10: 投资组合跟踪"
echo ""
echo "AkShare API服务: 提供金融数据API接口"
echo ""
echo "要停止服务，请运行:"
echo "kill $SCHEDULER_PID  # 停止定时任务调度器"
echo "kill $AKSHARE_PID   # 停止AkShare API服务"
echo ""
echo "或者使用以下命令查看运行状态:"
echo "ps aux | grep -E '(scheduler.py|ak_api.py)'"
echo ""
echo "按Ctrl+C退出此脚本（服务将继续在后台运行）"

# 等待用户中断或者定期检查服务状态
trap 'echo "脚本已退出，服务继续在后台运行"; exit 0' INT

while true; do
    sleep 30
    # 检查服务是否还在运行
    if ! kill -0 $SCHEDULER_PID 2>/dev/null; then
        echo "警告: 定时任务调度器已停止 (PID: $SCHEDULER_PID)"
    fi
    if ! kill -0 $AKSHARE_PID 2>/dev/null; then
        echo "警告: AkShare API服务已停止 (PID: $AKSHARE_PID)"
    fi
done