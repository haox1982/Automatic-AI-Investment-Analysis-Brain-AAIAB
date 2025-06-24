# 投资分析定时任务系统使用指南

## 📋 概述

本系统提供了一个自动化的投资分析流程，每天定时执行数据更新和图表生成，为n8n工作流提供最新的分析结果。

## 🚀 快速启动

### 1. 启动定时任务

```bash
# 方法1: 使用启动脚本（推荐）
./start_scheduler.sh

# 方法2: 直接运行Python脚本
python3 scheduler.py
```

### 2. 后台运行

```bash
# 使用nohup在后台运行
nohup ./start_scheduler.sh > scheduler_output.log 2>&1 &

# 或者使用screen
screen -S investment_scheduler
./start_scheduler.sh
# 按Ctrl+A, D分离会话
```

### 3. 检查运行状态

```bash
# 查看日志
tail -f scheduler.log

# 查看进程
ps aux | grep scheduler

# 查看生成的图表文件
ls -la /Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/http/bt/
```

## ⏰ 执行时间表

| 时间 | 任务 | 说明 |
|------|------|------|
| 10:00 | 数据更新 | 执行 `write_macro_data.py` 更新数据库 |
| 10:30 | 图表生成 | 执行 `plot_technical_analysis.py` 生成图表 |
| 10:40 | n8n推送 | n8n读取 `http://files.nltech.ggff.net/bt/index.html` |

## 📁 文件结构

```
Backtrader/
├── scheduler.py              # 主调度器脚本
├── start_scheduler.sh        # 启动脚本
├── scheduler.log            # 调度器日志
├── Core/
│   ├── write_macro_data.py  # 数据更新脚本
│   └── plot_technical_analysis.py  # 图表生成脚本
└── plot_html/               # 图表输出目录
```

## 🔧 配置说明

### 调度器配置

在 `scheduler.py` 中可以修改以下配置：

```python
# 脚本路径
DATA_SCRIPT = "/path/to/write_macro_data.py"
PLOT_SCRIPT = "/path/to/plot_technical_analysis.py"

# 输出目录
SOURCE_DIR = "./plot_html"
TARGET_DIR = "/path/to/http/bt"

# 执行时间
schedule.every().day.at("10:00").do(run_data_update)
schedule.every().day.at("10:30").do(run_plot_generation)
```

### 环境变量

确保以下环境变量已正确配置：

```bash
# 数据库连接
DB_HOST=n8n_postgres
DB_PORT=5432
DB_NAME=n8n
DB_USER=postgres
DB_PASSWORD=your_password

# API密钥（如需要）
FRED_API_KEY=your_fred_key
SEC_API_KEY=your_sec_key
```

## 🛠️ 故障排除

### 常见问题

1. **Python包缺失**
   ```bash
   pip3 install schedule pandas yfinance plotly akshare
   ```

2. **权限问题**
   ```bash
   chmod +x start_scheduler.sh
   chmod +x scheduler.py
   ```

3. **数据库连接失败**
   - 检查Docker容器是否运行
   - 验证数据库连接参数
   - 查看 `.env` 文件配置

4. **文件路径错误**
   - 确认所有路径使用绝对路径
   - 检查目录是否存在且有写权限

### 日志分析

```bash
# 查看详细日志
tail -f scheduler.log

# 搜索错误信息
grep -i error scheduler.log

# 查看特定时间的日志
grep "2025-01-" scheduler.log
```

## 🔄 手动执行

如需手动执行某个步骤：

```bash
# 手动更新数据
cd /Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader
python3 Core/write_macro_data.py

# 手动生成图表
python3 Core/plot_technical_analysis.py

# 手动复制文件
cp -r plot_html/* /Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/http/bt/
```

## 📊 监控建议

1. **设置日志轮转**
   ```bash
   # 添加到crontab
   0 0 * * 0 find /path/to/logs -name "*.log" -mtime +7 -delete
   ```

2. **监控磁盘空间**
   ```bash
   df -h /Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/
   ```

3. **检查进程健康状态**
   ```bash
   # 创建健康检查脚本
   ps aux | grep scheduler || echo "Scheduler not running!"
   ```

## 🚫 停止调度器

```bash
# 找到进程ID
ps aux | grep scheduler

# 停止进程
kill <PID>

# 或者使用pkill
pkill -f scheduler.py
```

## 📞 技术支持

如遇到问题，请检查：
1. 日志文件 `scheduler.log`
2. 系统资源使用情况
3. 网络连接状态
4. 数据库服务状态

---

**注意**: 首次运行前请确保所有依赖已安装，数据库连接正常，且有足够的磁盘空间存储图表文件。