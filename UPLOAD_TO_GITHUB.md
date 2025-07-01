# GitHub上传文件清单 - v0.2.0

## 需要上传的核心文件

### 根目录文件
```
README.md                    # 英文主文档
README_cn.md                 # 中文文档
requirements.txt             # Python依赖
scheduler.py                 # 调度器
start_scheduler.sh           # 启动脚本
stop_services.sh             # 停止脚本
.env.example                 # 环境变量模板
.gitignore                   # Git忽略文件
```

### Core目录
```
Core/
├── DB/
│   ├── db_init.py          # 数据库初始化
│   └── db_utils.py         # 数据库工具
├── bt_write_macro_data.py  # 宏观数据获取
├── bt_macro_tech_analysis.py # 技术分析引擎
├── bt_plot_tech_analysis.py # 图表生成引擎
├── bt_portfolio_get.py     # 投资组合跟踪
├── bt_benchmark_get.py     # 媒体情感分析
├── bt_test_run.py          # 回测引擎
├── bt_data_validator.py    # 数据验证
├── macro_config.py         # 配置文件
├── check_bank_data.py      # 银行数据检查
├── get_china_market_data.py # 中国市场数据
├── test_akshare_bank.py    # akshare银行测试
├── test_central_bank.py    # 央行数据测试
├── test_gold_spot.py       # 黄金现货测试
├── test_gold_write.py      # 黄金数据写入测试
└── update_bank_rates.py    # 银行利率更新
```

### 其他文件
```
SCHEDULER_USAGE.md          # 调度器使用说明
debug_file_match.py         # 调试文件匹配
```

## 不需要上传的文件

### 生成的文件和目录
```
plot_html/                  # 生成的HTML图表文件
├── *.html                  # 所有HTML文件
├── *.txt                   # 分析报告文件
└── *.md                    # 投资组合报告
```

### 环境和配置文件
```
.env                        # 实际环境变量（包含敏感信息）
.trae/                      # IDE配置目录
logs/                       # 日志文件（如果存在）
data/                       # 数据文件（如果存在）
__pycache__/                # Python缓存
*.pyc                       # Python编译文件
*.log                       # 日志文件
```

## GitHub上传步骤

1. **创建GitHub仓库**
   ```bash
   # 在GitHub网站创建新仓库：intelligent-investment-analysis
   # 描述：An intelligent investment analysis system built with Python and PostgreSQL
   ```

2. **初始化本地Git仓库**
   ```bash
   cd /path/to/Backtrader
   git init
   git add README.md README_cn.md requirements.txt scheduler.py
   git add start_scheduler.sh stop_services.sh .env.example .gitignore
   git add Core/ SCHEDULER_USAGE.md debug_file_match.py
   ```

3. **提交并推送**
   ```bash
   git commit -m "Initial commit - v0.2.0: Intelligent Investment Analysis System"
   git branch -M main
   git remote add origin https://github.com/your-username/intelligent-investment-analysis.git
   git push -u origin main
   ```

4. **创建版本标签**
   ```bash
   git tag -a v0.2.0 -m "Version 0.2.0 - Core system with multi-source data integration"
   git push origin v0.2.0
   ```

## 版本说明 - v0.2.0

### 主要特性
- ✅ 多源数据集成（yfinance, akshare）
- ✅ 技术分析引擎（30+技术指标）
- ✅ 智能评分系统
- ✅ 交互式图表生成
- ✅ 媒体情感分析
- ✅ 自动化调度系统
- ✅ PostgreSQL数据中心

### 数据覆盖
- 股票指数：上证、深证、沪深300、中证500、标普500、纳斯达克、道琼斯
- 外汇市场：美元指数、主要货币对、人民币汇率
- 大宗商品：黄金期货、原油期货、白银期货
- 贵金属：上海金系列（Au99.99/Au100g/Au(T+D)）、中国黄金储备
- 利率市场：美联储利率、中国LPR、SHIBOR、欧洲央行利率
- 数字资产：比特币、以太坊等主要加密货币

### 技术栈
- Python 3.8+
- PostgreSQL 12+
- Backtrader, Pandas, TA-Lib
- Plotly可视化
- n8n工作流编排
- FinBERT情感分析