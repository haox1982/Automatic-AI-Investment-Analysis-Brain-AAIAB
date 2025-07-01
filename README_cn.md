# 智能投资分析系统 (Intelligent Investment Analysis System)

## 项目概述

这是一个基于Python和PostgreSQL的智能投资分析系统，专注于宏观经济数据分析、技术指标计算和投资决策支持。系统通过自动化数据获取、多维度技术分析和智能报告生成，为投资者提供全面的市场洞察。

### 核心特性

- 🔄 **自动化数据获取**: 支持多数据源（yfinance、akshare）的增量和全量数据更新
- 📊 **多维度技术分析**: 集成MA、MACD、RSI、布林带等30+技术指标
- 🎯 **智能评分系统**: 基于多指标综合评分的投资建议
- 📈 **可视化图表**: 自动生成交互式HTML技术分析图表
- 🤖 **媒体观点聚合**: 基于FinBERT的财经新闻情感分析
- 📱 **自动化调度**: 通过n8n工作流实现定时执行和通知
- 💾 **数据中心**: PostgreSQL数据库存储历史数据和分析结果

## 系统架构

### 核心组件

```
智能投资分析系统
├── 数据获取层 (Data Acquisition)
│   ├── bt_write_macro_data.py     # 宏观数据获取
│   ├── bt_portfolio_get.py        # 投资组合跟踪
│   └── bt_benchmark_get.py        # 媒体观点聚合
├── 分析引擎 (Analysis Engine)
│   ├── bt_macro_tech_analysis.py  # 技术分析引擎
│   ├── bt_plot_tech_analysis.py   # 图表生成引擎
│   └── bt_test_run.py             # 回测引擎
├── 数据中心 (Data Center)
│   ├── PostgreSQL数据库           # 历史数据存储
│   └── Core/DB/                   # 数据库工具
├── 调度系统 (Scheduler)
│   ├── scheduler.py               # 自动化调度
│   └── n8n工作流                  # 可视化编排
└── 输出系统 (Output)
    ├── plot_html/                 # HTML图表
    ├── 文本报告                   # 分析报告
    └── Telegram通知               # 实时推送
```

### 技术栈

- **数据分析**: Python, Backtrader, Pandas, TA-Lib
- **数据源**: yfinance, akshare
- **数据库**: PostgreSQL
- **可视化**: Plotly, HTML/CSS/JavaScript
- **工作流**: n8n
- **NLP**: FinBERT, 中文关键词分析
- **通知**: Telegram Bot

## 数据覆盖

### 资产类别

| 类别 | 覆盖范围 | 数据源 | 更新频率 |
|------|----------|--------|----------|
| **股票指数** | 上证、深证、沪深300、中证500、标普500、纳斯达克、道琼斯 | yfinance | 日线 |
| **外汇市场** | 美元指数、主要货币对、人民币汇率 | yfinance | 日线 |
| **大宗商品** | 黄金期货、原油期货、白银期货 | yfinance | 日线 |
| **贵金属专项** | 上海金(Au99.99/Au100g/Au(T+D))、中国央行黄金储备 | akshare | 日线/月线 |
| **利率市场** | 美联储利率、中国LPR、SHIBOR、欧央行利率 | akshare | 实时 |
| **宏观指标** | CPI、PPI、GDP、货币供应量 | akshare | 月度/季度 |
| **数字资产** | 比特币、以太坊等主流加密货币 | yfinance | 日线 |

### 数据质量

- **历史深度**: 核心资产拥有20+年历史数据
- **数据完整性**: 自动数据验证和质量检查
- **更新机制**: 增量更新 + 全量备份
- **冲突处理**: 智能去重和数据覆盖策略

## 技术分析功能

### 技术指标

- **趋势指标**: MA(5,10,20,60), EMA, MACD, ADX
- **动量指标**: RSI, Stochastic, Williams %R
- **波动率指标**: 布林带, ATR, 标准差
- **成交量指标**: OBV, 成交量MA, 价量背离
- **支撑阻力**: 关键价位识别, 突破信号

### 智能评分

系统基于多维度技术指标计算综合评分(0-10分):

- **趋势强度** (30%): 基于MA排列和MACD
- **动量状态** (25%): 基于RSI和随机指标
- **波动率** (20%): 基于布林带和ATR
- **成交量确认** (15%): 基于OBV和成交量
- **技术形态** (10%): 基于关键位突破

### 图表功能

- **交互式K线图**: 支持缩放、悬停、指标切换
- **多时间框架**: 日线、周线、月线分析
- **技术指标叠加**: 可自定义指标组合
- **关键位标注**: 自动标识支撑阻力位
- **信号提示**: 买卖信号可视化

## 媒体观点聚合

### 支持媒体源

**英文媒体**:
- Yahoo Finance, Reuters, Bloomberg, MarketWatch

**中文媒体**:
- 新浪财经, 东方财富, 金融界, 证券时报

### 情感分析

- **英文**: 基于FinBERT的专业财经情感分析
- **中文**: 基于关键词和规则的情感识别
- **多维度**: 按资产类别、时间维度聚合
- **趋势识别**: 情感变化趋势和转折点识别

## 安装配置

### 环境要求

```bash
# Python 3.8+
# PostgreSQL 12+
# Docker (可选)
```

### 安装步骤

1. **克隆项目**
```bash
git clone https://github.com/your-username/intelligent-investment-analysis.git
cd intelligent-investment-analysis
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **配置数据库**
```bash
# 创建PostgreSQL数据库
createdb investment_analysis

# 配置环境变量
cp .env.example .env
# 编辑.env文件，填入数据库连接信息
```

4. **初始化数据**
```bash
# 首次运行，获取全量数据
python3 Core/bt_write_macro_data.py --full

# 验证数据
python3 Core/bt_data_validator.py
```

### 配置文件

在`.env`文件中配置以下参数:

```env
# 数据库配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=investment_analysis
DB_USER=your_username
DB_PASSWORD=your_password

# Telegram配置(可选)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# 数据源配置
YFINANCE_TIMEOUT=30
AKSHARE_TIMEOUT=30
```

## 使用指南

### 日常使用

```bash
# 1. 更新数据
python3 Core/bt_write_macro_data.py

# 2. 生成技术分析
python3 Core/bt_macro_tech_analysis.py

# 3. 生成图表
python3 bt_plot_tech_analysis.py --all

# 4. 获取媒体观点
python3 bt_benchmark_get.py

# 5. 自动化调度
python3 scheduler.py
```

### 高级功能

```bash
# 并发数据获取
python3 Core/bt_write_macro_data.py --full --workers 5

# 指定资产分析
python3 bt_plot_tech_analysis.py --symbols "^GSPC,GC=F"

# 自定义时间范围
python3 bt_benchmark_get.py --days 30

# 回测策略
python3 bt_test_run.py
```

### 输出文件

- **HTML图表**: `plot_html/` 目录下的交互式图表
- **分析报告**: `macro_technical_analysis_YYYYMMDD.txt`
- **投资组合**: `portfolio_tracking_report_YYYYMMDD.md`
- **索引页面**: `plot_html/index.html` 统一导航

## 自动化调度

### scheduler.py

内置调度器支持:
- 工作日自动数据更新
- 定时技术分析生成
- 异常处理和重试机制
- Telegram通知推送

### n8n工作流

可视化工作流编排:
- 主工作流定时调度
- 子工作流模块化执行
- 错误处理和监控
- 结果聚合和通知

## 与交易系统集成

### QMT集成方案

通过信号文件实现与QMT等交易终端的集成:

1. **信号生成**: `Core/generate_qmt_signals.py`
2. **文件格式**: CSV/JSON标准化信号
3. **实时同步**: 共享目录文件监控
4. **风险控制**: 信号验证和过滤

### 信号文件示例

```csv
asset_code,signal,timestamp,target_price,confidence,source
sh.000300,BUY,2025-01-15T10:30:00Z,3500.0,0.85,bt_macro_v1
GC=F,SELL,2025-01-15T10:30:00Z,2050.0,0.78,bt_macro_v1
```

## 开发路线图

### 已完成功能 ✅

- [x] 多数据源集成和管理
- [x] 技术分析引擎和指标计算
- [x] 可视化图表生成
- [x] 媒体观点聚合和情感分析
- [x] 自动化调度系统
- [x] 数据质量验证
- [x] PostgreSQL数据中心

### 开发中功能 🔄

- [ ] 跨资产相关性分析
- [ ] 市场状态识别模型
- [ ] 投资组合风险管理
- [ ] 策略参数优化
- [ ] 实时数据流处理

### 规划功能 📋

- [ ] AI投资助理集成
- [ ] 另类数据源接入
- [ ] 高频数据支持
- [ ] 移动端应用
- [ ] 云端部署方案

## 项目结构

```
intelligent-investment-analysis/
├── Core/                          # 核心模块
│   ├── DB/                        # 数据库工具
│   ├── bt_write_macro_data.py     # 数据获取
│   ├── bt_macro_tech_analysis.py  # 技术分析
│   ├── bt_data_validator.py       # 数据验证
│   └── macro_config.py            # 配置文件
├── bt_plot_tech_analysis.py       # 图表生成
├── bt_benchmark_get.py            # 媒体观点
├── bt_portfolio_get.py            # 投资组合
├── bt_test_run.py                 # 回测引擎
├── scheduler.py                   # 调度器
├── plot_html/                     # 输出目录
├── requirements.txt               # 依赖列表
├── .env.example                   # 配置模板
└── README.md                      # 项目文档
```

## 贡献指南

欢迎贡献代码和建议！请遵循以下步骤:

1. Fork项目仓库
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送分支 (`git push origin feature/AmazingFeature`)
5. 创建Pull Request

### 代码规范

- 遵循PEP 8 Python代码规范
- 添加适当的注释和文档字符串
- 编写单元测试
- 更新相关文档

## 许可证

本项目采用MIT许可证 - 详见 [LICENSE](LICENSE) 文件

## 联系方式

- 项目主页: https://github.com/your-username/intelligent-investment-analysis
- 问题反馈: https://github.com/your-username/intelligent-investment-analysis/issues
- 邮箱: your-email@example.com

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。投资有风险，决策需谨慎。使用本系统进行投资决策的风险由用户自行承担。

---

**版本**: v0.2.0  
**最后更新**: 2025-01-15  
**Python版本**: 3.8+  
**数据库**: PostgreSQL 12+