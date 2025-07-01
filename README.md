# Intelligent Investment Analysis System

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-v0.2.0-orange.svg)](https://github.com/your-username/intelligent-investment-analysis/releases)

[中文文档](README_cn.md) | English

## Overview

An intelligent investment analysis system built with Python and PostgreSQL, focusing on macroeconomic data analysis, technical indicator calculations, and investment decision support. The system provides comprehensive market insights through automated data acquisition, multi-dimensional technical analysis, and intelligent report generation.

### Key Features

- 🔄 **Automated Data Acquisition**: Multi-source data integration (yfinance, akshare) with incremental and full updates
- 📊 **Multi-Dimensional Technical Analysis**: 30+ technical indicators including MA, MACD, RSI, Bollinger Bands
- 🎯 **Intelligent Scoring System**: Investment recommendations based on multi-indicator composite scoring
- 📈 **Interactive Visualizations**: Auto-generated interactive HTML technical analysis charts
- 🤖 **Media Sentiment Aggregation**: FinBERT-based financial news sentiment analysis
- 📱 **Automated Scheduling**: Timed execution and notifications via n8n workflows
- 💾 **Data Center**: PostgreSQL database for historical data and analysis results storage

## System Architecture

### Core Components

```
Intelligent Investment Analysis System
├── Data Acquisition Layer
│   ├── bt_write_macro_data.py     # Macro data acquisition
│   ├── bt_portfolio_get.py        # Portfolio tracking
│   └── bt_benchmark_get.py        # Media sentiment aggregation
├── Analysis Engine
│   ├── bt_macro_tech_analysis.py  # Technical analysis engine
│   ├── bt_plot_tech_analysis.py   # Chart generation engine
│   └── bt_test_run.py             # Backtesting engine
├── Data Center
│   ├── PostgreSQL Database        # Historical data storage
│   └── Core/DB/                   # Database utilities
├── Scheduler System
│   ├── scheduler.py               # Automated scheduling
│   └── n8n Workflows              # Visual orchestration
└── Output System
    ├── plot_html/                 # HTML charts
    ├── Text Reports               # Analysis reports
    └── Telegram Notifications     # Real-time alerts
```

### Technology Stack

- **Data Analysis**: Python, Backtrader, Pandas, TA-Lib
- **Data Sources**: yfinance, akshare
- **Database**: PostgreSQL
- **Visualization**: Plotly, HTML/CSS/JavaScript
- **Workflow**: n8n
- **NLP**: FinBERT, Chinese keyword analysis
- **Notifications**: Telegram Bot

## Data Coverage

### Asset Classes

| Category | Coverage | Data Source | Update Frequency |
|----------|----------|-------------|------------------|
| **Equity Indices** | Shanghai, Shenzhen, CSI300, CSI500, S&P500, NASDAQ, Dow Jones | yfinance | Daily |
| **Foreign Exchange** | USD Index, Major currency pairs, CNY rates | yfinance | Daily |
| **Commodities** | Gold futures, Crude oil futures, Silver futures | yfinance | Daily |
| **Precious Metals** | Shanghai Gold (Au99.99/Au100g/Au(T+D)), China's gold reserves | akshare | Daily/Monthly |
| **Interest Rates** | Fed rates, China LPR, SHIBOR, ECB rates | akshare | Real-time |
| **Macro Indicators** | CPI, PPI, GDP, Money supply | akshare | Monthly/Quarterly |
| **Digital Assets** | Bitcoin, Ethereum, Major cryptocurrencies | yfinance | Daily |

### Data Quality

- **Historical Depth**: 20+ years of historical data for core assets
- **Data Integrity**: Automated data validation and quality checks
- **Update Mechanism**: Incremental updates + full backups
- **Conflict Resolution**: Smart deduplication and data override strategies

## Technical Analysis Features

### Technical Indicators

- **Trend Indicators**: MA(5,10,20,60), EMA, MACD, ADX
- **Momentum Indicators**: RSI, Stochastic, Williams %R
- **Volatility Indicators**: Bollinger Bands, ATR, Standard Deviation
- **Volume Indicators**: OBV, Volume MA, Price-Volume Divergence
- **Support/Resistance**: Key level identification, Breakout signals

### Intelligent Scoring

The system calculates comprehensive scores (0-10) based on multi-dimensional technical indicators:

- **Trend Strength** (30%): Based on MA alignment and MACD
- **Momentum Status** (25%): Based on RSI and Stochastic
- **Volatility** (20%): Based on Bollinger Bands and ATR
- **Volume Confirmation** (15%): Based on OBV and Volume
- **Technical Patterns** (10%): Based on key level breakouts

### Chart Features

- **Interactive Candlestick Charts**: Support zoom, hover, indicator switching
- **Multiple Timeframes**: Daily, weekly, monthly analysis
- **Technical Indicator Overlay**: Customizable indicator combinations
- **Key Level Annotations**: Auto-identification of support/resistance
- **Signal Alerts**: Buy/sell signal visualization

## Media Sentiment Aggregation

### Supported Media Sources

**English Media**:
- Yahoo Finance, Reuters, Bloomberg, MarketWatch

**Chinese Media**:
- Sina Finance, East Money, JRJ, Securities Times

### Sentiment Analysis

- **English**: Professional financial sentiment analysis based on FinBERT
- **Chinese**: Keyword and rule-based sentiment recognition
- **Multi-Dimensional**: Aggregation by asset class and time dimension
- **Trend Identification**: Sentiment change trends and turning point identification

## Installation & Configuration

### Requirements

```bash
# Python 3.8+
# PostgreSQL 12+
# Docker (optional)
```

### Installation Steps

1. **Clone Repository**
```bash
git clone https://github.com/your-username/intelligent-investment-analysis.git
cd intelligent-investment-analysis
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure Database**
```bash
# Create PostgreSQL database
createdb investment_analysis

# Configure environment variables
cp .env.example .env
# Edit .env file with your database connection details
```

4. **Initialize Data**
```bash
# First run - get full data
python3 Core/bt_write_macro_data.py --full

# Validate data
python3 Core/bt_data_validator.py
```

### Configuration File

Configure the following parameters in `.env` file:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=investment_analysis
DB_USER=your_username
DB_PASSWORD=your_password

# Telegram Configuration (optional)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Data Source Configuration
YFINANCE_TIMEOUT=30
AKSHARE_TIMEOUT=30
```

## Usage Guide

### Daily Usage

```bash
# 1. Update data
python3 Core/bt_write_macro_data.py

# 2. Generate technical analysis
python3 Core/bt_macro_tech_analysis.py

# 3. Generate charts
python3 bt_plot_tech_analysis.py --all

# 4. Get media sentiment
python3 bt_benchmark_get.py

# 5. Automated scheduling
python3 scheduler.py
```

### Advanced Features

```bash
# Concurrent data acquisition
python3 Core/bt_write_macro_data.py --full --workers 5

# Specific asset analysis
python3 bt_plot_tech_analysis.py --symbols "^GSPC,GC=F"

# Custom time range
python3 bt_benchmark_get.py --days 30

# Strategy backtesting
python3 bt_test_run.py
```

### Output Files

- **HTML Charts**: Interactive charts in `plot_html/` directory
- **Analysis Reports**: `macro_technical_analysis_YYYYMMDD.txt`
- **Portfolio Reports**: `portfolio_tracking_report_YYYYMMDD.md`
- **Index Page**: `plot_html/index.html` unified navigation

## Automated Scheduling

### scheduler.py

Built-in scheduler supports:
- Weekday automatic data updates
- Timed technical analysis generation
- Exception handling and retry mechanisms
- Telegram notification push

### n8n Workflows

Visual workflow orchestration:
- Main workflow timed scheduling
- Sub-workflow modular execution
- Error handling and monitoring
- Result aggregation and notifications

## Trading System Integration

### QMT Integration Solution

Integration with QMT and other trading terminals through signal files:

1. **Signal Generation**: `Core/generate_qmt_signals.py`
2. **File Format**: Standardized CSV/JSON signals
3. **Real-time Sync**: Shared directory file monitoring
4. **Risk Control**: Signal validation and filtering

### Signal File Example

```csv
asset_code,signal,timestamp,target_price,confidence,source
sh.000300,BUY,2025-01-15T10:30:00Z,3500.0,0.85,bt_macro_v1
GC=F,SELL,2025-01-15T10:30:00Z,2050.0,0.78,bt_macro_v1
```

## Development Roadmap

### Completed Features ✅

- [x] Multi-source data integration and management
- [x] Technical analysis engine and indicator calculations
- [x] Visualization chart generation
- [x] Media sentiment aggregation and analysis
- [x] Automated scheduling system
- [x] Data quality validation
- [x] PostgreSQL data center

### In Development 🔄

- [ ] Cross-asset correlation analysis
- [ ] Market regime identification model
- [ ] Portfolio risk management
- [ ] Strategy parameter optimization
- [ ] Real-time data streaming

### Planned Features 📋

- [ ] AI investment assistant integration
- [ ] Alternative data source integration
- [ ] High-frequency data support
- [ ] Mobile application
- [ ] Cloud deployment solution

## Project Structure

```
intelligent-investment-analysis/
├── Core/                          # Core modules
│   ├── DB/                        # Database utilities
│   ├── bt_write_macro_data.py     # Data acquisition
│   ├── bt_macro_tech_analysis.py  # Technical analysis
│   ├── bt_data_validator.py       # Data validation
│   └── macro_config.py            # Configuration
├── bt_plot_tech_analysis.py       # Chart generation
├── bt_benchmark_get.py            # Media sentiment
├── bt_portfolio_get.py            # Portfolio tracking
├── bt_test_run.py                 # Backtesting engine
├── scheduler.py                   # Scheduler
├── plot_html/                     # Output directory
├── requirements.txt               # Dependencies
├── .env.example                   # Configuration template
└── README.md                      # Project documentation
```

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Create a Pull Request

### Code Standards

- Follow PEP 8 Python coding standards
- Add appropriate comments and docstrings
- Write unit tests
- Update relevant documentation

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Contact

- Project Homepage: https://github.com/your-username/intelligent-investment-analysis
- Issue Tracker: https://github.com/your-username/intelligent-investment-analysis/issues
- Email: your-email@example.com

## Disclaimer

This system is for educational and research purposes only and does not constitute investment advice. Investment involves risks, and decisions should be made carefully. Users bear the risk of using this system for investment decisions.

---

**Version**: v0.2.0  
**Last Updated**: 2025-01-15  
**Python Version**: 3.8+  
**Database**: PostgreSQL 12+