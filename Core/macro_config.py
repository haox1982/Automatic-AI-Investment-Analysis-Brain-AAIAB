#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
宏观数据配置文件
包含所有支持的宏观经济数据源配置
"""

# --- 扩展的宏观资产配置 ---
MACRO_ASSETS_CONFIG = [
    # A股指数 (akshare)
    {'name': '沪深300', 'code': 'sh000300', 'source': 'ak_index', 'type': 'INDEX'},
    {'name': '上证指数', 'code': 'sh000001', 'source': 'ak_index', 'type': 'INDEX', 'description': '上海证券交易所综合股价指数'},
    {'name': '深证成指', 'code': 'sz399001', 'source': 'ak_index', 'type': 'INDEX', 'description': '深圳证券交易所成份股价指数'},
    {'name': '中证500', 'code': 'sh000905', 'source': 'ak_index', 'type': 'INDEX', 'description': '中证500指数'},
    
    # 全球商品与指数 (yfinance)
    {
        'name': '标普500指数',
        'code': '^GSPC',
        'source': 'yfinance',
        'type': 'INDEX',
        'description': '标准普尔500指数'
    },
    {
        'name': '纳斯达克指数',
        'code': '^IXIC',
        'source': 'yfinance',
        'type': 'INDEX',
        'description': '纳斯达克综合指数'
    },
    {
        'name': '道琼斯指数',
        'code': '^DJI',
        'source': 'yfinance',
        'type': 'INDEX',
        'description': '道琼斯工业平均指数'
    },
    {
        'name': '黄金期货',
        'code': 'GC=F',
        'source': 'yfinance',
        'type': 'COMMODITY',
        'description': 'COMEX黄金期货'
    },
    {
        'name': '原油期货',
        'code': 'CL=F',
        'source': 'yfinance',
        'type': 'COMMODITY',
        'description': 'WTI原油期货'
    },
    {
        'name': '比特币',
        'code': 'BTC-USD',
        'source': 'yfinance',
        'type': 'CRYPTO',
        'description': '比特币对美元汇率'
    },
    {'name': '白银', 'code': 'SI=F', 'source': 'yfinance', 'type': 'COMMODITY'},  # 新增白银
    {'name': '美元指数', 'code': 'DX-Y.NYB', 'source': 'yfinance', 'type': 'CURRENCY'},

    # 汇率数据
    {
        'name': '美元兑人民币',
        'code': 'USDCNY',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '美元对人民币汇率（中国银行牌价）'
    },
    {
        'name': '欧元兑人民币',
        'code': 'EURCNY',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '欧元对人民币汇率（中国银行牌价）'
    },
    {
        'name': '英镑兑人民币',
        'code': 'GBPCNY',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '英镑对人民币汇率（中国银行牌价）'
    },
    {
        'name': '日元兑人民币',
        'code': 'JPYCNY',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '日元对人民币汇率（中国银行牌价）'
    },
    # 国际货币对 (yfinance)
    {
        'name': '欧元兑美元',
        'code': 'EURUSD',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '欧元对美元汇率（国际市场）'
    },
    {
        'name': '英镑兑美元',
        'code': 'GBPUSD',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '英镑对美元汇率（国际市场）'
    },
    {
        'name': '美元兑日元',
        'code': 'USDJPY',
        'source': 'ak_forex',
        'type': 'CURRENCY',
        'description': '美元对日元汇率（国际市场）'
    },

    # 利率数据
    {'name': '美国短期利率', 'code': '^IRX', 'source': 'yfinance', 'type': 'INTEREST_RATE'},
    {'name': '美联储基准利率', 'code': 'macro_usa_federal_funds_rate', 'source': 'ak_macro', 'type': 'INTEREST_RATE', 'plot': False},  # 新增美联储利率
    {'name': '中国银行间同业拆借利率', 'code': 'macro_china_shibor', 'source': 'ak_macro', 'type': 'INTEREST_RATE', 'plot': False},  # 新增中国银行利率
    {
        'name': '欧洲央行利率',
        'code': 'macro_bank_euro_interest_rate',
        'source': 'ak_macro',
        'type': 'INTEREST_RATE',
        'description': '欧洲央行基准利率'
    },
    # 注意：akshare中暂无英国央行和日本央行利率函数
    # 可考虑使用yfinance获取国债收益率作为替代指标，但需要验证代码有效性
    # {
    #     'name': '英国10年期国债收益率',
    #     'code': '^TNX',  # 需要验证
    #     'source': 'yfinance',
    #     'type': 'INTEREST_RATE',
    #     'description': '英国10年期国债收益率（央行利率替代指标）'
    # },
    # {
    #     'name': '日本10年期国债收益率',
    #     'code': '^TNX',  # 需要验证
    #     'source': 'yfinance',
    #     'type': 'INTEREST_RATE',
    #     'description': '日本10年期国债收益率（央行利率替代指标）'
    # },
    {'name': '中国贷款市场报价利率', 'code': 'macro_china_lpr', 'source': 'ak_macro', 'type': 'INTEREST_RATE', 'plot': False},

    # 经济指标 - CPI
    {'name': '中国CPI月率', 'code': 'macro_china_cpi_monthly', 'source': 'ak_macro', 'type': 'CPI', 'plot': False},
    {'name': '美国CPI月率', 'code': 'macro_usa_cpi_monthly', 'source': 'ak_macro', 'type': 'CPI', 'plot': False},
    
    # 经济指标 - PPI (新增)
    {'name': '中国PPI月率', 'code': 'macro_china_ppi', 'source': 'ak_macro', 'type': 'PPI', 'plot': False},
    {'name': '美国PPI月率', 'code': 'macro_usa_ppi', 'source': 'ak_macro', 'type': 'PPI', 'plot': False},
    
    # 经济指标 - GDP (新增)
    {'name': '中国GDP季率', 'code': 'macro_china_gdp', 'source': 'ak_macro', 'type': 'GDP', 'plot': False},
    # {'name': '美国GDP季率', 'code': 'macro_usa_gdp', 'source': 'ak_macro', 'type': 'GDP', 'plot': False},  # akshare中暂无此函数
    
    # 经济指标 - 货币供应量 (新增)
    {'name': '中国货币供应量', 'code': 'macro_china_money_supply', 'source': 'ak_macro', 'type': 'MONEY_SUPPLY', 'plot': False},
    
    # 黄金储备相关数据
    {
        'name': '黄金ETF-GLD',
        'code': 'GLD',
        'source': 'yfinance',
        'type': 'COMMODITY',
        'description': 'SPDR Gold Shares ETF'
    },
    {
        'name': '黄金ETF-IAU',
        'code': 'IAU',
        'source': 'yfinance',
        'type': 'COMMODITY',
        'description': 'iShares Gold Trust ETF'
    },
    {
        'name': '全球黄金ETF持仓报告',
        'code': 'macro_cons_gold',
        'source': 'ak_macro',
        'type': 'COMMODITY',
        'description': '全球黄金ETF持仓数据'
    }
]

# 数据源映射到具体的获取函数
AK_MACRO_FUNCTION_MAP = {
    'macro_china_cpi_monthly': 'macro_china_cpi_monthly',
    'macro_usa_cpi_monthly': 'macro_usa_cpi_monthly', 
    'macro_china_lpr': 'macro_china_lpr',
    'macro_bank_euro_interest_rate': 'macro_bank_euro_interest_rate',  # 欧洲央行利率
    'macro_usa_federal_funds_rate': 'macro_bank_usa_interest_rate',  # 正确的函数名
    'macro_china_shibor': 'rate_interbank',  # 银行间拆借利率
    'macro_china_ppi': 'macro_china_ppi_yearly',  # 正确的函数名
    'macro_usa_ppi': 'macro_usa_ppi',  # 需要确认是否存在
    'macro_china_gdp': 'macro_china_gdp_yearly',  # 正确的函数名
    # 'macro_usa_gdp': 'macro_usa_gdp_yearly',  # akshare中暂无此函数
    'macro_cons_silver': 'macro_cons_silver',  # 白银ETF持仓报告
    'macro_china_money_supply': 'macro_china_money_supply',  # 货币供应量(包含M0、M1、M2)
    'macro_cons_gold': 'macro_cons_gold',  # 全球最大黄金ETF持仓报告
}

# 数据验证规则
DATA_VALIDATION_RULES = {
    'yfinance': {
        'required_columns': ['Open', 'High', 'Low', 'Close', 'Volume'],
        'min_rows': 1,
        'date_column': 'Date'
    },
    'ak_index': {
        'required_columns': ['open', 'high', 'low', 'close', 'volume'],
        'min_rows': 1,
        'date_column': 'date'
    },
    'ak_forex': {
        'required_columns': ['open', 'high', 'low', 'close'],
        'min_rows': 1,
        'date_column': 'date'
    },
    'ak_macro': {
        'min_rows': 1,
        'required_numeric_columns': 1  # 至少有一个数值列
    }
}