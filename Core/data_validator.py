# -*- coding: utf-8 -*-
"""
@Time    : 2025/06/24
@Author  : Trae
@File    : data_validator.py
@Description : 数据验证器
- 每日数据更新后执行，用于保障数据的准确性。
- 通过交叉验证（Cross-Validation）的原则，使用不同的数据源来核对已入库的数据。
"""

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import random

# 加载环境变量
load_dotenv()

# 数据库连接
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def get_recent_data_from_db(num_samples=5):
    """从数据库中随机抽取最近更新的N条数据"""
    query = """
    SELECT symbol, data_date, close_price
    FROM macro_data
    WHERE close_price IS NOT NULL
    ORDER BY updated_at DESC
    LIMIT 100; -- 先取最近的100条，再从中随机抽样
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        if len(df) > num_samples:
            return df.sample(n=num_samples)
        return df
    except Exception as e:
        print(f"从数据库读取数据失败: {e}")
        return pd.DataFrame()

# 符号映射：将数据库中的symbol映射到yfinance的ticker
SYMBOL_MAP = {
    "上证指数": "000001.SS",
    "深证成指": "399001.SZ",
    "沪深300": "000300.SS",
    "创业板指": "399006.SZ",
    "科创50": "000688.SS",
    "道琼斯指数": "^DJI",
    "纳斯达克指数": "^IXIC",
    "标普500指数": "^GSPC",
    "美元指数": "DX-Y.NYB",
    "美元兑人民币": "CNY=X",
    "美元兑日元": "JPY=X",
    "欧元兑美元": "EURUSD=X",
    "英镑兑美元": "GBPUSD=X",
    "欧元兑人民币": "EURCNY=X",
    "黄金期货": "GC=F",
    "白银期货": "SI=F",
    "原油期货": "CL=F",
    "比特币": "BTC-USD",
    "黄金ETF-GLD": "GLD",
    "黄金ETF-IAU": "IAU",
    # ... 其他需要映射的symbol
}

def get_validation_data_from_yfinance(symbol, date):
    """从Yahoo Finance获取特定日期的数据用于验证"""
    try:
        # yfinance需要日期范围，所以我们取日期当天和后一天
        start_date = date
        end_date = date + pd.Timedelta(days=1)
        ticker_symbol = SYMBOL_MAP.get(symbol, symbol)
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
        if not hist.empty:
            return hist['Close'].iloc[0]
    except Exception as e:
        print(f"从yfinance获取 {symbol} (ticker: {ticker_symbol}) 数据失败: {e}")
    return None

def validate_data(tolerance=0.05):
    """
    执行数据验证的核心逻辑
    1. 从数据库抽取样本
    2. 从第三方源获取验证数据
    3. 比对差异
    4. 报告结果
    """
    print("开始执行数据交叉验证...")
    db_samples = get_recent_data_from_db()

    if db_samples.empty:
        print("未能从数据库获取到样本数据，验证中止。")
        return

    validation_results = []

    for index, row in db_samples.iterrows():
        symbol = row['symbol']
        db_date = row['data_date']
        db_price = row['close_price']

        # 这里可以根据symbol选择不同的验证源，当前统一使用yfinance
        # 注意：yfinance的symbol和我们系统内的可能不同，需要转换
        # 例如，A股需要加后缀 .SS 或 .SZ
        validation_price = get_validation_data_from_yfinance(symbol, db_date)

        if validation_price is not None:
            diff = abs(db_price - validation_price) / db_price if db_price != 0 else 0
            status = '✅ 通过' if diff <= tolerance else '❌ 失败'
            result = {
                "symbol": symbol,
                "date": db_date.strftime('%Y-%m-%d'),
                "db_price": db_price,
                "validation_price": validation_price,
                "difference": f"{diff:.2%}",
                "status": status
            }
            validation_results.append(result)
        else:
            result = {
                "symbol": symbol,
                "date": db_date.strftime('%Y-%m-%d'),
                "db_price": db_price,
                "validation_price": "获取失败",
                "difference": "N/A",
                "status": "⚠️ 无法验证"
            }
            validation_results.append(result)

    print("\n--- 数据验证报告 ---")
    report_df = pd.DataFrame(validation_results)
    print(report_df.to_string())
    
    # 生成并打印摘要
    summary = report_df['status'].value_counts()
    print("\n--- 验证摘要 ---")
    print(summary.to_string())
    print("------------------\n")

if __name__ == "__main__":
    validate_data()