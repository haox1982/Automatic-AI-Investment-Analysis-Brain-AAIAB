#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import akshare as ak
import pandas as pd
import traceback

def test_gold_spot():
    """测试上海金现货数据获取"""
    symbols = ['Au99.99', 'Au100g', 'Au(T+D)']
    
    for symbol in symbols:
        print(f"\n=== 测试 {symbol} ===")
        try:
            data = ak.spot_hist_sge(symbol=symbol)
            if data is not None and not data.empty:
                print(f"数据行数: {len(data)}")
                print(f"列名: {list(data.columns)}")
                print(f"前3行数据:")
                print(data.head(3))
                print(f"最新日期: {data['date'].max() if 'date' in data.columns else '无date列'}")
            else:
                print("数据为空")
        except Exception as e:
            print(f"错误: {e}")
            print(f"详细错误: {traceback.format_exc()}")

if __name__ == "__main__":
    test_gold_spot()