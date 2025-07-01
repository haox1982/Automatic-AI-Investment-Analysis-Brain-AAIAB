#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import akshare as ak
import pandas as pd

print("测试akshare央行利率数据获取...")

try:
    print("\n1. 测试美联储利率:")
    df_usa = ak.macro_bank_usa_interest_rate()
    print(f"数据形状: {df_usa.shape}")
    print(f"列名: {df_usa.columns.tolist()}")
    print(f"数据类型: {df_usa.dtypes}")
    if not df_usa.empty:
        print("前5行:")
        print(df_usa.head())
        print(f"\n最新数据: {df_usa.iloc[-1]}")
        print(f"\n是否有日期列: {'日期' in df_usa.columns}")
        print(f"是否有今值列: {'今值' in df_usa.columns}")
    else:
        print("无数据")
except Exception as e:
    print(f"美联储利率获取失败: {e}")

try:
    print("\n2. 测试欧洲央行利率:")
    df_euro = ak.macro_bank_euro_interest_rate()
    print(f"数据形状: {df_euro.shape}")
    if not df_euro.empty:
        print(df_euro.head())
        print(f"最新数据: {df_euro.iloc[-1]}")
    else:
        print("无数据")
except Exception as e:
    print(f"欧洲央行利率获取失败: {e}")

try:
    print("\n3. 测试瑞士央行利率:")
    df_swiss = ak.macro_bank_switzerland_interest_rate()
    print(f"数据形状: {df_swiss.shape}")
    if not df_swiss.empty:
        print(df_swiss.head())
        print(f"最新数据: {df_swiss.iloc[-1]}")
    else:
        print("无数据")
except Exception as e:
    print(f"瑞士央行利率获取失败: {e}")

print("\n测试完成")