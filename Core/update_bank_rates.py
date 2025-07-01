#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
sys.path.append('.')

from bt_write_macro_data import MacroDataWriter
from macro_config import MACRO_ASSETS_CONFIG
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print("开始更新央行利率数据...")

writer = MacroDataWriter()

# 央行利率配置
bank_configs = [
    config for config in MACRO_ASSETS_CONFIG 
    if config['name'] in ['美联储基准利率', '欧洲央行利率', '瑞士央行利率', '英国央行利率', '日本央行利率', '俄罗斯央行利率']
]

for config in bank_configs:
    print(f"\n处理: {config['name']}")
    try:
        # 获取数据
        success, msg, data = writer.get_ak_macro_data(config, incremental=False)
        print(f"获取结果: {success}, {msg}")
        
        if success and data is not None and not data.empty:
            print(f"数据形状: {data.shape}")
            print(f"列名: {data.columns.tolist()}")
            
            # 保存数据
            new_count, updated_count = writer.process_and_save_data(config, data, incremental=False)
            print(f"保存结果: 新增{new_count}条, 更新{updated_count}条")
        else:
            print("无数据或获取失败")
            
    except Exception as e:
        print(f"处理失败: {e}")
        import traceback
        traceback.print_exc()

print("\n央行利率数据更新完成")