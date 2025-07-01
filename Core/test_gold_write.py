#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bt_write_macro_data import MacroDataWriter
from macro_config import MACRO_ASSETS_CONFIG
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_gold_write():
    """测试上海金数据写入"""
    writer = MacroDataWriter()
    
    # 找到上海金配置
    gold_configs = []
    for config in MACRO_ASSETS_CONFIG:
        if config.get('source') == 'ak_gold_spot':
            gold_configs.append(config)
    
    print(f"找到 {len(gold_configs)} 个上海金配置")
    
    for config in gold_configs:
        print(f"\n=== 测试写入 {config['name']} ===")
        try:
            # 测试数据获取
            success, message, data = writer.get_ak_gold_spot_data(config, incremental=False)
            print(f"数据获取结果: success={success}, message={message}")
            
            if success and data is not None:
                print(f"获取到数据行数: {len(data)}")
                print(f"数据列名: {list(data.columns)}")
                
                # 测试数据处理和保存
                new_count, updated_count = writer.process_and_save_data(config, data, incremental=False)
                print(f"保存结果: 新增{new_count}条, 更新{updated_count}条")
            else:
                print("数据获取失败")
                
        except Exception as e:
            print(f"错误: {e}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")

if __name__ == "__main__":
    test_gold_write()