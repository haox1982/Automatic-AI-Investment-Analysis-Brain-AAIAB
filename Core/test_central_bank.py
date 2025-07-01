#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from dotenv import load_dotenv

# 添加路径以导入数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from DB.db_utils import get_db_connection
from bt_plot_tech_analysis import TechnicalAnalysisPlotter

load_dotenv()

def test_central_bank_chart():
    print("开始测试央行利率图表生成...")
    
    # 直接测试数据库查询
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 测试查询
    from datetime import datetime, timedelta
    
    # 测试央行利率图表生成
    print("\n开始测试央行利率图表生成...")
    
    try:
        from bt_plot_tech_analysis import TechnicalAnalysisPlotter
        plotter = TechnicalAnalysisPlotter()
        
        # 直接调用生成函数
        result = plotter.generate_central_bank_rates_chart()
        print(f"图表生成结果: {result}")
        
    except Exception as e:
        print(f"图表生成失败: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    print("测试完成")

if __name__ == "__main__":
    test_central_bank_chart()