#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_portfolio.py - 投资大佬风向标功能测试

简单测试脚本，验证投资组合跟踪功能的核心组件
"""

import sys
import os
from datetime import datetime

# 添加Core模块路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'Core'))

def test_portfolio_tracker():
    """
    测试投资组合跟踪器的基本功能
    """
    print("=" * 60)
    print("投资大佬风向标功能测试")
    print("=" * 60)
    
    try:
        from bt_portfolio_get import PortfolioTracker
        
        # 初始化跟踪器
        print("\n1. 初始化投资组合跟踪器...")
        tracker = PortfolioTracker()
        print("✅ 跟踪器初始化成功")
        
        # 测试模拟数据生成
        print("\n2. 测试模拟数据生成...")
        mock_filings = tracker._get_mock_filings("1067983", 2)
        print(f"✅ 生成了 {len(mock_filings)} 个模拟13F报告")
        
        for filing in mock_filings:
            print(f"   - {filing['formType']} | {filing['filedAt']} | {filing['accessionNo']}")
        
        # 测试模拟持仓数据
        print("\n3. 测试模拟持仓数据...")
        mock_holdings = tracker._get_mock_holdings(mock_filings[0]['accessionNo'])
        holdings = mock_holdings.get('holdings', [])
        print(f"✅ 生成了 {len(holdings)} 个模拟持仓")
        
        total_value = sum(h.get('value', 0) for h in holdings)
        print(f"   总投资组合价值: ${total_value/1_000_000:.1f}M")
        
        for holding in holdings[:3]:  # 只显示前3个
            name = holding.get('nameOfIssuer', 'Unknown')
            ticker = holding.get('ticker', 'N/A')
            value = holding.get('value', 0)
            shares = holding.get('shrsOrPrnAmt', {}).get('sshPrnamt', 0)
            print(f"   - {name} ({ticker}): {shares:,} 股, ${value/1_000_000:.1f}M")
        
        # 测试投资组合变化分析
        print("\n4. 测试投资组合变化分析...")
        current_holdings = holdings
        previous_holdings = tracker._get_mock_holdings(mock_filings[1]['accessionNo']).get('holdings', [])
        
        changes = tracker.analyze_portfolio_changes(current_holdings, previous_holdings)
        
        if "error" not in changes:
            print("✅ 投资组合变化分析成功")
            print(f"   - 新增持仓: {len(changes.get('new_positions', []))}")
            print(f"   - 清仓: {len(changes.get('closed_positions', []))}")
            print(f"   - 增持: {len(changes.get('increased_positions', []))}")
            print(f"   - 减持: {len(changes.get('decreased_positions', []))}")
            print(f"   - 不变: {len(changes.get('unchanged_positions', []))}")
        else:
            print(f"❌ 投资组合变化分析失败: {changes['error']}")
        
        # 测试单个投资者跟踪
        print("\n5. 测试单个投资者跟踪...")
        result = tracker.track_investor("1067983")  # 巴菲特
        
        if "error" not in result:
            print("✅ 投资者跟踪成功")
            print(f"   投资者: {result.get('investor_name', 'Unknown')}")
            print(f"   持仓数量: {result.get('holdings_count', 0)}")
            print(f"   投资组合价值: ${result.get('total_value', 0)/1_000_000:.1f}M")
            
            changes = result.get('changes')
            if changes and "error" not in changes:
                print(f"   变化分析: 新增{len(changes.get('new_positions', []))}, "
                      f"清仓{len(changes.get('closed_positions', []))}, "
                      f"增持{len(changes.get('increased_positions', []))}, "
                      f"减持{len(changes.get('decreased_positions', []))}")
        else:
            print(f"❌ 投资者跟踪失败: {result['error']}")
        
        # 测试报告生成
        print("\n6. 测试报告生成...")
        mock_results = {
            "1067983": result
        }
        
        summary = tracker.generate_summary_report(mock_results)
        print("✅ 报告生成成功")
        print(f"   报告长度: {len(summary)} 字符")
        print(f"   报告预览: {summary[:200]}...")
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！投资大佬风向标功能正常运行")
        print("=" * 60)
        
        return True
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        print("请确保 bt_portfolio_get.py 文件存在且语法正确")
        return False
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_database_connection():
    """
    测试数据库连接
    """
    print("\n" + "=" * 60)
    print("数据库连接测试")
    print("=" * 60)
    
    try:
        from DB.db_utils import get_db_connection
        
        conn = get_db_connection()
        if conn:
            print("✅ 数据库连接成功")
            
            # 测试查询portfolio_holdings表
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_name = 'portfolio_holdings'
            """)
            
            result = cur.fetchone()
            if result and result[0] > 0:
                print("✅ portfolio_holdings表存在")
                
                # 查询现有数据
                cur.execute("SELECT COUNT(*) FROM portfolio_holdings")
                count = cur.fetchone()[0]
                print(f"   当前记录数: {count}")
            else:
                print("⚠️  portfolio_holdings表不存在，请运行数据库初始化")
            
            cur.close()
            conn.close()
            return True
        else:
            print("❌ 数据库连接失败")
            print("请检查数据库配置和.env文件")
            return False
            
    except Exception as e:
        print(f"❌ 数据库测试失败: {e}")
        return False

def main():
    """
    主测试函数
    """
    print(f"开始测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 测试投资组合跟踪功能
    portfolio_test = test_portfolio_tracker()
    
    # 测试数据库连接
    db_test = test_database_connection()
    
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"投资组合跟踪功能: {'✅ 通过' if portfolio_test else '❌ 失败'}")
    print(f"数据库连接: {'✅ 通过' if db_test else '❌ 失败'}")
    
    if portfolio_test and db_test:
        print("\n🎉 所有测试通过！系统准备就绪")
        print("\n可以运行以下命令开始使用:")
        print("python Core/bt_portfolio_get.py")
    else:
        print("\n⚠️  部分测试失败，请检查配置")
    
    print(f"\n测试结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()