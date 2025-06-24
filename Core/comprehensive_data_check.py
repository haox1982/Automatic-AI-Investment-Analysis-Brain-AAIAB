#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观数据完整性和质量检查脚本
全面检查macro_data表中的数据质量、完整性和准确性
"""

import datetime
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams

# 相对路径导入我们的数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
from DB.db_utils import get_db_connection
from macro_config import MACRO_ASSETS_CONFIG

# 加载环境变量
load_dotenv()

# 配置中文字体
rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
rcParams['axes.unicode_minus'] = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('comprehensive_data_check.log')
    ]
)
logger = logging.getLogger(__name__)

class MacroDataChecker:
    """宏观数据完整性检查器"""
    
    def __init__(self):
        self.conn = get_db_connection()
        self.issues = []
        self.summary_stats = {}
        self.data_quality_report = {}
        
    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def get_table_overview(self) -> Dict:
        """获取表的基本概览"""
        try:
            cursor = self.conn.cursor()
            
            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM macro_data")
            total_records = cursor.fetchone()[0]
            
            # 数据类型分布
            cursor.execute("""
                SELECT data_type, COUNT(*) as count 
                FROM macro_data 
                GROUP BY data_type 
                ORDER BY count DESC
            """)
            type_distribution = dict(cursor.fetchall())
            
            # 数据源分布
            cursor.execute("""
                SELECT source, COUNT(*) as count 
                FROM macro_data 
                GROUP BY source 
                ORDER BY count DESC
            """)
            source_distribution = dict(cursor.fetchall())
            
            # 日期范围
            cursor.execute("""
                SELECT MIN(data_date) as earliest, MAX(data_date) as latest 
                FROM macro_data
            """)
            date_range = cursor.fetchone()
            
            # 资产数量
            cursor.execute("""
                SELECT COUNT(DISTINCT symbol) as unique_assets 
                FROM macro_data
            """)
            unique_assets = cursor.fetchone()[0]
            
            cursor.close()
            
            overview = {
                'total_records': total_records,
                'unique_assets': unique_assets,
                'date_range': {
                    'earliest': date_range[0],
                    'latest': date_range[1]
                },
                'type_distribution': type_distribution,
                'source_distribution': source_distribution
            }
            
            logger.info(f"数据库概览: {total_records}条记录, {unique_assets}个资产")
            return overview
            
        except Exception as e:
            logger.error(f"获取表概览失败: {str(e)}")
            return {}
    
    def check_data_completeness(self) -> Dict:
        """检查数据完整性"""
        try:
            cursor = self.conn.cursor()
            
            # 检查每个资产的数据完整性
            cursor.execute("""
                SELECT 
                    symbol,
                    data_type,
                    source,
                    COUNT(*) as record_count,
                    MIN(data_date) as earliest_date,
                    MAX(data_date) as latest_date,
                    COUNT(DISTINCT data_date) as unique_dates
                FROM macro_data 
                GROUP BY symbol, data_type, source
                ORDER BY symbol, data_type
            """)
            
            completeness_data = cursor.fetchall()
            cursor.close()
            
            completeness_report = []
            three_years_ago = datetime.datetime.now() - datetime.timedelta(days=3*365)
            
            for row in completeness_data:
                symbol, data_type, source, count, earliest, latest, unique_dates = row
                
                # 检查数据是否覆盖近3年
                covers_3_years = earliest <= three_years_ago
                is_recent = latest >= (datetime.datetime.now() - datetime.timedelta(days=30))
                
                # 计算数据密度（实际数据点/理论数据点）
                if earliest and latest:
                    total_days = (latest - earliest).days + 1
                    data_density = unique_dates / total_days if total_days > 0 else 0
                else:
                    data_density = 0
                
                asset_report = {
                    'symbol': symbol,
                    'data_type': data_type,
                    'source': source,
                    'record_count': count,
                    'unique_dates': unique_dates,
                    'earliest_date': earliest,
                    'latest_date': latest,
                    'covers_3_years': covers_3_years,
                    'is_recent': is_recent,
                    'data_density': round(data_density, 4)
                }
                
                # 标记问题
                issues = []
                if not covers_3_years:
                    issues.append("数据未覆盖3年")
                if not is_recent:
                    issues.append("数据不够新")
                if data_density < 0.1:  # 数据密度过低
                    issues.append("数据稀疏")
                if count < 100:  # 数据点过少
                    issues.append("数据点不足")
                
                asset_report['issues'] = issues
                completeness_report.append(asset_report)
                
                if issues:
                    self.issues.extend([f"{symbol}: {issue}" for issue in issues])
            
            logger.info(f"完整性检查完成: 检查了{len(completeness_report)}个数据集")
            return {'assets': completeness_report}
            
        except Exception as e:
            logger.error(f"数据完整性检查失败: {str(e)}")
            return {}
    
    def check_data_quality(self) -> Dict:
        """检查数据质量"""
        try:
            cursor = self.conn.cursor()
            
            # 检查空值和异常值
            cursor.execute("""
                SELECT 
                    symbol,
                    data_type,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN data_json IS NULL OR data_json = '' THEN 1 END) as null_data,
                    COUNT(CASE WHEN data_date IS NULL THEN 1 END) as null_dates
                FROM macro_data 
                GROUP BY symbol, data_type
            """)
            
            quality_data = cursor.fetchall()
            
            quality_report = []
            for row in quality_data:
                symbol, data_type, total, null_data, null_dates = row
                
                # 计算质量指标
                data_completeness = (total - null_data) / total if total > 0 else 0
                date_completeness = (total - null_dates) / total if total > 0 else 0
                
                quality_issues = []
                if data_completeness < 0.95:
                    quality_issues.append(f"数据缺失率{(1-data_completeness)*100:.1f}%")
                if date_completeness < 1.0:
                    quality_issues.append(f"日期缺失率{(1-date_completeness)*100:.1f}%")
                
                quality_report.append({
                    'symbol': symbol,
                    'data_type': data_type,
                    'total_records': total,
                    'data_completeness': round(data_completeness, 4),
                    'date_completeness': round(date_completeness, 4),
                    'quality_issues': quality_issues
                })
                
                if quality_issues:
                    self.issues.extend([f"{symbol}: {issue}" for issue in quality_issues])
            
            cursor.close()
            logger.info(f"数据质量检查完成: 检查了{len(quality_report)}个数据集")
            return {'quality_metrics': quality_report}
            
        except Exception as e:
            logger.error(f"数据质量检查失败: {str(e)}")
            return {}
    
    def check_price_data_validity(self) -> Dict:
        """检查价格数据的有效性"""
        try:
            cursor = self.conn.cursor()
            
            # 获取包含价格数据的记录
            cursor.execute("""
                SELECT symbol, data_type, data_json, data_date
                FROM macro_data 
                WHERE data_json IS NOT NULL 
                AND data_json != ''
                AND (data_type IN ('INDEX', 'CURRENCY', 'COMMODITY', 'CRYPTO'))
                ORDER BY symbol, data_date
            """)
            
            price_data = cursor.fetchall()
            cursor.close()
            
            price_validity_report = defaultdict(list)
            
            for symbol, data_type, data_json_str, data_date in price_data:
                try:
                    data_json = json.loads(data_json_str)
                    
                    # 检查价格字段
                    price_fields = ['close', 'open', 'high', 'low', 'price', 'value']
                    price_value = None
                    
                    for field in price_fields:
                        if field in data_json and data_json[field] is not None:
                            price_value = float(data_json[field])
                            break
                    
                    if price_value is None:
                        price_validity_report[symbol].append({
                            'date': data_date,
                            'issue': '无价格数据',
                            'data': data_json
                        })
                        continue
                    
                    # 检查价格合理性
                    issues = []
                    if price_value <= 0:
                        issues.append('价格为负或零')
                    elif price_value > 1000000:  # 价格过高可能有问题
                        issues.append('价格异常高')
                    elif data_type == 'CURRENCY' and price_value > 1000:
                        issues.append('汇率数值异常')
                    
                    if issues:
                        price_validity_report[symbol].extend([{
                            'date': data_date,
                            'issue': issue,
                            'price': price_value
                        } for issue in issues])
                        
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    price_validity_report[symbol].append({
                        'date': data_date,
                        'issue': f'JSON解析错误: {str(e)}',
                        'raw_data': data_json_str[:100]
                    })
            
            # 转换为普通字典
            price_validity_report = dict(price_validity_report)
            
            # 统计问题
            total_issues = sum(len(issues) for issues in price_validity_report.values())
            logger.info(f"价格数据有效性检查完成: 发现{total_issues}个问题")
            
            return {'price_validity': price_validity_report}
            
        except Exception as e:
            logger.error(f"价格数据有效性检查失败: {str(e)}")
            return {}
    
    def generate_data_coverage_chart(self) -> str:
        """生成数据覆盖图表"""
        try:
            cursor = self.conn.cursor()
            
            # 获取每个资产的数据覆盖情况
            cursor.execute("""
                SELECT 
                    symbol,
                    data_type,
                    DATE(data_date) as date,
                    COUNT(*) as daily_count
                FROM macro_data 
                WHERE data_date >= NOW() - INTERVAL '3 years'
                GROUP BY symbol, data_type, DATE(data_date)
                ORDER BY symbol, date
            """)
            
            coverage_data = cursor.fetchall()
            cursor.close()
            
            if not coverage_data:
                return "无数据可生成图表"
            
            # 转换为DataFrame
            df = pd.DataFrame(coverage_data, columns=['symbol', 'data_type', 'date', 'count'])
            df['date'] = pd.to_datetime(df['date'])
            
            # 创建热力图
            plt.figure(figsize=(15, 10))
            
            # 为每个资产创建时间序列
            symbols = df['symbol'].unique()[:20]  # 限制显示前20个资产
            
            for i, symbol in enumerate(symbols):
                symbol_data = df[df['symbol'] == symbol]
                plt.subplot(4, 5, i+1)
                
                if not symbol_data.empty:
                    # 创建日期范围
                    date_range = pd.date_range(
                        start=symbol_data['date'].min(),
                        end=symbol_data['date'].max(),
                        freq='D'
                    )
                    
                    # 重新索引以显示缺失数据
                    symbol_data = symbol_data.set_index('date').reindex(date_range, fill_value=0)
                    
                    plt.plot(symbol_data.index, symbol_data['count'], linewidth=1)
                    plt.title(symbol[:15], fontsize=8)
                    plt.xticks(rotation=45, fontsize=6)
                    plt.yticks(fontsize=6)
            
            plt.tight_layout()
            chart_path = 'data_coverage_chart.png'
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"数据覆盖图表已生成: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"生成数据覆盖图表失败: {str(e)}")
            return f"图表生成失败: {str(e)}"
    
    def run_comprehensive_check(self) -> Dict:
        """运行全面检查"""
        logger.info("开始全面数据检查...")
        
        report = {
            'check_time': datetime.datetime.now().isoformat(),
            'overview': self.get_table_overview(),
            'completeness': self.check_data_completeness(),
            'quality': self.check_data_quality(),
            'price_validity': self.check_price_data_validity(),
            'issues_summary': self.issues,
            'chart_path': self.generate_data_coverage_chart()
        }
        
        # 生成总结
        total_issues = len(self.issues)
        total_assets = report['overview'].get('unique_assets', 0)
        
        report['summary'] = {
            'total_issues': total_issues,
            'total_assets': total_assets,
            'data_quality_score': max(0, 100 - (total_issues * 2)),  # 简单评分
            'recommendations': self._generate_recommendations(report)
        }
        
        logger.info(f"全面检查完成: 发现{total_issues}个问题")
        return report
    
    def _generate_recommendations(self, report: Dict) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        # 基于检查结果生成建议
        if report['overview'].get('total_records', 0) < 10000:
            recommendations.append("数据量偏少，建议增加数据源或延长数据获取周期")
        
        completeness_issues = sum(1 for asset in report['completeness'].get('assets', []) 
                                if asset.get('issues', []))
        if completeness_issues > 0:
            recommendations.append(f"有{completeness_issues}个资产存在完整性问题，建议重新获取数据")
        
        quality_issues = sum(1 for asset in report['quality'].get('quality_metrics', []) 
                           if asset.get('quality_issues', []))
        if quality_issues > 0:
            recommendations.append(f"有{quality_issues}个资产存在质量问题，建议检查数据源")
        
        price_issues = len(report['price_validity'].get('price_validity', {}))
        if price_issues > 0:
            recommendations.append(f"有{price_issues}个资产存在价格数据问题，建议验证数据格式")
        
        if not recommendations:
            recommendations.append("数据质量良好，可以支持分析和图表生成")
        
        return recommendations
    
    def save_report(self, report: Dict, filename: str = 'comprehensive_data_check_report.json'):
        """保存检查报告"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"检查报告已保存: {filename}")
        except Exception as e:
            logger.error(f"保存报告失败: {str(e)}")

def main():
    """主函数"""
    checker = MacroDataChecker()
    
    try:
        # 运行全面检查
        report = checker.run_comprehensive_check()
        
        # 保存报告
        checker.save_report(report)
        
        # 打印摘要
        print("\n" + "="*80)
        print("宏观数据完整性和质量检查报告")
        print("="*80)
        print(f"检查时间: {report['check_time']}")
        print(f"总记录数: {report['overview'].get('total_records', 0):,}")
        print(f"资产数量: {report['overview'].get('unique_assets', 0)}")
        print(f"发现问题: {report['summary']['total_issues']}个")
        print(f"数据质量评分: {report['summary']['data_quality_score']}/100")
        
        print("\n改进建议:")
        for i, rec in enumerate(report['summary']['recommendations'], 1):
            print(f"{i}. {rec}")
        
        if report['issues_summary']:
            print("\n主要问题:")
            for issue in report['issues_summary'][:10]:  # 显示前10个问题
                print(f"- {issue}")
            if len(report['issues_summary']) > 10:
                print(f"... 还有{len(report['issues_summary'])-10}个问题")
        
        print(f"\n详细报告已保存到: comprehensive_data_check_report.json")
        if report.get('chart_path'):
            print(f"数据覆盖图表: {report['chart_path']}")
        
        return report['summary']['data_quality_score'] > 70
        
    except Exception as e:
        logger.error(f"检查过程出错: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)