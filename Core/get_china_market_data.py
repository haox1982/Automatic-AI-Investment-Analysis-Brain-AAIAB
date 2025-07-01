#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
中国股市和香港股市市场数据获取脚本
获取成交量、上涨下跌家数、资金净流入、南向资金净流入等数据
"""

import datetime
import json
import logging
import os
import sys
import traceback
from typing import Dict, List, Optional

import akshare as ak
import pandas as pd

# 相对路径导入我们的数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
from DB.db_utils import get_db_connection, insert_macro_data

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('china_market_data.log')
    ]
)
logger = logging.getLogger(__name__)

# 中国股市和香港股市数据配置
CHINA_MARKET_CONFIG = [
    # 上证指数相关数据
    {
        'name': '上证指数成交量',
        'symbol': 'sh000001',
        'type_code': 'MARKET_DATA',
        'data_type': 'volume',
        'ak_function': 'stock_zh_index_daily',
        'description': '上证指数日成交量'
    },
    {
        'name': '上证指数上涨下跌家数',
        'symbol': 'sh000001',
        'type_code': 'MARKET_DATA',
        'data_type': 'up_down_count',
        'ak_function': 'stock_zh_a_hist_min_em',
        'description': '上证指数上涨下跌家数统计'
    },
    
    # 深证成指相关数据
    {
        'name': '深证成指成交量',
        'symbol': 'sz399001',
        'type_code': 'MARKET_DATA',
        'data_type': 'volume',
        'ak_function': 'stock_zh_index_daily',
        'description': '深证成指日成交量'
    },
    
    # 沪深300相关数据
    {
        'name': '沪深300成交量',
        'symbol': 'sh000300',
        'type_code': 'MARKET_DATA',
        'data_type': 'volume',
        'ak_function': 'stock_zh_index_daily',
        'description': '沪深300指数日成交量'
    },
    
    # 中证500相关数据
    {
        'name': '中证500成交量',
        'symbol': 'sh000905',
        'type_code': 'MARKET_DATA',
        'data_type': 'volume',
        'ak_function': 'stock_zh_index_daily',
        'description': '中证500指数日成交量'
    },
    
    # 港股相关数据
    {
        'name': '恒生指数成交量',
        'symbol': 'HSI',
        'type_code': 'MARKET_DATA',
        'data_type': 'volume',
        'ak_function': 'stock_hk_index_daily_em',
        'description': '恒生指数日成交量'
    },
    
    # 市场整体数据
    {
        'name': 'A股市场资金流向',
        'symbol': 'A_STOCK_FLOW',
        'type_code': 'MARKET_DATA',
        'data_type': 'money_flow',
        'ak_function': 'stock_market_fund_flow',
        'description': 'A股市场整体资金流向'
    },
    
    # 南向资金数据
    {
        'name': '南向资金净流入',
        'symbol': 'SOUTHBOUND_FLOW',
        'type_code': 'MARKET_DATA',
        'data_type': 'southbound_flow',
        'ak_function': 'stock_connect_bk_statistics_em',
        'description': '南向资金净流入统计'
    },
    
    # 北向资金数据
    {
        'name': '北向资金净流入',
        'symbol': 'NORTHBOUND_FLOW',
        'type_code': 'MARKET_DATA',
        'data_type': 'northbound_flow',
        'ak_function': 'stock_connect_bk_statistics_em',
        'description': '北向资金净流入统计'
    }
]

class ChinaMarketDataCollector:
    """中国股市数据收集器"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.results = []
    
    def get_index_volume_data(self, config: Dict) -> Optional[pd.DataFrame]:
        """获取指数成交量数据"""
        try:
            logger.info(f"获取指数成交量数据: {config['name']}")
            
            if config['ak_function'] == 'stock_zh_index_daily':
                # A股指数数据
                data = ak.stock_zh_index_daily(symbol=config['symbol'])
            elif config['ak_function'] == 'stock_hk_index_daily_em':
                # 港股指数数据
                data = ak.stock_hk_index_daily_em(symbol=config['symbol'])
            else:
                logger.error(f"未知的函数: {config['ak_function']}")
                return None
            
            if data is None or data.empty:
                logger.warning(f"未获取到数据: {config['name']}")
                return None
            
            # 确保有日期列
            if 'date' not in data.columns:
                logger.error(f"数据缺少date列: {config['name']}")
                return None
            
            # 确保有成交量列
            volume_columns = ['volume', '成交量', 'vol']
            volume_col = None
            for col in volume_columns:
                if col in data.columns:
                    volume_col = col
                    break
            
            if volume_col is None:
                logger.error(f"数据缺少成交量列: {config['name']}, 可用列: {list(data.columns)}")
                return None
            
            # 重命名列以标准化
            data = data.rename(columns={volume_col: 'volume'})
            
            logger.info(f"成功获取{config['name']}数据，共{len(data)}行")
            return data
            
        except Exception as e:
            logger.error(f"获取{config['name']}数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def get_market_flow_data(self, config: Dict) -> Optional[pd.DataFrame]:
        """获取市场资金流向数据"""
        try:
            logger.info(f"获取市场资金流向数据: {config['name']}")
            
            if config['data_type'] == 'money_flow':
                # A股市场资金流向
                data = ak.stock_market_fund_flow()
            elif config['data_type'] in ['southbound_flow', 'northbound_flow']:
                # 南北向资金流向
                data = ak.stock_connect_bk_statistics_em()
            else:
                logger.error(f"未知的数据类型: {config['data_type']}")
                return None
            
            if data is None or data.empty:
                logger.warning(f"未获取到数据: {config['name']}")
                return None
            
            logger.info(f"成功获取{config['name']}数据，共{len(data)}行")
            return data
            
        except Exception as e:
            logger.error(f"获取{config['name']}数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def get_up_down_count_data(self, config: Dict) -> Optional[pd.DataFrame]:
        """获取上涨下跌家数数据"""
        try:
            logger.info(f"获取上涨下跌家数数据: {config['name']}")
            
            # 获取A股涨跌停数据作为替代
            data = ak.stock_zt_pool_em(date=datetime.datetime.now().strftime('%Y%m%d'))
            
            if data is None or data.empty:
                logger.warning(f"未获取到数据: {config['name']}")
                return None
            
            logger.info(f"成功获取{config['name']}数据，共{len(data)}行")
            return data
            
        except Exception as e:
            logger.error(f"获取{config['name']}数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def process_and_save_data(self, config: Dict, data: pd.DataFrame) -> bool:
        """处理并保存数据到数据库"""
        try:
            if data is None or data.empty:
                return False
            
            # 根据数据类型处理数据
            if config['data_type'] == 'volume':
                return self._save_volume_data(config, data)
            elif config['data_type'] == 'money_flow':
                return self._save_money_flow_data(config, data)
            elif config['data_type'] in ['southbound_flow', 'northbound_flow']:
                return self._save_capital_flow_data(config, data)
            elif config['data_type'] == 'up_down_count':
                return self._save_up_down_data(config, data)
            else:
                logger.error(f"未知的数据类型: {config['data_type']}")
                return False
                
        except Exception as e:
            logger.error(f"处理和保存数据失败: {config['name']}, 错误: {str(e)}")
            return False
    
    def _save_volume_data(self, config: Dict, data: pd.DataFrame) -> bool:
        """保存成交量数据"""
        try:
            saved_count = 0
            for _, row in data.iterrows():
                # 准备数据
                data_dict = {
                    'type_code': config['type_code'],
                    'source': 'akshare',
                    'symbol': config['symbol'],
                    'data_date': pd.to_datetime(row['date']).date(),
                    'value': float(row.get('volume', 0)),
                    'open_price': float(row.get('open', 0)) if 'open' in row else None,
                    'high_price': float(row.get('high', 0)) if 'high' in row else None,
                    'low_price': float(row.get('low', 0)) if 'low' in row else None,
                    'close_price': float(row.get('close', 0)) if 'close' in row else None,
                    'volume': float(row.get('volume', 0)),
                    'additional_data': {
                        'data_type': config['data_type'],
                        'description': config['description']
                    }
                }
                
                if insert_macro_data(data_dict):
                    saved_count += 1
            
            logger.info(f"成功保存{config['name']}数据: {saved_count}条")
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"保存成交量数据失败: {str(e)}")
            return False
    
    def _save_money_flow_data(self, config: Dict, data: pd.DataFrame) -> bool:
        """保存资金流向数据"""
        try:
            saved_count = 0
            today = datetime.datetime.now().date()
            
            # 资金流向数据通常是当日汇总数据
            for _, row in data.iterrows():
                data_dict = {
                    'type_code': config['type_code'],
                    'source': 'akshare',
                    'symbol': config['symbol'],
                    'data_date': today,
                    'value': float(row.iloc[1]) if len(row) > 1 else 0,  # 取第二列作为主要数值
                    'additional_data': {
                        'data_type': config['data_type'],
                        'description': config['description'],
                        'raw_data': row.to_dict()
                    }
                }
                
                if insert_macro_data(data_dict):
                    saved_count += 1
            
            logger.info(f"成功保存{config['name']}数据: {saved_count}条")
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"保存资金流向数据失败: {str(e)}")
            return False
    
    def _save_capital_flow_data(self, config: Dict, data: pd.DataFrame) -> bool:
        """保存南北向资金流向数据"""
        try:
            saved_count = 0
            today = datetime.datetime.now().date()
            
            # 处理南北向资金数据
            for _, row in data.iterrows():
                data_dict = {
                    'type_code': config['type_code'],
                    'source': 'akshare',
                    'symbol': config['symbol'],
                    'data_date': today,
                    'value': float(row.iloc[1]) if len(row) > 1 else 0,
                    'additional_data': {
                        'data_type': config['data_type'],
                        'description': config['description'],
                        'raw_data': row.to_dict()
                    }
                }
                
                if insert_macro_data(data_dict):
                    saved_count += 1
            
            logger.info(f"成功保存{config['name']}数据: {saved_count}条")
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"保存南北向资金数据失败: {str(e)}")
            return False
    
    def _save_up_down_data(self, config: Dict, data: pd.DataFrame) -> bool:
        """保存上涨下跌家数数据"""
        try:
            saved_count = 0
            today = datetime.datetime.now().date()
            
            # 统计涨跌停家数
            up_count = len(data) if not data.empty else 0
            
            data_dict = {
                'type_code': config['type_code'],
                'source': 'akshare',
                'symbol': config['symbol'],
                'data_date': today,
                'value': up_count,
                'additional_data': {
                    'data_type': config['data_type'],
                    'description': config['description'],
                    'up_count': up_count,
                    'total_count': len(data)
                }
            }
            
            if insert_macro_data(data_dict):
                saved_count = 1
            
            logger.info(f"成功保存{config['name']}数据: {saved_count}条")
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"保存上涨下跌数据失败: {str(e)}")
            return False
    
    def collect_all_data(self) -> Dict:
        """收集所有中国股市数据"""
        logger.info("开始收集中国股市和香港股市数据")
        
        for config in CHINA_MARKET_CONFIG:
            try:
                logger.info(f"处理: {config['name']}")
                
                # 根据数据类型选择获取方法
                if config['data_type'] == 'volume':
                    data = self.get_index_volume_data(config)
                elif config['data_type'] == 'money_flow':
                    data = self.get_market_flow_data(config)
                elif config['data_type'] in ['southbound_flow', 'northbound_flow']:
                    data = self.get_market_flow_data(config)
                elif config['data_type'] == 'up_down_count':
                    data = self.get_up_down_count_data(config)
                else:
                    logger.error(f"未知的数据类型: {config['data_type']}")
                    self.error_count += 1
                    continue
                
                # 处理并保存数据
                if self.process_and_save_data(config, data):
                    self.success_count += 1
                    self.results.append({
                        'name': config['name'],
                        'status': 'success',
                        'message': '数据获取和保存成功'
                    })
                else:
                    self.error_count += 1
                    self.results.append({
                        'name': config['name'],
                        'status': 'error',
                        'message': '数据保存失败'
                    })
                    
            except Exception as e:
                logger.error(f"处理{config['name']}时发生错误: {str(e)}")
                self.error_count += 1
                self.results.append({
                    'name': config['name'],
                    'status': 'error',
                    'message': str(e)
                })
        
        # 返回汇总结果
        summary = {
            'total_processed': len(CHINA_MARKET_CONFIG),
            'success_count': self.success_count,
            'error_count': self.error_count,
            'results': self.results
        }
        
        logger.info(f"中国股市数据收集完成: 成功{self.success_count}个，失败{self.error_count}个")
        return summary

def main():
    """主函数"""
    try:
        collector = ChinaMarketDataCollector()
        result = collector.collect_all_data()
        
        # 输出结果
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result['error_count'] == 0
        
    except Exception as e:
        logger.error(f"主程序执行失败: {str(e)}")
        logger.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)