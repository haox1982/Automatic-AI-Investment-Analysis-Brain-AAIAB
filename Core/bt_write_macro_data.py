#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
宏观数据写入脚本
专门负责将验证过的宏观数据写入数据库
支持增量更新和重复数据检测
"""

import datetime
import json
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional

import akshare as ak
import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta

# 相对路径导入我们的数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
from DB.db_utils import get_db_connection, insert_macro_data

# 导入配置
from macro_config import MACRO_ASSETS_CONFIG, AK_MACRO_FUNCTION_MAP

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('macro_data_write.log')
    ]
)
logger = logging.getLogger(__name__)

# 数据更新频率配置
DATA_UPDATE_FREQUENCY = {
    'DAILY': ['INDEX', 'CURRENCY', 'COMMODITY', 'CRYPTO'],  # 日更数据
    'WEEKLY': ['INTEREST_RATE', 'MONEY_SUPPLY'],  # 周更数据  
    'MONTHLY': ['CPI', 'PPI', 'GDP']  # 月更数据
}

# 获取数据的默认时间范围（年）
DEFAULT_DATA_YEARS = 3

def insert_macro_data_new(asset_name: str, asset_code: str, source: str, data_type: str, 
                         data_date: datetime.datetime, data_json: str) -> bool:
    """新的宏观数据插入函数，适配新的表结构"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        # 插入或更新数据
        query = """
        INSERT INTO macro_data (asset_name, asset_code, source, data_type, data_date, data_json, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (asset_name, source, data_date) 
        DO UPDATE SET 
            asset_code = EXCLUDED.asset_code,
            data_type = EXCLUDED.data_type,
            data_json = EXCLUDED.data_json,
            updated_at = NOW()
        """
        
        cursor.execute(query, (asset_name, asset_code, source, data_type, data_date, data_json))
        conn.commit()
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"插入数据失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def should_update_data(data_type: str, last_update: datetime.datetime = None, force_update: bool = False) -> bool:
    """根据数据类型和上次更新时间判断是否需要更新数据"""
    if force_update or not last_update:
        return True
        
    now = datetime.datetime.now()
    # 确保last_update是datetime.datetime类型
    if isinstance(last_update, datetime.date) and not isinstance(last_update, datetime.datetime):
        last_update = datetime.datetime.combine(last_update, datetime.time.min)
    days_since_update = (now - last_update).days
    
    # 检查数据更新频率
    for frequency, types in DATA_UPDATE_FREQUENCY.items():
        if data_type in types:
            if frequency == 'DAILY' and days_since_update >= 1:
                return True
            elif frequency == 'WEEKLY' and days_since_update >= 7:
                return True
            elif frequency == 'MONTHLY' and days_since_update >= 30:
                return True
            else:
                return False
    
    # 默认按日更新
    return days_since_update >= 1

class MacroDataWriter:
    """宏观数据写入器"""
    
    def __init__(self):
        self.results = []
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.new_records_count = 0
        self.updated_records_count = 0
    
    def get_existing_data_info(self, asset_name: str, source: str) -> Tuple[Optional[datetime.datetime], int]:
        """获取数据库中已存在数据的信息"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 查询最新数据日期和总记录数
            query = """
            SELECT MAX(data_date) as latest_date, COUNT(*) as total_count
            FROM macro_data 
            WHERE symbol = %s AND source = %s
            """
            
            cursor.execute(query, (asset_name, source))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result and result[0]:
                return result[0], result[1]
            else:
                return None, 0
                
        except Exception as e:
            logger.error(f"查询已存在数据信息失败: {str(e)}")
            return None, 0
    
    def check_data_exists(self, asset_name: str, source: str, data_date: datetime.datetime) -> bool:
        """检查特定日期的数据是否已存在"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT COUNT(*) FROM macro_data 
            WHERE symbol = %s AND source = %s AND data_date = %s
            """
            
            cursor.execute(query, (asset_name, source, data_date))
            count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return count > 0
            
        except Exception as e:
            logger.error(f"检查数据是否存在失败: {str(e)}")
            return False
    
    def get_yfinance_data(self, asset_config: Dict, incremental: bool = True) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取yfinance数据"""
        try:
            logger.info(f"开始获取yfinance数据: {asset_config['name']} ({asset_config['code']})")
            
            # 检查是否需要更新数据
            latest_date, existing_count = self.get_existing_data_info(asset_config['name'], 'yfinance')
            if incremental and latest_date:
                data_type = asset_config.get('type', 'UNKNOWN')
                if not should_update_data(data_type, latest_date, force_update=not incremental):
                    latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                    logger.info(f"数据无需更新: {asset_config['name']} (上次更新: {latest_date_str})")
                    return True, "数据无需更新", pd.DataFrame()
            elif not incremental:
                # 全量模式，强制更新
                logger.info(f"全量模式: 强制更新 {asset_config['name']}")
            
            ticker = yf.Ticker(asset_config['code'])
            end_date = datetime.datetime.now()
            
            if incremental and latest_date:
                # 增量模式：从最新数据日期开始获取
                start_date = latest_date - datetime.timedelta(days=1)  # 往前一天确保不遗漏
                start_date_str = start_date.date() if isinstance(start_date, datetime.datetime) else start_date
                logger.info(f"增量模式: 从{start_date_str}开始获取数据 (已有{existing_count}条记录)")
            else:
                # 全量模式或首次获取：获取3年数据
                start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
                start_date_str = start_date.date() if isinstance(start_date, datetime.datetime) else start_date
                logger.info(f"{'全量模式' if not incremental else '首次获取'}: 从{start_date_str}开始获取数据")
            
            data = ticker.history(start=start_date, end=end_date)
            
            if data.empty:
                return False, "无法获取数据或数据为空", None
            
            logger.info(f"yfinance数据获取成功: {asset_config['name']}, 数据行数: {len(data)}")
            return True, f"成功获取{len(data)}行数据", data
            
        except Exception as e:
            error_msg = f"yfinance数据获取失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            return False, error_msg, None
    
    def get_ak_index_data(self, asset_config: Dict, incremental: bool = True) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取akshare指数数据"""
        try:
            logger.info(f"开始获取ak_index数据: {asset_config['name']} ({asset_config['code']})")
            
            # 检查是否需要更新数据
            latest_date, existing_count = self.get_existing_data_info(asset_config['name'], 'ak_index')
            if incremental and latest_date:
                data_type = asset_config.get('type', 'UNKNOWN')
                if not should_update_data(data_type, latest_date, force_update=not incremental):
                    latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                    logger.info(f"数据无需更新: {asset_config['name']} (上次更新: {latest_date_str})")
                    return True, "数据无需更新", pd.DataFrame()
            elif not incremental:
                logger.info(f"全量模式: 强制更新 {asset_config['name']}")
            
            if incremental and latest_date:
                latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                logger.info(f"增量模式: 已有{existing_count}条记录，最新日期{latest_date_str}")
            else:
                logger.info(f"{'全量模式' if not incremental else '首次获取'}: 获取指数数据")
            
            # 使用新浪财经接口获取指数数据，只传递symbol参数
            data = ak.stock_zh_index_daily(symbol=asset_config['code'])
            
            if data is None or data.empty:
                return False, "无法获取数据或数据为空", None
            
            # 检查数据列名
            logger.info(f"ak_index数据列名: {asset_config['name']}, 列: {list(data.columns)}")
            
            # stock_zh_index_daily返回的数据已经包含date列
            if 'date' not in data.columns:
                logger.error(f"ak_index未找到date列: {asset_config['name']}, 列名: {list(data.columns)}")
                return False, "未找到date列", None
            
            # 如果是增量模式且有历史数据，过滤新数据
            if incremental and latest_date:
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    latest_date_pd = pd.to_datetime(latest_date)
                    
                    # 增加日志，观察过滤前的数据
                    logger.info(f"过滤前，从akshare获取的最新数据日期: {data['date'].max()}")
                    logger.info(f"数据库中最新日期: {latest_date_pd}")

                    # 规范化日期后再比较，避免时间部分影响
                    data = data[data['date'].dt.normalize() > latest_date_pd.normalize()]
                    
                    if data.empty:
                        logger.info(f"没有新数据需要更新: {asset_config['name']}")
                        return True, "没有新数据", pd.DataFrame()
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}")
            elif not incremental:
                # 全量模式：手动过滤最近3年的数据
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    end_date = datetime.datetime.now()
                    start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
                    data = data[data['date'] >= start_date]
                    data = data[data['date'] <= end_date]
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}, 使用全部数据")
            
            if data.empty:
                return False, "过滤后数据为空", None
            
            logger.info(f"ak_index数据获取成功: {asset_config['name']}, 数据行数: {len(data)}")
            return True, f"成功获取{len(data)}行数据", data
            
        except Exception as e:
            error_msg = f"ak_index数据获取失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False, error_msg, None
    
    def get_ak_forex_data(self, asset_config: Dict, incremental: bool = True) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取外汇数据 - 智能选择数据源"""
        try:
            logger.info(f"开始获取外汇数据: {asset_config['name']} ({asset_config['code']})")
            
            # 检查是否需要更新数据
            latest_date, existing_count = self.get_existing_data_info(asset_config['name'], 'ak_forex')
            if incremental and latest_date:
                data_type = asset_config.get('type', 'UNKNOWN')
                if not should_update_data(data_type, latest_date, force_update=not incremental):
                    latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                    logger.info(f"数据无需更新: {asset_config['name']} (上次更新: {latest_date_str})")
                    return True, "数据无需更新", pd.DataFrame()
            elif not incremental:
                logger.info(f"全量模式: 强制更新 {asset_config['name']}")
            
            if incremental and latest_date:
                latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                logger.info(f"增量模式: 已有{existing_count}条记录，最新日期{latest_date_str}")
            else:
                logger.info(f"{'全量模式' if not incremental else '首次获取'}: 获取外汇数据")
            
            # 解析货币对
            code = asset_config['code']
            
            if len(code) == 6:  # 如USDCNY
                base_currency = code[:3]  # USD
                quote_currency = code[3:]  # CNY
            else:
                # 如果是空格分隔的格式
                currencies = code.split(' ')
                if len(currencies) != 2:
                    return False, "货币对格式错误，应为'USDCNY'或'USD CNY'", None
                base_currency = currencies[0]
                quote_currency = currencies[1]
            
            # 判断是否为人民币相关的货币对
            if quote_currency == 'CNY':
                # 使用akshare获取中国银行外汇牌价（相对于人民币）
                return self._get_ak_forex_cny_data(asset_config, base_currency, incremental, latest_date)
            else:
                # 使用yfinance获取真实的货币对数据
                return self._get_yfinance_forex_data(asset_config, code, incremental, latest_date)
                
        except Exception as e:
            error_msg = f"外汇数据获取失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False, error_msg, None
    
    def _get_ak_forex_cny_data(self, asset_config: Dict, base_currency: str, incremental: bool, latest_date) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取相对于人民币的外汇数据（使用akshare）"""
        # 货币代码到中文名称的映射
        currency_mapping = {
            'USD': '美元',
            'EUR': '欧元', 
            'GBP': '英镑',
            'JPY': '日元',
            'HKD': '港币',
            'AUD': '澳元',
            'CAD': '加元'
        }
        
        if base_currency not in currency_mapping:
            return False, f"不支持的货币代码: {base_currency}", None
        
        symbol = currency_mapping[base_currency]
        
        # 确定获取数据的时间范围
        end_date = datetime.datetime.now()
        
        if incremental and latest_date:
            # 增量模式：从最新日期后开始获取
            start_date = latest_date + datetime.timedelta(days=1)
            logger.info(f"增量模式获取{symbol}数据: 从{start_date.strftime('%Y%m%d')}到{end_date.strftime('%Y%m%d')}")
        else:
            # 全量模式或首次获取：获取最近3年数据
            start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
            logger.info(f"全量模式获取{symbol}数据: 从{start_date.strftime('%Y%m%d')}到{end_date.strftime('%Y%m%d')}")
        
        try:
            # 使用currency_boc_sina函数获取中国银行外汇数据，需要指定日期范围
            data = ak.currency_boc_sina(
                symbol=symbol, 
                start_date=start_date.strftime('%Y%m%d'), 
                end_date=end_date.strftime('%Y%m%d')
            )
            
            if data is None or data.empty:
                logger.warning(f"currency_boc_sina返回空数据: {symbol}")
                return False, "无法获取数据或数据为空", None
            
            logger.info(f"成功获取{symbol}数据，行数: {len(data)}，列: {list(data.columns)}")
            
        except Exception as e:
            logger.error(f"currency_boc_sina调用失败: {symbol}, 错误: {str(e)}")
            return False, f"currency_boc_sina调用失败: {str(e)}", None
        
        # 重命名列以符合标准格式
        column_mapping = {
            '日期': 'date',
            '中行汇买价': 'open',
            '中行钞买价': 'low', 
            '中行钞卖价/汇卖价': 'high',
            '央行中间价': 'close'
        }
        
        return self._process_forex_data(data, asset_config, column_mapping, incremental, latest_date)
    
    def _get_yfinance_forex_data(self, asset_config: Dict, code: str, incremental: bool, latest_date) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取真实货币对数据（使用yfinance）"""
        import yfinance as yf
        
        # 转换为yfinance格式
        yf_symbol = f"{code}=X"  # 如EURUSD=X
        
        try:
            ticker = yf.Ticker(yf_symbol)
            
            # 确定获取数据的时间范围
            if incremental and latest_date:
                # 增量模式：从最新日期后开始获取
                start_date = latest_date + datetime.timedelta(days=1)
                end_date = datetime.datetime.now()
                data = ticker.history(start=start_date, end=end_date)
            else:
                # 全量模式：获取最近3年数据
                end_date = datetime.datetime.now()
                start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
                data = ticker.history(start=start_date, end=end_date)
            
            if data is None or data.empty:
                return False, "无法获取数据或数据为空", None
            
            # 重置索引，将日期作为列
            data = data.reset_index()
            data['date'] = data['Date']
            
            # 重命名列以符合标准格式
            column_mapping = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }
            
            return self._process_forex_data(data, asset_config, column_mapping, incremental, latest_date)
            
        except Exception as e:
            return False, f"yfinance获取{yf_symbol}失败: {str(e)}", None
    
    def _process_forex_data(self, data: pd.DataFrame, asset_config: Dict, column_mapping: Dict, incremental: bool, latest_date) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """处理外汇数据的通用方法"""
        try:
            # 检查并重命名列
            for old_col, new_col in column_mapping.items():
                if old_col in data.columns:
                    data = data.rename(columns={old_col: new_col})
            
            # 确保有必需的列
            required_cols = ['date', 'open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                return False, f"缺少必需列: {missing_cols}", None
            
            # 如果是增量模式且有历史数据，过滤新数据
            if incremental and latest_date:
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    # 确保latest_date也是pandas兼容的datetime类型
                    latest_date_pd = pd.to_datetime(latest_date)
                    data = data[data['date'] > latest_date_pd]
                    if data.empty:
                        logger.info(f"没有新数据需要更新: {asset_config['name']}")
                        return True, "没有新数据", pd.DataFrame()
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}")
            elif not incremental:
                # 全量模式：手动过滤最近3年的数据
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    end_date = datetime.datetime.now()
                    start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
                    data = data[data['date'] >= start_date]
                    data = data[data['date'] <= end_date]
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}, 使用全部数据")
            
            if data.empty:
                return False, "过滤后数据为空", None
            
            logger.info(f"外汇数据获取成功: {asset_config['name']}, 数据行数: {len(data)}")
            return True, f"成功获取{len(data)}行数据", data
            
        except Exception as e:
            error_msg = f"外汇数据处理失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            return False, error_msg, None
    
    def get_ak_macro_data(self, asset_config: Dict, incremental: bool = True) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取akshare宏观数据"""
        try:
            logger.info(f"开始获取ak_macro数据: {asset_config['name']} ({asset_config['code']})")
            
            # 检查是否需要更新数据
            latest_date, existing_count = self.get_existing_data_info(asset_config['name'], 'ak_macro')
            if incremental and latest_date:
                data_type = asset_config.get('type', 'UNKNOWN')
                if not should_update_data(data_type, latest_date, force_update=not incremental):
                    latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                    logger.info(f"数据无需更新: {asset_config['name']} (上次更新: {latest_date_str})")
                    return True, "数据无需更新", pd.DataFrame()
            elif not incremental:
                logger.info(f"全量模式: 强制更新 {asset_config['name']}")
            
            func_name = AK_MACRO_FUNCTION_MAP.get(asset_config['code'])
            if not func_name:
                return False, f"未找到对应的akshare函数: {asset_config['code']}", None
            
            logger.info(f"使用akshare函数: {func_name}")
            
            if incremental and latest_date:
                latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                logger.info(f"增量模式: 已有{existing_count}条记录，最新日期{latest_date_str}")
            else:
                logger.info(f"{'全量模式' if not incremental else '首次获取'}宏观数据")
            
            # 动态调用akshare函数
            if hasattr(ak, func_name):
                func = getattr(ak, func_name)
                logger.info(f"正在调用函数: {func_name}()")
                data = func()
                    
                logger.info(f"函数调用完成，返回数据类型: {type(data)}")
                
                if data is None:
                    return False, "函数返回None", None
                    
                if hasattr(data, 'empty') and data.empty:
                    return False, "返回的DataFrame为空", None
                    
                logger.info(f"数据行数: {len(data)}, 列数: {len(data.columns)}")
                logger.info(f"数据列名: {list(data.columns)}")
            else:
                return False, f"akshare中不存在函数: {func_name}", None
            
            logger.info(f"ak_macro数据获取成功: {asset_config['name']}, 数据行数: {len(data)}")
            return True, f"成功获取{len(data)}行数据", data
            
        except Exception as e:
            error_msg = f"ak_macro数据获取失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False, error_msg, None
    
    def get_ak_gold_spot_data(self, asset_config: Dict, incremental: bool = True) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """获取上海黄金交易所现货数据"""
        try:
            logger.info(f"开始获取ak_gold_spot数据: {asset_config['name']} ({asset_config['code']})")
            
            # 检查是否需要更新数据
            latest_date, existing_count = self.get_existing_data_info(asset_config['name'], 'ak_gold_spot')
            if incremental and latest_date:
                data_type = asset_config.get('type', 'UNKNOWN')
                if not should_update_data(data_type, latest_date, force_update=not incremental):
                    latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                    logger.info(f"数据无需更新: {asset_config['name']} (上次更新: {latest_date_str})")
                    return True, "数据无需更新", pd.DataFrame()
            elif not incremental:
                logger.info(f"全量模式: 强制更新 {asset_config['name']}")
            
            if incremental and latest_date:
                latest_date_str = latest_date.date() if isinstance(latest_date, datetime.datetime) else latest_date
                logger.info(f"增量模式: 已有{existing_count}条记录，最新日期{latest_date_str}")
            else:
                logger.info(f"{'全量模式' if not incremental else '首次获取'}: 获取上海金现货数据")
            
            # 使用akshare的spot_hist_sge函数获取上海黄金交易所历史数据
            symbol = asset_config['code']  # Au99.99, Au100g, Au(T+D)等
            
            logger.info(f"正在调用spot_hist_sge(symbol='{symbol}')")
            data = ak.spot_hist_sge(symbol=symbol)
            
            if data is None or data.empty:
                return False, "无法获取数据或数据为空", None
            
            # 检查数据列名
            logger.info(f"ak_gold_spot数据列名: {asset_config['name']}, 列: {list(data.columns)}")
            
            # spot_hist_sge返回的数据应该包含date列
            if 'date' not in data.columns:
                logger.error(f"ak_gold_spot未找到date列: {asset_config['name']}, 列名: {list(data.columns)}")
                return False, "未找到date列", None
            
            # 如果是增量模式且有历史数据，过滤新数据
            if incremental and latest_date:
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    latest_date_pd = pd.to_datetime(latest_date)
                    
                    # 增加日志，观察过滤前的数据
                    logger.info(f"过滤前，从akshare获取的最新数据日期: {data['date'].max()}")
                    logger.info(f"数据库中最新日期: {latest_date_pd}")

                    # 规范化日期后再比较，避免时间部分影响
                    data = data[data['date'].dt.normalize() > latest_date_pd.normalize()]
                    
                    if data.empty:
                        logger.info(f"没有新数据需要更新: {asset_config['name']}")
                        return True, "没有新数据", pd.DataFrame()
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}")
            elif not incremental:
                # 全量模式：手动过滤最近3年的数据
                try:
                    data['date'] = pd.to_datetime(data['date'])
                    end_date = datetime.datetime.now()
                    start_date = end_date - relativedelta(years=DEFAULT_DATA_YEARS)
                    data = data[data['date'] >= start_date]
                    data = data[data['date'] <= end_date]
                except Exception as date_error:
                    logger.warning(f"日期过滤失败: {asset_config['name']}, 错误: {str(date_error)}, 使用全部数据")
            
            if data.empty:
                return False, "过滤后数据为空", None
            
            logger.info(f"ak_gold_spot数据获取成功: {asset_config['name']}, 数据行数: {len(data)}")
            return True, f"成功获取{len(data)}行数据", data
            
        except Exception as e:
            error_msg = f"ak_gold_spot数据获取失败: {str(e)}"
            logger.error(f"{asset_config['name']}: {error_msg}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False, error_msg, None
    
    def process_and_save_data(self, asset_config: Dict, data: pd.DataFrame, incremental: bool = True) -> Tuple[int, int]:
        """处理并保存数据到数据库"""
        new_count = 0
        updated_count = 0
        
        try:
            # 确保数据有日期索引或日期列
            if hasattr(data.index, 'date') or 'date' in data.columns:
                if 'date' in data.columns:
                    data['date'] = pd.to_datetime(data['date'])
                    data.set_index('date', inplace=True)
                
                # 遍历每一行数据
                for date, row in data.iterrows():
                    if pd.isna(date):
                        continue
                    
                    # 转换为datetime对象
                    if hasattr(date, 'to_pydatetime'):
                        data_date = date.to_pydatetime()
                    else:
                        data_date = pd.to_datetime(date).to_pydatetime()
                    
                    # 检查是否已存在
                    if incremental and self.check_data_exists(asset_config['name'], asset_config['source'], data_date):
                        continue  # 跳过已存在的数据
                    
                    # 准备数据
                    data_dict = row.to_dict()
                    
                    # 处理NaN值
                    for key, value in data_dict.items():
                        if pd.isna(value):
                            data_dict[key] = None
                    
                    # 获取价格数据，根据数据源优先使用对应的字段名
                    # akshare数据源使用小写字段名，yfinance使用大写字段名
                    if asset_config['source'].startswith('ak_'):
                        # akshare数据源：优先使用小写字段名
                        close_value = data_dict.get('close') or data_dict.get('Close') or data_dict.get('收盘价')
                        open_value = data_dict.get('open') or data_dict.get('Open') or data_dict.get('开盘价')
                        high_value = data_dict.get('high') or data_dict.get('High') or data_dict.get('最高价')
                        low_value = data_dict.get('low') or data_dict.get('Low') or data_dict.get('最低价')
                        volume_value = data_dict.get('volume') or data_dict.get('Volume') or data_dict.get('成交量')
                    else:
                        # yfinance等其他数据源：优先使用大写字段名
                        close_value = data_dict.get('Close') or data_dict.get('close') or data_dict.get('收盘价')
                        open_value = data_dict.get('Open') or data_dict.get('open') or data_dict.get('开盘价')
                        high_value = data_dict.get('High') or data_dict.get('high') or data_dict.get('最高价')
                        low_value = data_dict.get('Low') or data_dict.get('low') or data_dict.get('最低价')
                        volume_value = data_dict.get('Volume') or data_dict.get('volume') or data_dict.get('成交量')
                    
                    # 调试日志
                    logger.info(f"数据处理 {asset_config['name']} {data_date}: close={close_value}, open={open_value}")
                    
                    # 插入数据
                    insert_data = {
                        'type_code': asset_config.get('type', 'UNKNOWN'),
                        'source': asset_config['source'],
                        'symbol': asset_config['name'],
                        'data_date': data_date,
                        'value': close_value,
                        'open_price': open_value,
                        'high_price': high_value,
                        'low_price': low_value,
                        'close_price': close_value,
                        'volume': volume_value,
                        'additional_data': data_dict
                    }
                    
                    try:
                        insert_macro_data(insert_data)
                        success = True
                    except Exception as e:
                        logger.error(f"插入数据失败 {asset_config['name']}: {e}")
                        success = False
                    
                    if success:
                        new_count += 1
                    
            elif '日期' in data.columns:
                # 处理有'日期'列的宏观数据（如央行利率）
                logger.info(f"处理包含'日期'列的宏观数据: {asset_config['name']}")
                
                for index, row in data.iterrows():
                    # 获取日期
                    date_value = row['日期']
                    if pd.isna(date_value):
                        continue
                    
                    # 转换为datetime对象
                    data_date = pd.to_datetime(date_value).to_pydatetime()
                    
                    # 检查是否已存在
                    if incremental and self.check_data_exists(asset_config['name'], asset_config['source'], data_date):
                        continue  # 跳过已存在的数据
                    
                    # 获取数值（优先使用'今值'列）
                    value = row.get('今值') or row.get('value') or row.get('数值')
                    if pd.isna(value):
                        value = None
                    
                    # 准备数据
                    data_dict = row.to_dict()
                    
                    # 处理NaN值
                    for key, val in data_dict.items():
                        if pd.isna(val):
                            data_dict[key] = None
                    
                    logger.info(f"处理央行利率数据 {asset_config['name']} {data_date}: value={value}")
                    
                    insert_data = {
                        'type_code': asset_config.get('type', 'UNKNOWN'),
                        'source': asset_config['source'],
                        'symbol': asset_config['name'],
                        'data_date': data_date,
                        'value': value,
                        'additional_data': data_dict
                    }
                    
                    try:
                        insert_macro_data(insert_data)
                        success = True
                    except Exception as e:
                        logger.error(f"插入央行利率数据失败 {asset_config['name']}: {e}")
                        success = False
                    
                    if success:
                        new_count += 1
            else:
                # 对于没有明确日期的宏观数据，使用当前时间
                current_time = datetime.datetime.now()
                
                # 检查是否已存在今天的数据
                if incremental and self.check_data_exists(asset_config['name'], asset_config['source'], current_time.date()):
                    logger.info(f"{asset_config['name']}: 今日数据已存在，跳过")
                    return 0, 0
                
                # 将整个DataFrame转换为JSON
                data_dict = data.to_dict('records')
                
                # 处理NaN值
                for record in data_dict:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                
                insert_data = {
                    'type_code': asset_config.get('type', 'UNKNOWN'),
                    'source': asset_config['source'],
                    'symbol': asset_config['name'],
                    'data_date': current_time,
                    'value': None,  # 宏观数据通常没有单一数值
                    'additional_data': data_dict
                }
                
                try:
                    insert_macro_data(insert_data)
                    success = True
                except Exception as e:
                    logger.error(f"插入宏观数据失败 {asset_config['name']}: {e}")
                    success = False
                
                if success:
                    new_count = 1
            
            logger.info(f"{asset_config['name']}: 新增{new_count}条记录")
            return new_count, updated_count
            
        except Exception as e:
            logger.error(f"处理和保存数据失败 {asset_config['name']}: {str(e)}")
            return 0, 0
    
    def write_single_asset(self, asset_config: Dict, incremental: bool = True) -> Dict:
        """写入单个资产数据"""
        result = {
            'name': asset_config['name'],
            'code': asset_config['code'],
            'source': asset_config['source'],
            'type': asset_config.get('type', 'UNKNOWN'),
            'success': False,
            'message': '',
            'new_records': 0,
            'updated_records': 0
        }
        
        try:
            # 根据数据源调用相应的获取函数
            if asset_config['source'] == 'yfinance':
                success, message, data = self.get_yfinance_data(asset_config, incremental)
            elif asset_config['source'] == 'ak_index':
                success, message, data = self.get_ak_index_data(asset_config, incremental)
            elif asset_config['source'] == 'ak_forex':
                success, message, data = self.get_ak_forex_data(asset_config, incremental)
            elif asset_config['source'] == 'ak_macro':
                success, message, data = self.get_ak_macro_data(asset_config, incremental)
            elif asset_config['source'] == 'ak_gold_spot':
                success, message, data = self.get_ak_gold_spot_data(asset_config, incremental)
            else:
                success, message, data = False, f"不支持的数据源: {asset_config['source']}", None
            
            if success and data is not None:
                # 处理并保存数据
                new_count, updated_count = self.process_and_save_data(asset_config, data, incremental)
                
                result['success'] = True
                result['message'] = f"成功处理数据，新增{new_count}条记录"
                result['new_records'] = new_count
                result['updated_records'] = updated_count
                
                self.success_count += 1
                self.new_records_count += new_count
                self.updated_records_count += updated_count
                
                if new_count == 0:
                    self.skipped_count += 1
                    result['message'] += " (无新数据)"
            else:
                result['success'] = False
                result['message'] = message
                self.error_count += 1
                
        except Exception as e:
            result['success'] = False
            result['message'] = f"写入过程出错: {str(e)}"
            self.error_count += 1
            logger.error(f"写入{asset_config['name']}时出错: {traceback.format_exc()}")
        
        return result
    
    def write_all_assets(self, incremental: bool = True, max_workers: int = 3) -> List[Dict]:
        """并行写入所有资产数据"""
        logger.info(f"开始写入{len(MACRO_ASSETS_CONFIG)}个宏观数据源 (增量模式: {incremental})...")
        
        # 使用线程池并行处理（减少并发数避免API限制）
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_asset = {executor.submit(self.write_single_asset, asset, incremental): asset 
                             for asset in MACRO_ASSETS_CONFIG}
            
            # 收集结果
            for future in as_completed(future_to_asset):
                asset = future_to_asset[future]
                try:
                    result = future.result()
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"处理{asset['name']}时出错: {str(e)}")
                    self.results.append({
                        'name': asset['name'],
                        'code': asset['code'],
                        'source': asset['source'],
                        'success': False,
                        'message': f"并行处理出错: {str(e)}",
                        'new_records': 0,
                        'updated_records': 0
                    })
                    self.error_count += 1
        
        return self.results
    
    def generate_report(self) -> str:
        """生成写入报告"""
        report = []
        report.append("=" * 80)
        report.append("宏观数据写入报告")
        report.append("=" * 80)
        report.append(f"写入时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"总计: {len(self.results)}个数据源")
        report.append(f"成功: {self.success_count}个")
        report.append(f"失败: {self.error_count}个")
        report.append(f"跳过(无新数据): {self.skipped_count}个")
        report.append(f"新增记录总数: {self.new_records_count}条")
        report.append(f"更新记录总数: {self.updated_records_count}条")
        report.append("")
        
        # 按数据源分组
        sources = {}
        for result in self.results:
            source = result['source']
            if source not in sources:
                sources[source] = {'success': [], 'failed': []}
            
            if result['success']:
                sources[source]['success'].append(result)
            else:
                sources[source]['failed'].append(result)
        
        # 详细报告
        for source, data in sources.items():
            report.append(f"\n【{source.upper()}】")
            report.append("-" * 40)
            
            if data['success']:
                report.append("✅ 成功写入的数据源:")
                for result in data['success']:
                    report.append(f"  • {result['name']} ({result['code']}) - 新增{result['new_records']}条")
            
            if data['failed']:
                report.append("❌ 写入失败的数据源:")
                for result in data['failed']:
                    report.append(f"  • {result['name']} ({result['code']}) - {result['message']}")
        
        return "\n".join(report)
    
    def save_detailed_report(self, filename: str = 'macro_data_write_detailed.json'):
        """保存详细的JSON报告"""
        report_data = {
            'write_time': datetime.datetime.now().isoformat(),
            'summary': {
                'total': len(self.results),
                'success': self.success_count,
                'failed': self.error_count,
                'skipped': self.skipped_count,
                'new_records': self.new_records_count,
                'updated_records': self.updated_records_count
            },
            'results': self.results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"详细报告已保存到: {filename}")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='宏观数据写入脚本')
    parser.add_argument('--full', action='store_true', help='全量模式（默认为增量模式）')
    parser.add_argument('--workers', type=int, default=3, help='并发线程数（默认3）')
    args = parser.parse_args()
    
    incremental = not args.full
    
    writer = MacroDataWriter()
    
    try:
        # 写入所有数据源
        results = writer.write_all_assets(incremental=incremental, max_workers=args.workers)
        
        # 生成并显示报告
        report = writer.generate_report()
        print(report)
        
        # 保存详细报告
        writer.save_detailed_report()
        
        # 保存文本报告
        with open('macro_data_write_report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info("写入完成，报告已保存")
        
        return writer.error_count == 0
        
    except Exception as e:
        logger.error(f"写入过程出错: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)