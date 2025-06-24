import os
import psycopg2
from psycopg2.extras import Json
import logging
from dotenv import load_dotenv

# 加载.env文件
load_dotenv('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader/.env')

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """建立并返回一个到PostgreSQL数据库的连接。"""
    try:
        # 调试信息：打印环境变量
        db_host = os.environ.get("DB_HOST", "localhost")
        db_port = os.environ.get("DB_PORT", 5432)
        db_name = os.environ.get("DB_NAME", "backtrader25")
        db_user = os.environ.get("DB_USER", "n8n")
        logging.info(f"尝试连接数据库: host={db_host}, port={db_port}, dbname={db_name}, user={db_user}")
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=os.environ.get("DB_PASSWORD", "n8npass")
        )
        logging.info("数据库连接成功")
        return conn
    except psycopg2.Error as e:
        logging.error(f"数据库连接错误: {e}")
        return None

def get_macro_data_type_id(type_code):
    """根据类型代码获取类型ID"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM macro_data_types WHERE type_code = %s", (type_code,))
        result = cur.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"获取数据类型ID错误: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def insert_macro_data(data):
    """
    将宏观数据插入到macro_data表中。
    
    参数:
    data (dict): 包含以下键的字典:
        - type_code: 数据类型代码 (如 'CURRENCY', 'INTEREST_RATE'等)
        - source: 数据源
        - symbol: 资产符号
        - data_date: 数据日期
        - value: 主要数值
        - open_price, high_price, low_price, close_price: OHLC价格（可选）
        - volume: 成交量（可选）
        - additional_data: 额外数据（字典格式）
    
    返回:
    bool: 插入成功返回True，失败返回False
    """
    conn = get_db_connection()
    if conn is None:
        return False
    
    try:
        cur = conn.cursor()
        
        # 获取数据类型ID
        type_id = get_macro_data_type_id(data.get('type_code'))
        if type_id is None:
            logging.error(f"未找到数据类型: {data.get('type_code')}")
            return False
        
        # 使用ON CONFLICT来处理重复数据
        insert_query = """
        INSERT INTO macro_data (type_id, source, symbol, data_date, value, 
                               open_price, high_price, low_price, close_price, 
                               volume, additional_data, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (type_id, symbol, data_date) 
        DO UPDATE SET 
            source = EXCLUDED.source,
            value = EXCLUDED.value,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            additional_data = EXCLUDED.additional_data,
            updated_at = NOW()
        """
        
        # 准备数据 - 处理嵌套字典的序列化问题
        additional_data = data.get('additional_data', {})
        if isinstance(additional_data, (dict, list)):
            # 递归处理嵌套的日期对象
            import json
            from datetime import datetime, date
            
            def serialize_datetime(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                elif isinstance(obj, dict):
                    return {k: serialize_datetime(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [serialize_datetime(item) for item in obj]
                else:
                    return obj
            
            serialized_data = serialize_datetime(additional_data)
            additional_data = Json(serialized_data)
        
        cur.execute(insert_query, (
            type_id,
            data.get('source'),
            data.get('symbol'),
            data.get('data_date'),
            data.get('value'),
            data.get('open_price'),
            data.get('high_price'),
            data.get('low_price'),
            data.get('close_price'),
            data.get('volume'),
            additional_data
        ))
        
        conn.commit()
        logging.info(f"成功插入/更新数据: {data.get('symbol')} on {data.get('data_date')}")
        return True
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"插入数据时发生错误: {error}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_macro_analysis_report(data):
    """
    将宏观分析报告插入到macro_analysis_reports表中。
    
    参数:
    data (dict): 包含以下键的字典:
        - type_code: 数据类型代码
        - symbol: 资产符号
        - report_date: 报告日期
        - analysis_period: 分析周期
        - summary: 摘要
        - key_metrics: 关键指标（字典格式）
        - chart_path: 图表路径
        - backtest_results: 回测结果（字典格式）
    """
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cur = conn.cursor()
        
        # 获取数据类型ID
        type_id = get_macro_data_type_id(data.get('type_code'))
        if type_id is None:
            logging.error(f"未找到数据类型: {data.get('type_code')}")
            return
        
        insert_query = """
        INSERT INTO macro_analysis_reports (type_id, symbol, report_date, analysis_period,
                                          summary, key_metrics, chart_path, backtest_results)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (type_id, symbol, report_date, analysis_period) 
        DO UPDATE SET 
            summary = EXCLUDED.summary,
            key_metrics = EXCLUDED.key_metrics,
            chart_path = EXCLUDED.chart_path,
            backtest_results = EXCLUDED.backtest_results,
            created_at = NOW()
        """
        
        # 准备JSON数据
        key_metrics = data.get('key_metrics', {})
        if isinstance(key_metrics, dict):
            key_metrics = Json(key_metrics)
            
        backtest_results = data.get('backtest_results', {})
        if isinstance(backtest_results, dict):
            backtest_results = Json(backtest_results)
        
        cur.execute(insert_query, (
            type_id,
            data.get('symbol'),
            data.get('report_date'),
            data.get('analysis_period'),
            data.get('summary'),
            key_metrics,
            data.get('chart_path'),
            backtest_results
        ))
        
        conn.commit()
        logging.info(f"成功插入/更新分析报告: {data.get('symbol')} - {data.get('analysis_period')}")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"插入分析报告时发生错误: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def get_data_coverage_by_symbol():
    """
    获取每个金融产品的数据覆盖时间范围。
    
    返回:
    list: 包含每个symbol的数据覆盖信息的列表，每个元素是一个字典，包含:
        - symbol: 资产符号
        - type_name: 数据类型名称
        - earliest_date: 最早数据日期
        - latest_date: 最新数据日期
        - total_records: 总记录数
        - source: 数据来源
    """
    conn = get_db_connection()
    if conn is None:
        return []
    
    try:
        cur = conn.cursor()
        
        query = """
        SELECT 
            md.symbol,
            mdt.type_name,
            MIN(md.data_date) as earliest_date,
            MAX(md.data_date) as latest_date,
            COUNT(*) as total_records,
            md.source
        FROM macro_data md
        JOIN macro_data_types mdt ON md.type_id = mdt.id
        GROUP BY md.symbol, mdt.type_name, md.source
        ORDER BY mdt.type_name, md.symbol;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        coverage_info = [{
            'symbol': row[0],
            'type_name': row[1],
            'earliest_date': row[2].strftime('%Y-%m-%d'),
            'latest_date': row[3].strftime('%Y-%m-%d'),
            'total_records': row[4],
            'source': row[5]
        } for row in results]
        
        return coverage_info
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"获取数据覆盖信息时发生错误: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()

# 你可以在这里添加其他表的插入函数，例如 insert_portfolio_holdings 等