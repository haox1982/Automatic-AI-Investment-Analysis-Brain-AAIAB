import os
import psycopg2
import time
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

def get_db_connection():
    """Establishes a connection to the PostgreSQL database with retries."""
    retries = 5
    delay = 5 # seconds
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=os.environ.get("DB_HOST", "localhost"),
                port=os.environ.get("DB_PORT", 5432),
                dbname=os.environ.get("DB_NAME", "backtrader25"),
                user=os.environ.get("DB_USER", "n8n"),
                password=os.environ.get("DB_PASSWORD", "n8npass")
            )
            print("Database connection established successfully.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Attempt {i+1}/{retries}: Could not connect to the database. Retrying in {delay} seconds...")
            time.sleep(delay)
    print("Error: Could not connect to the database after several retries.")
    return None

def create_database_if_not_exists():
    """创建backtrader25数据库（如果不存在）"""
    try:
        # 连接到默认的postgres数据库来创建新数据库
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=os.environ.get("DB_PORT", 5432),
            dbname="postgres",  # 连接到默认数据库
            user=os.environ.get("DB_USER", "n8n"),
            password=os.environ.get("DB_PASSWORD", "n8npass")
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # 检查数据库是否存在
        db_name = os.environ.get("DB_NAME", "backtrader25")
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
            
        cur.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
        return False

def create_tables(conn):
    """Creates the necessary tables and indexes in the database."""
    commands = [
        # 宏观数据类型表
        """
        CREATE TABLE IF NOT EXISTS macro_data_types (
            id SERIAL PRIMARY KEY,
            type_code VARCHAR(50) UNIQUE NOT NULL,
            type_name VARCHAR(100) NOT NULL,
            description TEXT,
            unit VARCHAR(20),
            data_frequency VARCHAR(20), -- daily, weekly, monthly, quarterly, yearly
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
        
        # 宏观数据主表 - 重新设计以适应不同类型数据
        """
        CREATE TABLE IF NOT EXISTS macro_data (
            id SERIAL PRIMARY KEY,
            type_id INTEGER REFERENCES macro_data_types(id),
            source VARCHAR(50) NOT NULL,
            symbol VARCHAR(100) NOT NULL, -- 如DXY, GOLD, USD_CNY等
            data_date DATE NOT NULL,
            value DECIMAL(20,6),
            open_price DECIMAL(20,6),
            high_price DECIMAL(20,6),
            low_price DECIMAL(20,6),
            close_price DECIMAL(20,6),
            volume BIGINT,
            additional_data JSONB, -- 存储额外的特定数据
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(type_id, symbol, data_date)
        );
        """,
        
        # 宏观分析报告表
        """
        CREATE TABLE IF NOT EXISTS macro_analysis_reports (
            id SERIAL PRIMARY KEY,
            type_id INTEGER REFERENCES macro_data_types(id),
            symbol VARCHAR(100) NOT NULL,
            report_date DATE NOT NULL,
            analysis_period VARCHAR(50), -- 1Y, 2Y, 5Y等
            summary TEXT,
            key_metrics JSONB, -- 关键指标如收益率、波动率等
            chart_path VARCHAR(255),
            backtest_results JSONB, -- 回测结果
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(type_id, symbol, report_date, analysis_period)
        );
        """,
        
        # 投资组合持仓表
        """
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50),
            investor_name VARCHAR(100) NOT NULL,
            report_date DATE NOT NULL,
            asset_symbol VARCHAR(50) NOT NULL,
            asset_type VARCHAR(50), -- stock, bond, commodity, currency等
            holding_change TEXT,
            shares BIGINT,
            value_usd BIGINT,
            percentage DECIMAL(5,2), -- 占投资组合百分比
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(investor_name, report_date, asset_symbol)
        );
        """,
        
        # 市场情绪表
        """
        CREATE TABLE IF NOT EXISTS market_sentiments (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50),
            source_url VARCHAR(512) UNIQUE,
            publish_date DATE NOT NULL,
            title TEXT,
            summary TEXT,
            sentiment_score FLOAT,
            related_assets TEXT[], -- 相关资产数组
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
        
        # 创建索引
        "CREATE INDEX IF NOT EXISTS idx_macro_data_type_symbol ON macro_data(type_id, symbol);",
        "CREATE INDEX IF NOT EXISTS idx_macro_data_date ON macro_data(data_date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_macro_data_symbol ON macro_data(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_analysis_reports_type_symbol ON macro_analysis_reports(type_id, symbol);",
        "CREATE INDEX IF NOT EXISTS idx_analysis_reports_date ON macro_analysis_reports(report_date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_portfolio_investor ON portfolio_holdings(investor_name);",
        "CREATE INDEX IF NOT EXISTS idx_portfolio_date ON portfolio_holdings(report_date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_sentiments_date ON market_sentiments(publish_date DESC);"
    ]
    
    cur = conn.cursor()
    try:
        for command in commands:
            cur.execute(command)
        conn.commit()
        print("Tables and indexes created successfully (if they didn't exist).")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error executing SQL: {error}")
        conn.rollback()
    finally:
        cur.close()

def insert_initial_macro_data_types(conn):
    """插入初始的宏观数据类型"""
    initial_types = [
        ('CURRENCY', '货币汇率', '主要货币对汇率数据', 'Rate', 'daily'),
        ('INTEREST_RATE', '利率', '各国央行利率和债券收益率', '%', 'daily'),
        ('COMMODITY', '大宗商品', '黄金、原油、农产品等大宗商品价格', 'USD', 'daily'),
        ('INDEX', '指数', '美元指数、波动率指数等', 'Points', 'daily'),
        ('BOND', '债券', '国债收益率、企业债等', '%', 'daily'),
        ('ECONOMIC_INDICATOR', '经济指标', 'GDP、CPI、失业率等宏观经济指标', 'Various', 'monthly'),
        ('CRYPTO', '加密货币', '比特币、以太坊等加密货币', 'USD', 'daily')
    ]
    
    cur = conn.cursor()
    try:
        for type_code, type_name, description, unit, frequency in initial_types:
            cur.execute("""
                INSERT INTO macro_data_types (type_code, type_name, description, unit, data_frequency)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (type_code) DO NOTHING
            """, (type_code, type_name, description, unit, frequency))
        
        conn.commit()
        print("Initial macro data types inserted successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting initial data types: {error}")
        conn.rollback()
    finally:
        cur.close()

if __name__ == "__main__":
    print("Attempting to initialize the database...")
    
    # 首先创建数据库
    if create_database_if_not_exists():
        # 然后连接到新数据库并创建表
        connection = get_db_connection()
        if connection:
            create_tables(connection)
            insert_initial_macro_data_types(connection)
            connection.close()
            print("Database initialization completed successfully.")
        else:
            print("Failed to connect to the database.")
    else:
        print("Failed to create database.")