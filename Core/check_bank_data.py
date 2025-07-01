import os
import sys
from dotenv import load_dotenv

# 添加路径以导入数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from DB.db_utils import get_db_connection

load_dotenv()

conn = get_db_connection()

cur = conn.cursor()

# 检查所有利率相关数据
cur.execute("""
    SELECT md.symbol, mdt.type_name, COUNT(*) as count, MAX(md.data_date) as latest_date
    FROM macro_data md 
    JOIN macro_data_types mdt ON md.type_id = mdt.id 
    WHERE md.symbol LIKE '%利率%' OR md.symbol LIKE '%bank%' OR mdt.type_name LIKE '%利率%'
    GROUP BY md.symbol, mdt.type_name
    ORDER BY count DESC
""")

rows = cur.fetchall()
print('央行利率数据统计:')
for row in rows:
    print(f'{row[0]} ({row[1]}): {row[2]}条, 最新日期: {row[3]}')

conn.close()