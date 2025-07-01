#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技术分析图表生成器
使用Plotly生成动态的宏观经济数据技术分析图表
"""

import sys
import os
# 当前文件已在Core目录下，无需添加路径

import pandas as pd
import pandas_ta as ta
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from DB.db_utils import get_db_connection
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TechnicalAnalysisPlotter:
    def __init__(self):
        self.conn = get_db_connection()
        if not self.conn:
            raise Exception("无法连接到数据库")
        
        # 确保输出目录存在
        self.output_dir = '../plot_html'  # 输出到项目根目录的plot_html文件夹
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
    def save_chart_with_description(self, fig, filename, description=None, title=None):
        """
        保存图表并添加可折叠的描述区域
        """
        # 获取Plotly生成的HTML内容
        html_content = fig.to_html(include_plotlyjs='cdn')
        
        # 如果有描述文本，添加可折叠的描述区域
        if description:
            # 在<body>标签后插入描述区域 - 使用更高透明度的背景
            description_html = f'''
            <style>
                /* 全局样式 */
                .floating-container {{  
                    position: absolute;
                    z-index: 1000;
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                    transition: all 0.3s ease;
                }}
                
                /* 描述容器样式 */
                #description-container {{  
                    top: 10px;
                    left: 10px;
                    background: rgba(255,255,255,0.85); /* 调整透明度让文字更清晰 */
                    border: 1px solid rgba(200,200,200,0.4);
                    padding: 10px;
                    max-width: 380px;
                    overflow: hidden;
                }}
                
                /* 时间范围容器样式 */
                #time-range-container {{  
                    bottom: 60px;
                    right: 10px;
                    background: rgba(255,255,255,0.8); /* 稍微不透明一些 */
                    border: 1px solid rgba(200,200,200,0.5);
                    padding: 8px;
                    max-height: 40px;
                    overflow: hidden;
                    transition: max-height 0.3s ease;
                }}
                
                #time-range-container.expanded {{
                    max-height: 200px;
                }}
                
                #time-range-header {{
                    cursor: pointer;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    font-weight: bold;
                    font-size: 14px;
                    margin-bottom: 5px;
                }}
                
                #time-range-buttons {{
                    display: none;
                }}
                
                #time-range-container.expanded #time-range-buttons {{
                    display: block;
                }}
                
                /* 时间按钮样式 */
                .time-btn {{  
                    padding: 5px 10px;
                    margin: 3px 0;
                    border-radius: 4px;
                    border: 1px solid #ddd;
                    background: rgba(248,248,248,0.7);
                    cursor: pointer;
                    font-size: 12px;
                    transition: all 0.2s ease;
                }}
                
                .time-btn:hover {{  
                    background: rgba(230,247,255,0.9);
                }}
                
                .time-btn.active {{  
                    background: rgba(24,144,255,0.2);
                    border-color: rgba(24,144,255,0.5);
                    font-weight: bold;
                }}
                
                /* 当前时间戳样式 */
                #current-timestamp {{  
                    position: absolute;
                    bottom: 10px;
                    right: 10px;
                    background: rgba(255,255,255,0.5);
                    padding: 5px 10px;
                    border-radius: 4px;
                    font-size: 12px;
                    color: #666;
                    z-index: 1000;
                }}
            </style>
            
            <!-- 描述容器 -->
            <div id="description-container" class="floating-container">
                <div id="description-header" style="display: flex; justify-content: space-between; align-items: center; 
                     cursor: pointer;" onclick="toggleDescription()">
                    <h3 style="margin: 0; font-size: 16px; color: #333;">{title or '技术分析详情'} 📊</h3>
                    <button id="toggle-btn" style="background: none; border: none; font-size: 18px; cursor: pointer;">▼</button>
                </div>
                <div id="description-content" style="margin-top: 8px; font-size: 14px; line-height: 1.5; white-space: pre-line;">
                    {description}
                </div>
            </div>
            
            <!-- 时间范围选择按钮 -->
            <div id="time-range-container" class="floating-container">
                <div id="time-range-header" onclick="toggleTimeRange()">
                    <span>时间范围</span>
                    <span id="time-range-toggle">▲</span>
                </div>
                <div id="time-range-buttons">
                    <button class="time-btn" data-days="1">过去1天</button>
                    <button class="time-btn" data-days="7">过去1周</button>
                    <button class="time-btn" data-days="30">过去30天</button>
                    <button class="time-btn" data-days="90">过去90天</button>
                    <button class="time-btn active" data-days="365">过去1年</button>
                </div>
            </div>
            
            <!-- 当前时间戳显示 -->
            <div id="current-timestamp">当前日期: {datetime.now().strftime('%Y-%m-%d')}</div>
            
            <script>
                // 页面加载完成后执行
                document.addEventListener('DOMContentLoaded', function() {{
                    // 初始化时间范围按钮
                    initTimeRangeButtons();
                    
                    // 默认展开描述
                    document.getElementById('description-content').style.display = 'block';
                    
                    // 默认折叠时间范围选择器
                    document.getElementById('time-range-container').classList.remove('expanded');
                }});
                
                // 折叠描述功能
                function toggleDescription() {{
                    var content = document.getElementById('description-content');
                    var btn = document.getElementById('toggle-btn');
                    if (content.style.display === 'none') {{
                        content.style.display = 'block';
                        btn.innerHTML = '▼';
                    }} else {{
                        content.style.display = 'none';
                        btn.innerHTML = '▶';
                    }}
                }}
                
                // 折叠时间范围选择器功能
                function toggleTimeRange() {{
                    var container = document.getElementById('time-range-container');
                    var toggle = document.getElementById('time-range-toggle');
                    if (container.classList.contains('expanded')) {{
                        container.classList.remove('expanded');
                        toggle.innerHTML = '▲';
                    }} else {{
                        container.classList.add('expanded');
                        toggle.innerHTML = '▼';
                    }}
                }}
                
                // 初始化时间范围按钮
                function initTimeRangeButtons() {{
                    var buttons = document.querySelectorAll('.time-btn');
                    buttons.forEach(function(btn) {{
                        btn.addEventListener('click', function() {{
                            // 移除所有按钮的active类
                            buttons.forEach(function(b) {{
                                b.classList.remove('active');
                            }});
                            
                            // 为当前按钮添加active类
                            this.classList.add('active');
                            
                            // 更新时间范围
                            var days = parseInt(this.getAttribute('data-days'));
                            updateTimeRange(days);
                        }});
                    }});
                }}
                
                // 时间范围选择功能
                function updateTimeRange(days) {{
                    // 获取当前日期
                    var endDate = new Date();
                    // 计算开始日期
                    var startDate = new Date();
                    startDate.setDate(startDate.getDate() - days);
                    
                    // 格式化日期为ISO字符串
                    var startDateStr = startDate.toISOString().split('T')[0];
                    var endDateStr = endDate.toISOString().split('T')[0];
                    
                    // 更新所有图表的时间范围
                    var graphDivs = document.querySelectorAll('.js-plotly-plot');
                    graphDivs.forEach(function(div) {{
                        if (div && div.layout) {{
                            Plotly.relayout(div, {{
                                'xaxis.range': [startDateStr, endDateStr]
                            }}).catch(function(err) {{
                                console.log('Plotly relayout error:', err);
                            }});
                            
                            // 如果有多个子图，也更新它们的x轴
                            for (var i = 2; i <= 4; i++) {{
                                var xaxisKey = 'xaxis' + i + '.range';
                                var updateObj = {{}};
                                updateObj[xaxisKey] = [startDateStr, endDateStr];
                                Plotly.relayout(div, updateObj).catch(function(err) {{
                                    console.log('Plotly relayout error for subplot:', err);
                                }});
                            }}
                        }}
                    }});
                    
                    // 更新时间戳显示
                    var timestampElement = document.getElementById('current-timestamp');
                    if (timestampElement) {{
                        timestampElement.innerHTML = '时间范围: ' + startDateStr + ' 至 ' + endDateStr;
                    }}
                }}
            </script>
            '''
            
            # 在<body>标签后插入描述区域
            html_content = html_content.replace('<body>', '<body>' + description_html)
        
        # 写入文件
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logging.info(f"已保存: {filename}")
        return filename
    
    def get_data(self, symbol=None, type_name=None, days=365):
        """
        从数据库获取数据
        """
        try:
            # 特殊处理道琼斯指数
            original_symbol = symbol
            if symbol == '^DJI':
                logging.info(f"特殊处理 {symbol} 的数据获取...")
                # 尝试使用替代符号
                alt_symbol = 'DJI'
                logging.info(f"尝试使用替代符号 {alt_symbol} 获取数据...")
                
                # 先尝试使用原始符号
                df = self._execute_data_query(symbol, type_name, days)
                
                # 如果数据为空，尝试使用替代符号
                if df.empty:
                    logging.info(f"使用原始符号 {symbol} 未找到数据，尝试使用替代符号 {alt_symbol}...")
                    df_alt = self._execute_data_query(alt_symbol, type_name, days)
                    
                    if not df_alt.empty:
                        logging.info(f"使用替代符号 {alt_symbol} 成功获取到 {len(df_alt)} 条数据")
                        # 将替代符号的数据中的symbol替换为原始符号
                        if 'symbol' in df_alt.columns:
                            df_alt['symbol'] = original_symbol
                        return df_alt
                    else:
                        logging.warning(f"使用替代符号 {alt_symbol} 仍未找到数据")
                else:
                    logging.info(f"使用原始符号 {symbol} 成功获取到 {len(df)} 条数据")
                    return df
            
            # 常规数据获取
            return self._execute_data_query(symbol, type_name, days)
            
        except Exception as e:
            logging.error(f"获取数据时出错: {e}")
            return pd.DataFrame()
    
    def _execute_data_query(self, symbol=None, type_name=None, days=365):
        """
        执行数据库查询
        """
        try:
            cur = self.conn.cursor()
            
            # 构建查询条件
            where_conditions = []
            params = []
            
            if symbol:
                where_conditions.append("md.symbol = %s")
                params.append(symbol)
                logging.info(f"查询条件: symbol = {symbol}")
            
            if type_name:
                where_conditions.append("mdt.type_name = %s")
                params.append(type_name)
                logging.info(f"查询条件: type_name = {type_name}")
            
            # 添加日期限制
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            where_conditions.append("md.data_date >= %s")
            params.append(start_date)
            logging.info(f"查询条件: data_date >= {start_date.strftime('%Y-%m-%d')}")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT 
                md.symbol,
                md.data_date,
                md.value,
                md.open_price,
                md.high_price,
                md.low_price,
                md.close_price,
                md.volume,
                md.source,
                mdt.type_name,
                mdt.type_code
            FROM macro_data md
            JOIN macro_data_types mdt ON md.type_id = mdt.id
            WHERE {where_clause}
            ORDER BY md.symbol, md.data_date, 
                CASE md.source 
                    WHEN 'yfinance' THEN 1 
                    WHEN 'ak_forex' THEN 2 
                    ELSE 3 
                END
            """
            
            logging.info(f"执行查询: {query.strip()}")
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            
            logging.info(f"查询结果: 获取到 {len(data)} 条记录")
            
            if not data:
                logging.warning(f"查询 {symbol} 未返回任何数据")
                return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=columns)
            df['data_date'] = pd.to_datetime(df['data_date'])
            
            # 对于同一symbol和日期的重复数据，优先保留yfinance数据源
            if not df.empty and 'source' in df.columns:
                # 定义数据源优先级
                source_priority = {'yfinance': 1, 'ak_forex': 2}
                df['source_priority'] = df['source'].map(source_priority).fillna(3)
                
                # 按symbol、data_date分组，保留优先级最高的记录
                df = df.sort_values(['symbol', 'data_date', 'source_priority'])
                df = df.drop_duplicates(subset=['symbol', 'data_date'], keep='first')
                df = df.drop('source_priority', axis=1)
                
                logging.info(f"数据去重完成，剩余记录数: {len(df)}")
            
            return df
            
        except Exception as e:
            logging.error(f"执行数据库查询时出错: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df):
        """
        计算技术指标
        """
        if df.empty:
            return df
        
        # 使用close_price或value作为价格
        price_col = 'close_price' if 'close_price' in df.columns and df['close_price'].notna().any() else 'value'
        
        # 确保价格数据是数值类型，处理Decimal和None值
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        df = df.dropna(subset=[price_col])  # 删除价格为空的行
        
        if df.empty:
            logging.warning(f"处理后数据为空，无法计算技术指标")
            return df
        
        # 准备OHLCV数据用于pandas_ta
        df_ta = df.copy()
        df_ta['close'] = df_ta[price_col]
        
        # 如果有完整的OHLCV数据，使用pandas_ta计算指标
        has_ohlc = all(col in df.columns and df[col].notna().any() 
                      for col in ['open_price', 'high_price', 'low_price', 'close_price'])
        
        if has_ohlc:
            # 确保OHLC数据也是数值类型
            df_ta['open'] = pd.to_numeric(df_ta['open_price'], errors='coerce')
            df_ta['high'] = pd.to_numeric(df_ta['high_price'], errors='coerce')
            df_ta['low'] = pd.to_numeric(df_ta['low_price'], errors='coerce')
            if 'volume' in df.columns:
                df_ta['volume'] = pd.to_numeric(df_ta['volume'], errors='coerce').fillna(0)
            else:
                df_ta['volume'] = 0
        else:
            # 如果只有价格数据，用价格填充OHLC
            df_ta['open'] = df_ta['close']
            df_ta['high'] = df_ta['close']
            df_ta['low'] = df_ta['close']
            df_ta['volume'] = 0
        
        # 使用pandas_ta计算技术指标
        try:
            # 移动平均线
            df['MA5'] = ta.sma(df_ta['close'], length=5)
            df['MA10'] = ta.sma(df_ta['close'], length=10)
            df['MA20'] = ta.sma(df_ta['close'], length=20)
            df['MA50'] = ta.sma(df_ta['close'], length=50)
            
            # 布林带
            bb = ta.bbands(df_ta['close'], length=20)
            if bb is not None:
                df['BB_lower'] = bb.iloc[:, 0]
                df['BB_middle'] = bb.iloc[:, 1]
                df['BB_upper'] = bb.iloc[:, 2]
            
            # RSI
            df['RSI'] = ta.rsi(df_ta['close'], length=14)
            
            # MACD
            macd = ta.macd(df_ta['close'])
            if macd is not None:
                df['MACD'] = macd.iloc[:, 0]
                df['MACD_histogram'] = macd.iloc[:, 1]
                df['MACD_signal'] = macd.iloc[:, 2]
            
            # 随机指标
            stoch = ta.stoch(df_ta['high'], df_ta['low'], df_ta['close'])
            if stoch is not None:
                df['STOCH_K'] = stoch.iloc[:, 0]
                df['STOCH_D'] = stoch.iloc[:, 1]
            
            # ATR (平均真实波幅)
            df['ATR'] = ta.atr(df_ta['high'], df_ta['low'], df_ta['close'], length=14)
            
            # 威廉指标
            df['WILLIAMS_R'] = ta.willr(df_ta['high'], df_ta['low'], df_ta['close'], length=14)
            
        except Exception as e:
            logging.warning(f"使用pandas_ta计算技术指标时出错: {e}，回退到基础计算")
            # 回退到基础计算方法
            df['MA5'] = df[price_col].rolling(window=5).mean()
            df['MA10'] = df[price_col].rolling(window=10).mean()
            df['MA20'] = df[price_col].rolling(window=20).mean()
            df['MA50'] = df[price_col].rolling(window=50).mean()
            
            # 布林带
            df['BB_middle'] = df[price_col].rolling(window=20).mean()
            bb_std = df[price_col].rolling(window=20).std()
            df['BB_upper'] = df['BB_middle'] + (bb_std * 2)
            df['BB_lower'] = df['BB_middle'] - (bb_std * 2)
            
            # RSI
            delta = df[price_col].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # MACD
            exp1 = df[price_col].ewm(span=12).mean()
            exp2 = df[price_col].ewm(span=26).mean()
            df['MACD'] = exp1 - exp2
            df['MACD_signal'] = df['MACD'].ewm(span=9).mean()
            df['MACD_histogram'] = df['MACD'] - df['MACD_signal']
        
        return df
    
    def generate_analysis_summary(self, df, symbol):
        """
        生成技术分析文字描述
        """
        if df.empty:
            return "暂无数据"
        
        # 获取最新数据
        latest = df.iloc[-1]
        latest_date = latest['data_date'].strftime('%Y-%m-%d') if pd.notna(latest['data_date']) else '未知日期'
        
        # 检查是否有OHLC数据
        has_ohlc = all(col in df.columns and df[col].notna().any() 
                      for col in ['open_price', 'high_price', 'low_price', 'close_price'])
        
        summary_parts = []
        
        # 数据日期
        summary_parts.append(f"📅 数据日期: {latest_date}")
        summary_parts.append("")
        
        # OHLCV数据
        if has_ohlc:
            open_price = latest.get('open_price', 0)
            high_price = latest.get('high_price', 0)
            low_price = latest.get('low_price', 0)
            close_price = latest.get('close_price', 0)
            volume = latest.get('volume', 0)
            
            # 计算涨跌幅
            if len(df) > 1:
                prev_close = df.iloc[-2].get('close_price', close_price)
                change = close_price - prev_close
                change_pct = (change / prev_close * 100) if prev_close != 0 else 0
                change_symbol = "📈" if change >= 0 else "📉"
                summary_parts.append(f"💰 当日行情数据:")
                summary_parts.append(f"   开盘: {open_price:.4f} | 最高: {high_price:.4f}")
                summary_parts.append(f"   最低: {low_price:.4f} | 收盘: {close_price:.4f}")
                summary_parts.append(f"   {change_symbol} 涨跌: {change:+.4f} ({change_pct:+.2f}%)")
                if volume is not None and volume > 0:
                    summary_parts.append(f"   📊 成交量: {volume:,.0f}")
            else:
                summary_parts.append(f"💰 当日行情数据:")
                summary_parts.append(f"   开盘: {open_price:.4f} | 最高: {high_price:.4f}")
                summary_parts.append(f"   最低: {low_price:.4f} | 收盘: {close_price:.4f}")
                if volume is not None and volume > 0:
                    summary_parts.append(f"   📊 成交量: {volume:,.0f}")
        else:
            value = latest.get('value', 0)
            if len(df) > 1:
                prev_value = df.iloc[-2].get('value', value)
                change = value - prev_value
                change_pct = (change / prev_value * 100) if prev_value != 0 else 0
                change_symbol = "📈" if change >= 0 else "📉"
                summary_parts.append(f"💰 当前价格: {value:.4f}")
                summary_parts.append(f"   {change_symbol} 涨跌: {change:+.4f} ({change_pct:+.2f}%)")
            else:
                summary_parts.append(f"💰 当前价格: {value:.4f}")
        
        summary_parts.append("")
        
        # 技术指标分析
        summary_parts.append("🔍 技术指标分析:")
        
        # 移动平均线分析
        ma_analysis = []
        current_price = latest.get('close_price', latest.get('value', 0))
        for ma_period in [5, 10, 20, 50]:
            ma_col = f'MA{ma_period}'
            if ma_col in df.columns and pd.notna(latest.get(ma_col)):
                ma_value = latest[ma_col]
                position = "上方" if current_price > ma_value else "下方"
                ma_analysis.append(f"MA{ma_period}({ma_value:.4f}){position}")
        
        if ma_analysis:
            summary_parts.append(f"   📈 均线: {' | '.join(ma_analysis)}")
        
        # RSI分析
        if 'RSI' in df.columns and pd.notna(latest.get('RSI')):
            rsi = latest['RSI']
            if rsi >= 70:
                rsi_status = "超买区间⚠️"
            elif rsi <= 30:
                rsi_status = "超卖区间⚠️"
            else:
                rsi_status = "正常区间✅"
            summary_parts.append(f"   📊 RSI: {rsi:.2f} ({rsi_status})")
        
        # MACD分析
        if all(col in df.columns and pd.notna(latest.get(col)) for col in ['MACD', 'MACD_signal']):
            macd = latest['MACD']
            macd_signal = latest['MACD_signal']
            macd_trend = "金叉📈" if macd > macd_signal else "死叉📉"
            summary_parts.append(f"   📈 MACD: {macd:.6f} | 信号线: {macd_signal:.6f} ({macd_trend})")
        
        # 布林带分析
        if all(col in df.columns and pd.notna(latest.get(col)) for col in ['BB_upper', 'BB_lower', 'BB_middle']):
            bb_upper = latest['BB_upper']
            bb_lower = latest['BB_lower']
            bb_middle = latest['BB_middle']
            if current_price >= bb_upper:
                bb_position = "上轨附近(超买)⚠️"
            elif current_price <= bb_lower:
                bb_position = "下轨附近(超卖)⚠️"
            else:
                bb_position = "中轨区间✅"
            summary_parts.append(f"   📊 布林带: {bb_position}")
        
        # 其他技术指标
        other_indicators = []
        if 'STOCH_K' in df.columns and pd.notna(latest.get('STOCH_K')):
            stoch_k = latest['STOCH_K']
            stoch_status = "超买" if stoch_k >= 80 else "超卖" if stoch_k <= 20 else "正常"
            other_indicators.append(f"KDJ-K: {stoch_k:.2f}({stoch_status})")
        
        if 'WILLIAMS_R' in df.columns and pd.notna(latest.get('WILLIAMS_R')):
            wr = latest['WILLIAMS_R']
            wr_status = "超买" if wr >= -20 else "超卖" if wr <= -80 else "正常"
            other_indicators.append(f"WR: {wr:.2f}({wr_status})")
        
        if 'ATR' in df.columns and pd.notna(latest.get('ATR')):
            atr = latest['ATR']
            other_indicators.append(f"ATR: {atr:.4f}")
        
        if other_indicators:
            summary_parts.append(f"   🔍 其他指标: {' | '.join(other_indicators)}")
        
        summary_parts.append("")
        summary_parts.append("💡 提示: 技术分析仅供参考，投资需谨慎")
        
        return "\n".join(summary_parts)
    
    def generate_overview_summary(self, df, data_type):
        """
        生成汇总分析文字描述
        """
        if df.empty:
            return "暂无数据"
        
        # 获取最新日期
        latest_date = df['data_date'].max().strftime('%Y-%m-%d') if pd.notna(df['data_date'].max()) else '未知日期'
        
        # 统计信息
        total_assets = df['symbol'].nunique()
        date_range = (df['data_date'].max() - df['data_date'].min()).days if pd.notna(df['data_date'].max()) and pd.notna(df['data_date'].min()) else 0
        
        summary_parts = []
        
        # 标题和基本信息
        summary_parts.append(f"📊 {data_type} 类型资产汇总分析")
        summary_parts.append(f"📅 最新数据: {latest_date}")
        summary_parts.append(f"📈 包含资产: {total_assets}个 | 数据跨度: {date_range}天")
        summary_parts.append("")
        
        # 获取每个资产的最新数据进行分析
        latest_data = df.loc[df.groupby('symbol')['data_date'].idxmax()]
        
        if not latest_data.empty:
            # 价格分析
            price_col = 'close_price' if 'close_price' in latest_data.columns and latest_data['close_price'].notna().any() else 'value'
            
            if price_col in latest_data.columns:
                avg_price = latest_data[price_col].mean()
                max_price = latest_data[price_col].max()
                min_price = latest_data[price_col].min()
                max_symbol = latest_data.loc[latest_data[price_col].idxmax(), 'symbol']
                min_symbol = latest_data.loc[latest_data[price_col].idxmin(), 'symbol']
                
                summary_parts.append(f"💰 价格概况:")
                summary_parts.append(f"   平均价格: {avg_price:.4f}")
                summary_parts.append(f"   📈 最高: {max_symbol} ({max_price:.4f})")
                summary_parts.append(f"   📉 最低: {min_symbol} ({min_price:.4f})")
            
            # 计算涨跌情况（需要历史数据）
            rising_count = 0
            falling_count = 0
            
            for symbol in latest_data['symbol'].unique():
                symbol_df = df[df['symbol'] == symbol].sort_values('data_date')
                if len(symbol_df) >= 2:
                    latest_price = symbol_df.iloc[-1][price_col] if price_col in symbol_df.columns else 0
                    prev_price = symbol_df.iloc[-2][price_col] if price_col in symbol_df.columns else 0
                    
                    # 确保价格不为空值
                    if pd.notna(latest_price) and pd.notna(prev_price) and latest_price != 0 and prev_price != 0:
                        if latest_price > prev_price:
                            rising_count += 1
                        elif latest_price < prev_price:
                            falling_count += 1
            
            if rising_count + falling_count > 0:
                summary_parts.append("")
                summary_parts.append(f"📊 市场情绪:")
                summary_parts.append(f"   📈 上涨: {rising_count}个 | 📉 下跌: {falling_count}个")
                
                rising_pct = rising_count / (rising_count + falling_count) * 100
                if rising_pct >= 60:
                    market_sentiment = "偏多头🟢"
                elif rising_pct <= 40:
                    market_sentiment = "偏空头🔴"
                else:
                    market_sentiment = "中性🟡"
                
                summary_parts.append(f"   整体趋势: {market_sentiment} (上涨比例: {rising_pct:.1f}%)")
        
        summary_parts.append("")
        summary_parts.append("💡 提示: 数据仅供参考，投资需谨慎")
        
        return "\n".join(summary_parts)
    
    def generate_correlation_summary(self, correlation_matrix, symbols_list):
        """
        生成相关性分析文字描述
        """
        if correlation_matrix.empty:
            return "暂无相关性数据"
        
        summary_parts = []
        
        # 标题
        summary_parts.append("📊 资产相关性分析报告")
        summary_parts.append(f"📅 分析日期: {datetime.now().strftime('%Y-%m-%d')}")
        summary_parts.append(f"📈 分析资产: {len(symbols_list)}个")
        summary_parts.append("")
        
        # 找出最高和最低相关性（排除自相关）
        corr_values = []
        pairs = []
        
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                symbol1 = correlation_matrix.columns[i]
                symbol2 = correlation_matrix.columns[j]
                corr_value = correlation_matrix.iloc[i, j]
                
                if pd.notna(corr_value):
                    corr_values.append(corr_value)
                    pairs.append((symbol1, symbol2, corr_value))
        
        if pairs:
            # 排序找出极值
            pairs_sorted = sorted(pairs, key=lambda x: x[2], reverse=True)
            
            # 最高相关性
            highest_corr = pairs_sorted[0]
            summary_parts.append(f"🔗 最高正相关:")
            summary_parts.append(f"   {highest_corr[0]} ↔ {highest_corr[1]}")
            summary_parts.append(f"   相关系数: {highest_corr[2]:.3f}")
            
            # 最低相关性
            lowest_corr = pairs_sorted[-1]
            summary_parts.append(f"")
            summary_parts.append(f"📉 最低相关性:")
            summary_parts.append(f"   {lowest_corr[0]} ↔ {lowest_corr[1]}")
            summary_parts.append(f"   相关系数: {lowest_corr[2]:.3f}")
            
            # 统计相关性分布
            strong_positive = sum(1 for _, _, corr in pairs if corr > 0.7)
            moderate_positive = sum(1 for _, _, corr in pairs if 0.3 < corr <= 0.7)
            weak_correlation = sum(1 for _, _, corr in pairs if -0.3 <= corr <= 0.3)
            negative_correlation = sum(1 for _, _, corr in pairs if corr < -0.3)
            
            summary_parts.append("")
            summary_parts.append(f"📊 相关性分布:")
            summary_parts.append(f"   🟢 强正相关(>0.7): {strong_positive}对")
            summary_parts.append(f"   🟡 中等正相关(0.3-0.7): {moderate_positive}对")
            summary_parts.append(f"   ⚪ 弱相关(-0.3-0.3): {weak_correlation}对")
            summary_parts.append(f"   🔴 负相关(<-0.3): {negative_correlation}对")
            
            # 平均相关性
            avg_corr = sum(corr_values) / len(corr_values)
            summary_parts.append(f"")
            summary_parts.append(f"📈 平均相关系数: {avg_corr:.3f}")
            
            if avg_corr > 0.5:
                market_status = "高度关联🔴"
            elif avg_corr > 0.2:
                market_status = "中度关联🟡"
            else:
                market_status = "低度关联🟢"
            
            summary_parts.append(f"🎯 市场关联度: {market_status}")
        
        summary_parts.append("")
        summary_parts.append("💡 提示: 相关性分析有助于资产配置和风险管理")
        
        return "\n".join(summary_parts)
    
    def generate_performance_summary(self, symbols_list, days):
        """
        生成绩效分析文字描述
        """
        summary_parts = []
        
        # 标题
        summary_parts.append("📊 资产绩效对比分析")
        summary_parts.append(f"📅 分析期间: {days}天")
        summary_parts.append(f"📈 对比资产: {len(symbols_list)}个")
        summary_parts.append("")
        
        # 收集各资产的绩效数据
        performance_data = []
        
        for symbol in symbols_list:
            df = self.get_data(symbol=symbol, days=days)
            if not df.empty:
                df = df.sort_values('data_date')
                price_col = 'close_price' if 'close_price' in df.columns and df['close_price'].notna().any() else 'value'
                
                if len(df) >= 2:
                    start_price = float(df.iloc[0][price_col])
                    end_price = float(df.iloc[-1][price_col])
                    total_return = (end_price - start_price) / start_price * 100
                    
                    # 计算波动率
                    df[price_col] = df[price_col].astype(float)
                    df['returns'] = df[price_col].pct_change()
                    volatility = float(df['returns'].std()) * (252 ** 0.5) * 100  # 年化波动率
                    
                    # 计算最大回撤
                    df['cumulative'] = (1 + df['returns']).cumprod()
                    df['running_max'] = df['cumulative'].expanding().max()
                    df['drawdown'] = (df['cumulative'] - df['running_max']) / df['running_max']
                    max_drawdown = float(df['drawdown'].min()) * 100
                    
                    performance_data.append({
                        'symbol': symbol,
                        'total_return': total_return,
                        'volatility': volatility,
                        'max_drawdown': max_drawdown,
                        'sharpe_ratio': total_return / volatility if volatility > 0 else 0
                    })
        
        if performance_data:
            # 排序找出最佳和最差表现
            performance_data.sort(key=lambda x: x['total_return'], reverse=True)
            
            best_performer = performance_data[0]
            worst_performer = performance_data[-1]
            
            summary_parts.append(f"🏆 最佳表现:")
            summary_parts.append(f"   {best_performer['symbol']}: {best_performer['total_return']:+.2f}%")
            summary_parts.append(f"   波动率: {best_performer['volatility']:.2f}% | 最大回撤: {best_performer['max_drawdown']:.2f}%")
            
            summary_parts.append(f"")
            summary_parts.append(f"📉 最差表现:")
            summary_parts.append(f"   {worst_performer['symbol']}: {worst_performer['total_return']:+.2f}%")
            summary_parts.append(f"   波动率: {worst_performer['volatility']:.2f}% | 最大回撤: {worst_performer['max_drawdown']:.2f}%")
            
            # 统计概况
            avg_return = sum(p['total_return'] for p in performance_data) / len(performance_data)
            avg_volatility = sum(p['volatility'] for p in performance_data) / len(performance_data)
            
            positive_count = sum(1 for p in performance_data if p['total_return'] > 0)
            negative_count = len(performance_data) - positive_count
            
            summary_parts.append(f"")
            summary_parts.append(f"📊 整体概况:")
            summary_parts.append(f"   平均收益: {avg_return:+.2f}% | 平均波动: {avg_volatility:.2f}%")
            summary_parts.append(f"   📈 正收益: {positive_count}个 | 📉 负收益: {negative_count}个")
            
            # 风险收益评估
            if avg_return > 10 and avg_volatility < 20:
                risk_assessment = "高收益低风险🟢"
            elif avg_return > 0 and avg_volatility < 30:
                risk_assessment = "稳健增长🟡"
            elif avg_return < 0:
                risk_assessment = "整体下跌🔴"
            else:
                risk_assessment = "高风险🟠"
            
            summary_parts.append(f"🎯 风险评估: {risk_assessment}")
        
        summary_parts.append("")
        summary_parts.append("💡 提示: 过往表现不代表未来收益，投资需谨慎")
        
        return "\n".join(summary_parts)
    
    def create_candlestick_chart(self, df, symbol, title_suffix=""):
        """
        创建K线图
        """
        if df.empty:
            return None
        
        # 生成技术分析文字描述
        analysis_summary = self.generate_analysis_summary(df, symbol)
        
        # 检查是否有OHLC数据
        has_ohlc = all(col in df.columns and df[col].notna().any() 
                      for col in ['open_price', 'high_price', 'low_price', 'close_price'])
        
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=[f'{symbol} 价格走势', '成交量', 'RSI', 'MACD'],
            row_heights=[0.5, 0.15, 0.175, 0.175]
        )
        
        if has_ohlc:
            # K线图
            fig.add_trace(
                go.Candlestick(
                    x=df['data_date'],
                    open=df['open_price'],
                    high=df['high_price'],
                    low=df['low_price'],
                    close=df['close_price'],
                    name='K线'
                ),
                row=1, col=1
            )
        else:
            # 线图
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['value'],
                    mode='lines',
                    name='价格',
                    line=dict(color='blue')
                ),
                row=1, col=1
            )
        
        # 移动平均线
        for ma, color in [('MA5', 'orange'), ('MA10', 'red'), ('MA20', 'green'), ('MA50', 'purple')]:
            if ma in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df['data_date'],
                        y=df[ma],
                        mode='lines',
                        name=ma,
                        line=dict(color=color, width=1)
                    ),
                    row=1, col=1
                )
        
        # 布林带
        if all(col in df.columns for col in ['BB_upper', 'BB_middle', 'BB_lower']):
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['BB_upper'],
                    mode='lines',
                    name='布林带上轨',
                    line=dict(color='gray', width=1, dash='dash'),
                    showlegend=False
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['BB_lower'],
                    mode='lines',
                    name='布林带下轨',
                    line=dict(color='gray', width=1, dash='dash'),
                    fill='tonexty',
                    fillcolor='rgba(128,128,128,0.1)',
                    showlegend=False
                ),
                row=1, col=1
            )
        
        # RSI - 移到成交量下方
        if 'RSI' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['RSI'],
                    mode='lines',
                    name='RSI',
                    line=dict(color='purple')
                ),
                row=3, col=1
            )
            # RSI超买超卖线
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1)
        
        # MACD - 移到最下方
        if all(col in df.columns for col in ['MACD', 'MACD_signal', 'MACD_histogram']):
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['MACD'],
                    mode='lines',
                    name='MACD',
                    line=dict(color='blue')
                ),
                row=4, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['MACD_signal'],
                    mode='lines',
                    name='MACD信号线',
                    line=dict(color='red')
                ),
                row=4, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=df['data_date'],
                    y=df['MACD_histogram'],
                    name='MACD柱状图',
                    marker_color='gray'
                ),
                row=4, col=1
            )
        
        # 成交量 - 移到价格走势图下方
        if 'volume' in df.columns and df['volume'].notna().any():
            fig.add_trace(
                go.Bar(
                    x=df['data_date'],
                    y=df['volume'],
                    name='成交量',
                    marker_color='lightblue'
                ),
                row=2, col=1
            )
        
        # 更新布局
        fig.update_layout(
            title={
                'text': f'{symbol} 技术分析图表{title_suffix}',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}
            },
            xaxis_rangeslider_visible=False,
            height=1000,  # 增加高度以容纳文字描述
            showlegend=True,
            margin=dict(t=120, b=50, l=50, r=50),  # 增加顶部边距
            font=dict(size=12)
        )
        
        # 生成技术分析描述文本
        description_parts = []
        
        # 基本信息
        latest_data = df.iloc[-1]
        current_date = datetime.now().strftime('%Y-%m-%d')
        description_parts.append(f"📅 最新数据日期: {latest_data['data_date'].strftime('%Y-%m-%d')}")
        
        if has_ohlc:
            description_parts.append(f"💰 最新价格: {latest_data['close_price']:.4f}")
            description_parts.append(f"📈 开盘: {latest_data['open_price']:.4f} | 最高: {latest_data['high_price']:.4f}")
            description_parts.append(f"📉 最低: {latest_data['low_price']:.4f} | 收盘: {latest_data['close_price']:.4f}")
            
            # 计算涨跌幅
            if len(df) > 1:
                prev_close = df.iloc[-2]['close_price']
                change = latest_data['close_price'] - prev_close
                change_pct = (change / prev_close) * 100
                change_symbol = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                description_parts.append(f"{change_symbol} 涨跌: {change:+.4f} ({change_pct:+.2f}%)")
        else:
            description_parts.append(f"💰 最新价值: {latest_data['value']:.4f}")
        
        description_parts.append("")
        
        # 技术指标分析
        description_parts.append("🔍 技术指标分析:")
        
        # 移动平均线分析
        ma_analysis = []
        for ma in ['MA5', 'MA10', 'MA20', 'MA50']:
            if ma in df.columns and not pd.isna(latest_data[ma]):
                current_price = latest_data['close_price'] if has_ohlc else latest_data['value']
                ma_value = latest_data[ma]
                position = "上方" if current_price > ma_value else "下方"
                ma_analysis.append(f"{ma}({ma_value:.4f}){position}")
        
        if ma_analysis:
            description_parts.append(f"📊 移动平均线: {' | '.join(ma_analysis)}")
        
        # RSI分析
        if 'RSI' in df.columns and not pd.isna(latest_data['RSI']):
            rsi_value = latest_data['RSI']
            if rsi_value > 70:
                rsi_status = "超买区域🔴"
            elif rsi_value < 30:
                rsi_status = "超卖区域🟢"
            else:
                rsi_status = "正常区域🟡"
            description_parts.append(f"⚡ RSI: {rsi_value:.2f} ({rsi_status})")
        
        # MACD分析
        if all(col in df.columns for col in ['MACD', 'MACD_signal']) and not pd.isna(latest_data['MACD']):
            macd_value = latest_data['MACD']
            signal_value = latest_data['MACD_signal']
            macd_trend = "金叉🟢" if macd_value > signal_value else "死叉🔴"
            description_parts.append(f"📈 MACD: {macd_value:.4f} | 信号线: {signal_value:.4f} ({macd_trend})")
        
        # 布林带分析
        if all(col in df.columns for col in ['BB_upper', 'BB_middle', 'BB_lower']):
            current_price = latest_data['close_price'] if has_ohlc else latest_data['value']
            bb_upper = latest_data['BB_upper']
            bb_lower = latest_data['BB_lower']
            bb_middle = latest_data['BB_middle']
            
            if current_price > bb_upper:
                bb_position = "上轨上方(超买)🔴"
            elif current_price < bb_lower:
                bb_position = "下轨下方(超卖)🟢"
            else:
                bb_position = "正常区间🟡"
            description_parts.append(f"📊 布林带: {bb_position}")
            description_parts.append(f"   上轨: {bb_upper:.4f} | 中轨: {bb_middle:.4f} | 下轨: {bb_lower:.4f}")
        
        # 成交量分析
        if 'volume' in df.columns and df['volume'].notna().any():
            latest_volume = latest_data['volume']
            if latest_volume is not None and latest_volume > 0:
                avg_volume = df['volume'].tail(20).mean()  # 20日平均成交量
                volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1
                volume_status = "放量" if volume_ratio > 1.5 else "缩量" if volume_ratio < 0.5 else "正常"
                description_parts.append(f"📊 成交量: {latest_volume:,.0f} ({volume_status}, 比20日均量{volume_ratio:.1f}倍)")
            else:
                description_parts.append("📊 成交量: 无数据")
        
        description_parts.append("")
        description_parts.append("⚠️ 免责声明: 本分析仅供参考，投资有风险，决策需谨慎")
        
        # 将描述文本存储到图表对象中，供保存时使用
        fig._description_text = "\n".join(description_parts)
        fig._chart_title = f"{symbol} 技术分析"
        
        return fig
    
    def create_correlation_heatmap(self, symbols_list, days=365):
        """
        创建相关性热力图
        """
        correlation_data = {}
        
        for symbol in symbols_list:
            df = self.get_data(symbol=symbol, days=days)
            if not df.empty:
                df = df.set_index('data_date')
                price_col = 'close_price' if 'close_price' in df.columns and df['close_price'].notna().any() else 'value'
                correlation_data[symbol] = df[price_col]
        
        if not correlation_data:
            return None
        
        # 创建相关性矩阵
        corr_df = pd.DataFrame(correlation_data)
        corr_matrix = corr_df.corr()
        
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index,
            colorscale='RdBu',
            zmid=0,
            text=np.round(corr_matrix.values, 2),
            texttemplate="%{text}",
            textfont={"size": 10},
            hoverongaps=False
        ))
        
        # 生成相关性分析描述
        correlation_summary = self.generate_correlation_summary(corr_matrix, corr_matrix.columns.tolist())
        
        fig.update_layout(
            title={
                'text': '资产相关性热力图',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='资产',
            yaxis_title='资产',
            height=700,
            margin=dict(t=100, b=50, l=50, r=50),
            font=dict(size=11)
        )
        
        # 将描述文本存储到图表对象中，供保存时使用
        fig._description_text = correlation_summary
        fig._chart_title = "相关性分析"
        
        return fig
    
    def create_performance_comparison(self, symbols_list, days=365):
        """
        创建绩效对比图
        """
        fig = go.Figure()
        
        for symbol in symbols_list:
            df = self.get_data(symbol=symbol, days=days)
            if not df.empty:
                df = df.sort_values('data_date')
                price_col = 'close_price' if 'close_price' in df.columns and df['close_price'].notna().any() else 'value'
                
                # 计算累计收益率
                df['returns'] = df[price_col].pct_change()
                df['cumulative_returns'] = (1 + df['returns']).cumprod() - 1
                
                fig.add_trace(
                    go.Scatter(
                        x=df['data_date'],
                        y=df['cumulative_returns'] * 100,
                        mode='lines',
                        name=symbol,
                        line=dict(width=2)
                    )
                )
        
        # 生成绩效分析描述
        performance_summary = self.generate_performance_summary(symbols_list, days)
        
        fig.update_layout(
            title={
                'text': '资产绩效对比图',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='日期',
            yaxis_title='累计收益率 (%)',
            height=700,
            hovermode='x unified',
            margin=dict(t=100, b=50, l=50, r=50),
            font=dict(size=11)
        )
        
        # 将描述文本存储到图表对象中，供保存时使用
        fig._description_text = performance_summary
        fig._chart_title = "绩效分析"
        
        return fig
    
    def generate_cny_currency_charts(self, processed_symbols):
        """
        专门处理人民币相关货币对的图表生成
        """
        logging.info("开始生成人民币相关货币对图表...")
        
        # 定义人民币相关货币对
        cny_currencies = {
            '美元兑人民币': '美元兑人民币',
            '欧元兑人民币': '欧元兑人民币', 
            '英镑兑人民币': '英镑兑人民币',
            '日元兑人民币': '日元兑人民币',
            '港币兑人民币': '港币兑人民币',
            '人民币兑美元': '人民币兑美元',
            '人民币兑日元': '人民币兑日元',
            '人民币兑欧元': '人民币兑欧元',
            'USDCNY=X': '美元兑人民币',
            'EURCNY=X': '欧元兑人民币',
            'GBPCNY=X': '英镑兑人民币',
            'JPYCNY=X': '日元兑人民币',
            'CNH=X': '离岸人民币'
        }
        
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT DISTINCT md.symbol, mdt.type_name, COUNT(*) as record_count
                FROM macro_data md
                JOIN macro_data_types mdt ON md.type_id = mdt.id
                WHERE md.data_date >= %s
                  AND (md.symbol LIKE '%人民币%' OR md.symbol LIKE '%CNY%' OR md.symbol LIKE '%CNH%')
                GROUP BY md.symbol, mdt.type_name
                HAVING COUNT(*) >= 10
                ORDER BY record_count DESC
            """, (datetime.now() - timedelta(days=365),))
            
            cny_assets = cur.fetchall()
            
            for symbol, type_name, count in cny_assets:
                # 跳过已经处理过的资产
                if symbol in processed_symbols:
                    continue
                    
                try:
                    logging.info(f"正在生成人民币货币对 {symbol} 的技术分析图表...")
                    
                    df = self.get_data(symbol=symbol, days=365)
                    if df.empty:
                        logging.warning(f"{symbol} 没有足够的数据")
                        continue
                    
                    # 数据预处理 - 确保volume字段存在且处理None值
                    if 'volume' not in df.columns:
                        df['volume'] = 0
                    else:
                        df['volume'] = df['volume'].fillna(0)
                    
                    # 确保所有价格字段都不为空
                    price_columns = ['open_price', 'high_price', 'low_price', 'close_price']
                    for col in price_columns:
                        if col in df.columns:
                            df[col] = df[col].fillna(method='ffill').fillna(method='bfill')
                    
                    # 检查是否有足够的有效数据
                    if df[price_columns].isnull().all().all():
                        logging.warning(f"{symbol} 所有价格数据都为空")
                        continue
                    
                    logging.info(f"{symbol} 数据处理成功，开始计算技术指标...")
                    
                    df = self.calculate_technical_indicators(df)
                    
                    # 获取显示名称
                    display_name = cny_currencies.get(symbol, symbol)
                    
                    fig = self.create_candlestick_chart(df, symbol, f" ({type_name})")
                    
                    if fig:
                        filename = f"{self.output_dir}/{display_name}_technical_analysis.html"
                        self.save_chart_with_description(
                            fig, filename, 
                            getattr(fig, '_description_text', None),
                            getattr(fig, '_chart_title', None)
                        )
                        processed_symbols.add(symbol)
                        logging.info(f"成功生成 {display_name} 图表")
                    
                except Exception as e:
                    logging.error(f"生成人民币货币对 {symbol} 图表时出错: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"获取人民币货币对数据时出错: {e}")
        
        logging.info("人民币相关货币对图表生成完成")
    
    def fix_database_symbols(self):
        """
        检查并修复数据库中的符号问题，特别是将中文符号更新为标准英文符号
        """
        try:
            # 定义符号映射
            symbol_mappings = {
                '道琼斯指数': '^DJI'
            }
            
            # 检查当前符号
            logging.info("检查数据库中的符号...")
            cur = self.conn.cursor()
            cur.execute("""
                SELECT symbol, COUNT(*) 
                FROM macro_data 
                WHERE symbol IN ('道琼斯指数') 
                GROUP BY symbol;
            """)
            
            results = cur.fetchall()
            
            if results:
                logging.info("发现需要更新的符号:")
                for row in results:
                    logging.info(f"{row[0]} | {row[1]} 条记录")
                
                # 更新符号
                for old_symbol, new_symbol in symbol_mappings.items():
                    logging.info(f"更新符号: {old_symbol} -> {new_symbol}")
                    
                    cur.execute("""
                        UPDATE macro_data 
                        SET symbol = %s 
                        WHERE symbol = %s;
                    """, (new_symbol, old_symbol))
                    
                    # 获取更新的行数
                    rows_updated = cur.rowcount
                    logging.info(f"已更新 {rows_updated} 行数据")
                
                # 提交事务
                self.conn.commit()
                logging.info("所有符号更新已提交到数据库")
                
                # 验证更新
                logging.info("验证符号更新...")
                cur.execute("""
                    SELECT symbol, COUNT(*) 
                    FROM macro_data 
                    WHERE symbol IN ('^DJI') 
                    GROUP BY symbol;
                """)
                
                results = cur.fetchall()
                for row in results:
                    logging.info(f"更新后: {row[0]} | {row[1]} 条记录")
            else:
                logging.info("未发现需要更新的符号")
                
        except Exception as e:
            logging.error(f"修复数据库符号时出错: {e}")
            self.conn.rollback()
    
    def generate_specific_charts(self, symbols=None):
        """
        生成特定符号的技术分析图表
        
        Args:
            symbols: 要生成图表的符号列表，如果为None，则使用默认的重要符号
        """
        if symbols is None:
            # 默认生成道琼斯指数的图表
            symbols = ['^DJI']
        
        # 符号到显示名称的映射
        symbol_display_map = {
            '^DJI': '道琼斯指数'
        }
        
        logging.info(f"开始为特定符号生成技术分析图表: {symbols}")
        
        # 首先生成央行利率汇总图表
        try:
            logging.info("正在生成各国央行利率汇总图表...")
            self.generate_central_bank_rates_chart()
        except Exception as e:
            import traceback
            logging.error(f"生成央行利率图表时出错: {e}")
            logging.error(traceback.format_exc())
        
        # 为每个符号生成图表
        for symbol in symbols:
            try:
                # 获取显示名称
                display_name = symbol_display_map.get(symbol, symbol)
                logging.info(f"正在为 {display_name} ({symbol}) 生成技术分析图表...")
                
                # 获取数据
                df = self.get_data(symbol=symbol, days=365)
                
                if df.empty:
                    logging.error(f"无法获取 {symbol} 的数据，请检查数据库")
                    continue
                
                # 计算技术指标
                df = self.calculate_technical_indicators(df)
                
                # 创建图表
                fig = self.create_candlestick_chart(df, symbol)
                
                if fig:
                    # 使用显示名称作为文件名
                    filename = f"{self.output_dir}/{display_name}_technical_analysis.html"
                    self.save_chart_with_description(
                        fig, filename, 
                        getattr(fig, '_description_text', None),
                        getattr(fig, '_chart_title', None)
                    )
                    logging.info(f"已生成 {display_name} 的技术分析图表: {filename}")
                else:
                    logging.warning(f"创建 {symbol} 的图表失败，fig 为 None")
                    
            except Exception as e:
                logging.error(f"生成 {symbol} 图表时出错: {e}")
        
        logging.info("特定符号的图表生成完成")
    
    def generate_all_charts(self):
        """
        生成所有图表
        """
        logging.info("开始生成技术分析图表...")
        
        # 首先生成央行利率汇总图表
        try:
            logging.info("正在生成各国央行利率汇总图表...")
            self.generate_central_bank_rates_chart()
        except Exception as e:
            import traceback
            logging.error(f"生成央行利率图表时出错: {e}")
            logging.error(traceback.format_exc())
        
        # 首先修复数据库中的符号问题
        self.fix_database_symbols()
        
        # 定义优先资产列表（确保重要资产优先生成图表）
        priority_assets = {
            # 中国指数（使用数据库中的实际symbol）
            '上证指数': '上证指数',
            '深证成指': '深证成指', 
            '沪深300': '沪深300',
            '中证500': '中证500',
            # 美国指数
            '^GSPC': '标普500指数',
            '^DJI': '道琼斯指数',
            '^IXIC': '纳斯达克指数',
            # 外汇
            'DX-Y.NYB': '美元指数',
            'EUR=X': '欧元兑美元',
            'GBP=X': '英镑兑美元',
            'JPY=X': '美元兑日元',
            'USDCNY=X': '美元兑人民币',
            'EURCNY=X': '欧元兑人民币',
            'GBPCNY=X': '英镑兑人民币',
            'JPYCNY=X': '日元兑人民币',
            'CNH=X': '离岸人民币',
            # 数据库中的中文名称映射
            '美元指数': '美元指数',
            '欧元兑美元': '欧元兑美元',
            '英镑兑美元': '英镑兑美元',
            '美元兑日元': '美元兑日元',
            '日元兑美元': '日元兑美元',
            '美元兑人民币': '美元兑人民币',
            '欧元兑人民币': '欧元兑人民币',
            '英镑兑人民币': '英镑兑人民币',
            '日元兑人民币': '日元兑人民币',
            '离岸人民币': '离岸人民币',
            # 大宗商品（使用数据库中的实际symbol）
            '原油期货': '原油期货',
            '白银': '白银',
            # 黄金现货数据（上海黄金交易所）
            '上海金Au99.99': '上海金Au99.99',
            '上海金Au100g': '上海金Au100g',
            '上海金Au(T+D)': '上海金Au(T+D)',
            # 加密货币
            'BTC-USD': '比特币',
            # 利率数据
            '各国央行利率': '各国央行利率'
        }
        
        # 获取所有可用的资产
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT DISTINCT md.symbol, mdt.type_name, COUNT(*) as record_count
                FROM macro_data md
                JOIN macro_data_types mdt ON md.type_id = mdt.id
                WHERE md.data_date >= %s
                GROUP BY md.symbol, mdt.type_name
                HAVING COUNT(*) >= 20
                ORDER BY record_count DESC
            """, (datetime.now() - timedelta(days=365),))
            
            assets = cur.fetchall()
            
        except Exception as e:
            logging.error(f"获取资产列表时出错: {e}")
            return
        
        # 创建资产映射字典
        available_assets = {asset[0]: asset[1] for asset in assets}
        
        # 首先为优先资产生成图表
        processed_symbols = set()
        for symbol, display_name in priority_assets.items():
            if symbol in available_assets:
                try:
                    logging.info(f"正在生成 {display_name} ({symbol}) 的技术分析图表...")
                    
                    # 特殊处理道琼斯指数
                    if symbol in ['^DJI']:
                        logging.info(f"特殊处理 {symbol} ({display_name})...")
                        special_display_name = {'^DJI': '道琼斯指数'}.get(symbol, symbol)
                        logging.info(f"将使用特殊显示名称: {special_display_name}")
                    
                    df = self.get_data(symbol=symbol, days=365)
                    if df.empty:
                        logging.warning(f"获取 {symbol} 数据为空，跳过生成")
                        # 记录更详细的信息
                        if symbol in ['^DJI']:
                            logging.error(f"无法获取 {symbol} ({display_name}) 的数据，请检查数据库中是否存在该资产的数据")
                            alt_symbols = {'^DJI': 'DJI'}
                            alt_symbol = alt_symbols.get(symbol, symbol)
                            logging.error(f"尝试使用替代符号 {alt_symbol} 仍未找到数据")
                        continue
                    
                    df = self.calculate_technical_indicators(df)
                    fig = self.create_candlestick_chart(df, symbol, f" ({available_assets[symbol]})")
                    
                    if fig:
                        # 使用中文显示名称作为文件名
                        filename = f"{self.output_dir}/{display_name}_technical_analysis.html"
                        self.save_chart_with_description(
                            fig, filename, 
                            getattr(fig, '_description_text', None),
                            getattr(fig, '_chart_title', None)
                        )
                        processed_symbols.add(symbol)
                        logging.info(f"已生成 {display_name} ({symbol}) 的技术分析图表: {filename}")
                        
                        # 确保符号和显示名称的映射正确
                        if symbol in ['^DJI']:
                            special_display_name = {'^DJI': '道琼斯指数'}.get(symbol, symbol)
                            special_filename = f"{self.output_dir}/{special_display_name}_technical_analysis.html"
                            # 如果文件名不同，则复制一份
                            if special_filename != filename:
                                logging.info(f"为 {symbol} 创建额外的文件: {special_filename}")
                                self.save_chart_with_description(
                                    fig, special_filename, 
                                    getattr(fig, '_description_text', None),
                                    getattr(fig, '_chart_title', None)
                                )
                                logging.info(f"已创建额外文件: {special_filename}")
                    else:
                        logging.warning(f"创建 {symbol} 的图表失败，fig 为 None")
                    
                except Exception as e:
                    logging.error(f"生成 {display_name} ({symbol}) 图表时出错: {e}")
        
        # 专门处理人民币相关货币对
        self.generate_cny_currency_charts(processed_symbols)
        
        # 定义需要排除的资产列表
        excluded_assets = {
            '黄金ETF-GLD', 'GLD', 
            '黄金ETF-IAU', 'IAU',
            '黄金期货',
            '美国短期利率'
        }
        
        # 然后为其他重要资产生成图表（排除已处理的和不需要的）
        remaining_assets = [(symbol, type_name, count) for symbol, type_name, count in assets 
                           if symbol not in processed_symbols and symbol not in excluded_assets]
        
        for symbol, type_name, count in remaining_assets[:10]:  # 额外生成10个图表
            try:
                logging.info(f"正在生成 {symbol} 的技术分析图表...")
                
                df = self.get_data(symbol=symbol, days=365)
                if df.empty:
                    continue
                
                df = self.calculate_technical_indicators(df)
                fig = self.create_candlestick_chart(df, symbol, f" ({type_name})")
                
                if fig:
                    filename = f"{self.output_dir}/{symbol}_technical_analysis.html"
                    self.save_chart_with_description(
                        fig, filename, 
                        getattr(fig, '_description_text', None),
                        getattr(fig, '_chart_title', None)
                    )
                
            except Exception as e:
                logging.error(f"生成 {symbol} 图表时出错: {e}")
        
        # 获取所有已处理的资产用于后续分析
        all_processed = list(processed_symbols) + [asset[0] for asset in remaining_assets[:10]]
        major_assets = all_processed[:15]  # 取前15个用于相关性分析
        
        # 生成相关性热力图
        try:
            logging.info("正在生成相关性热力图...")
            fig = self.create_correlation_heatmap(major_assets)
            if fig:
                filename = f"{self.output_dir}/correlation_heatmap.html"
                self.save_chart_with_description(
                    fig, filename, 
                    getattr(fig, '_description_text', None),
                    getattr(fig, '_chart_title', None)
                )
        except Exception as e:
            logging.error(f"生成相关性热力图时出错: {e}")
        
        # 生成绩效对比图
        try:
            logging.info("正在生成绩效对比图...")
            fig = self.create_performance_comparison(major_assets)
            if fig:
                filename = f"{self.output_dir}/performance_comparison.html"
                self.save_chart_with_description(
                    fig, filename, 
                    getattr(fig, '_description_text', None),
                    getattr(fig, '_chart_title', None)
                )
        except Exception as e:
            logging.error(f"生成绩效对比图时出错: {e}")
        
        # 按数据类型生成汇总图表
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT DISTINCT mdt.type_name
                FROM macro_data md
                JOIN macro_data_types mdt ON md.type_id = mdt.id
                WHERE md.data_date >= %s
            """, (datetime.now() - timedelta(days=365),))
            
            data_types = [row[0] for row in cur.fetchall()]
            
            for data_type in data_types:
                logging.info(f"正在生成 {data_type} 类型的汇总图表...")
                
                df = self.get_data(type_name=data_type, days=365)
                if df.empty:
                    continue
                
                # 按资产分组绘制
                fig = go.Figure()
                
                for symbol in df['symbol'].unique():
                    symbol_df = df[df['symbol'] == symbol].sort_values('data_date')
                    price_col = 'close_price' if 'close_price' in symbol_df.columns and symbol_df['close_price'].notna().any() else 'value'
                    
                    fig.add_trace(
                        go.Scatter(
                            x=symbol_df['data_date'],
                            y=symbol_df[price_col],
                            mode='lines',
                            name=symbol,
                            line=dict(width=2)
                        )
                    )
                
                # 生成汇总分析描述
                summary_text = self.generate_overview_summary(df, data_type)
                
                fig.update_layout(
                    title={
                        'text': f'{data_type} 类型资产价格走势',
                        'x': 0.5,
                        'xanchor': 'center',
                        'font': {'size': 18}
                    },
                    xaxis_title='日期',
                    yaxis_title='价格',
                    height=700,  # 增加高度
                    hovermode='x unified',
                    margin=dict(t=100, b=50, l=50, r=50),
                    font=dict(size=11)
                )
                
                # 将描述文本存储到图表对象中，供保存时使用
                fig._description_text = summary_text
                fig._chart_title = f"{data_type} 汇总分析"
                
                filename = f"{self.output_dir}/{data_type}_overview.html"
                self.save_chart_with_description(
                    fig, filename, 
                    getattr(fig, '_description_text', None),
                    getattr(fig, '_chart_title', None)
                )
                
        except Exception as e:
            logging.error(f"生成类型汇总图表时出错: {e}")
        
        logging.info("所有图表生成完成!")
    
    def generate_central_bank_rates_chart(self):
        """
        生成各国央行利率汇总图表
        """
        try:
            # 获取央行利率数据
            cur = self.conn.cursor()
            # 修复参数传递问题
            start_date = datetime.now() - timedelta(days=730)
            query = """
                 SELECT md.symbol, md.data_date, md.value, mdt.type_name
                 FROM macro_data md
                 JOIN macro_data_types mdt ON md.type_id = mdt.id
                 WHERE mdt.type_name = '利率' AND (
                     md.symbol LIKE '%央行%' OR 
                     md.symbol LIKE '%联储%' OR 
                     md.symbol IN ('美联储基准利率', '欧洲央行利率', '瑞士央行利率', '英国央行利率', '日本央行利率', '俄罗斯央行利率')
                 )
                 AND md.data_date >= %s
                 ORDER BY md.symbol, md.data_date
             """
            # 使用字符串格式化避免参数问题
            formatted_query = query.replace('%s', f"'{start_date}'")
            cur.execute(formatted_query)  # 获取2年数据
            
            data = cur.fetchall()
            logging.info(f"查询到央行利率数据条数: {len(data)}")
            
            if not data:
                logging.warning("未找到央行利率数据")
                return
            
            # 打印前几条数据用于调试
            if data:
                logging.info(f"数据样例: {data[:3] if len(data) >= 3 else data}")
            
            # 转换为DataFrame
            try:
                df = pd.DataFrame(data, columns=['symbol', 'data_date', 'value', 'type_name'])
                logging.info(f"DataFrame形状: {df.shape}")
                logging.info(f"唯一符号: {df['symbol'].unique().tolist()}")
                
                # 过滤掉value为None的数据
                df = df.dropna(subset=['value'])
                logging.info(f"过滤后DataFrame形状: {df.shape}")
                
                if df.empty:
                    logging.warning("过滤后无有效央行利率数据")
                    return
                    
            except Exception as e:
                logging.error(f"DataFrame创建或处理时出错: {e}")
                import traceback
                logging.error(traceback.format_exc())
                return
            
            # 创建图表
            fig = go.Figure()
            
            # 央行名称映射
            bank_names = {
                '美联储基准利率': '美联储',
                '欧洲央行利率': '欧洲央行',
                '瑞士央行利率': '瑞士央行',
                '英国央行利率': '英国央行',
                '日本央行利率': '日本央行',
                '俄罗斯央行利率': '俄罗斯央行'
            }
            
            # 为每个央行添加线条
            for symbol in df['symbol'].unique():
                symbol_df = df[df['symbol'] == symbol].sort_values('data_date')
                bank_name = bank_names.get(symbol, symbol)
                
                fig.add_trace(
                    go.Scatter(
                        x=symbol_df['data_date'],
                        y=symbol_df['value'],
                        mode='lines+markers',
                        name=bank_name,
                        line=dict(width=2),
                        marker=dict(size=4),
                        hovertemplate=f'<b>{bank_name}</b><br>' +
                                    '日期: %{x}<br>' +
                                    '利率: %{y:.2f}%<extra></extra>'
                    )
                )
            
            # 生成分析描述
            description = self._generate_central_bank_analysis_description(df)
            
            # 更新布局
            fig.update_layout(
                title={
                    'text': '各国央行利率走势对比',
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                xaxis_title='日期',
                yaxis_title='利率 (%)',
                height=700,
                hovermode='x unified',
                margin=dict(t=100, b=50, l=50, r=50),
                font=dict(size=12),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # 添加网格线
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            
            # 将描述文本存储到图表对象中
            fig._description_text = description
            fig._chart_title = "各国央行利率汇总分析"
            
            # 保存图表
            filename = f"{self.output_dir}/各国央行利率_technical_analysis.html"
            self.save_chart_with_description(
                fig, filename, 
                getattr(fig, '_description_text', None),
                getattr(fig, '_chart_title', None)
            )
            
            logging.info(f"已生成各国央行利率汇总图表: {filename}")
            return filename
            
        except Exception as e:
            import traceback
            logging.error(f"生成央行利率图表时出错: {e}")
            logging.error(traceback.format_exc())
            return None
    
    def _generate_central_bank_analysis_description(self, df):
        """
        生成央行利率分析描述
        """
        try:
            # 央行名称映射
            bank_names = {
                '美联储基准利率': '美联储',
                '欧洲央行利率': '欧洲央行',
                '瑞士央行利率': '瑞士央行',
                '英国央行利率': '英国央行',
                '日本央行利率': '日本央行',
                '俄罗斯央行利率': '俄罗斯央行'
            }
            
            description = "<h3>各国央行利率分析报告</h3>"
            description += "<h4>📊 数据概览</h4>"
            
            # 获取最新数据
            try:
                latest_data = df.groupby('symbol').last().reset_index()
                logging.info(f"最新数据形状: {latest_data.shape}")
            except Exception as e:
                logging.error(f"获取最新数据时出错: {e}")
                import traceback
                logging.error(traceback.format_exc())
                return "<p>分析描述生成失败: 获取最新数据出错</p>"
            
            description += "<table border='1' style='border-collapse: collapse; width: 100%; margin: 10px 0;'>"
            description += "<tr><th style='padding: 8px; background: #f5f5f5;'>央行</th><th style='padding: 8px; background: #f5f5f5;'>当前利率</th><th style='padding: 8px; background: #f5f5f5;'>最新更新</th></tr>"
            
            for _, row in latest_data.iterrows():
                bank_name = bank_names.get(row['symbol'], row['symbol'])
                rate = f"{row['value']:.2f}%"
                date = row['data_date'].strftime('%Y-%m-%d')
                description += f"<tr><td style='padding: 6px;'>{bank_name}</td><td style='padding: 6px; font-weight: bold;'>{rate}</td><td style='padding: 6px;'>{date}</td></tr>"
            
            description += "</table>"
            
            # 分析趋势
            description += "<h4>📈 利率趋势分析</h4>"
            
            for symbol in df['symbol'].unique():
                symbol_df = df[df['symbol'] == symbol].sort_values('data_date')
                if len(symbol_df) < 1:
                    continue
                    
                bank_name = bank_names.get(symbol, symbol)
                current_rate = symbol_df.iloc[-1]['value']
                
                if len(symbol_df) >= 2:
                    prev_rate = symbol_df.iloc[-2]['value']
                    change = current_rate - prev_rate
                    trend = "上升" if change > 0.01 else "下降" if change < -0.01 else "持平"
                    
                    description += f"<p style='margin: 8px 0; padding: 8px; background: #f9f9f9; border-radius: 4px;'><strong>{bank_name}</strong>: 当前利率 {current_rate:.2f}%，较上期{trend}"
                    if abs(change) > 0.01:
                        description += f" {abs(change):.2f}个基点"
                    description += "</p>"
                else:
                    description += f"<p style='margin: 8px 0; padding: 8px; background: #f9f9f9; border-radius: 4px;'><strong>{bank_name}</strong>: 当前利率 {current_rate:.2f}%（仅有单条数据）</p>"
            
            description += "<h4>💡 投资建议</h4>"
            description += "<ul style='margin: 10px 0; padding-left: 20px;'>"
            description += "<li style='margin: 6px 0; line-height: 1.4;'>关注主要央行利率政策变化，特别是美联储政策对全球市场的影响</li>"
            description += "<li style='margin: 6px 0; line-height: 1.4;'>利率上升通常利好银行股，但可能对债券和成长股造成压力</li>"
            description += "<li style='margin: 6px 0; line-height: 1.4;'>各国央行政策分化可能影响汇率走势和跨境资本流动</li>"
            description += "<li style='margin: 6px 0; line-height: 1.4;'>建议关注央行会议纪要和政策声明，及时调整投资策略</li>"
            description += "</ul>"
            
            return description
            
        except Exception as e:
            logging.error(f"生成央行利率分析描述时出错: {e}")
            return "<p>分析描述生成失败</p>"
    
    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

def create_index_html(output_dir):
    """创建index.html索引页面"""
    try:
        import glob
        from pathlib import Path
        
        output_path = Path(output_dir)
        html_files = list(output_path.glob('*.html'))
        html_files = [f for f in html_files if f.name != 'index.html']
        
        logging.info(f"找到 {len(html_files)} 个HTML文件用于创建索引")
        
        # 检查特殊文件是否存在
        special_files = ['道琼斯指数_technical_analysis.html']
        for special_file in special_files:
            if not any(f.name == special_file for f in html_files):
                logging.warning(f"特殊文件 {special_file} 不存在，可能会影响索引页面的完整性")
        
        # 按文件名排序
        html_files.sort(key=lambda x: x.name)
        
        # 定义详细分类，参考README.md 3.2节
        categories = {
            '🇨🇳 中国指数': {
                'keywords': ['上证指数', '深证成指', '沪深300', '中证500', '创业板', '科创板'],
                'description': '中国主要股票指数的技术分析，包括上证指数、深证成指、沪深300、中证500等',
                'files': []
            },
            '🇺🇸 美国指数': {
                'keywords': ['标普500', '纳斯达克', '道琼斯', 'S&P', 'NASDAQ', 'DOW'],
                'description': '美国主要股票指数的技术分析，包括标普500、纳斯达克、道琼斯工业指数等',
                'files': []
            },
            '💱 外汇汇率': {
                'keywords': ['美元指数', '欧元兑美元', '英镑兑美元', '美元兑日元', '美元兑人民币', '欧元兑人民币', '英镑兑人民币', '日元兑人民币', '汇率', 'USD', 'EUR', 'GBP', 'JPY', 'CNY'],
                'description': '主要货币对的技术分析，包括美元指数、欧元、英镑、日元、人民币等汇率走势',
                'files': []
            },
            '🥇 贵金属': {
                'keywords': ['黄金', '白银', 'SLV', '贵金属', '上海金', 'Au99.99', 'Au100g', 'Au(T+D)'],
                'description': '贵金属市场技术分析，包括黄金期货、白银期货、上海金及相关产品',
                'files': []
            },
            '🛢️ 能源商品': {
                'keywords': ['原油期货', '原油', '天然气', '石油', 'WTI', 'Brent', '能源'],
                'description': '能源商品技术分析，包括原油期货、天然气等能源产品价格走势',
                'files': []
            },
            '📈 其他商品': {
                'keywords': ['铜', '大豆', '玉米', '小麦', '棉花', '商品', '期货'],
                'description': '其他大宗商品技术分析，包括工业金属、农产品等',
                'files': []
            },
            '₿ 加密货币': {
                'keywords': ['比特币', '以太坊', 'BTC', 'ETH', '加密', '数字货币'],
                'description': '主要加密货币技术分析，包括比特币、以太坊等数字资产价格走势',
                'files': []
            },
            '📊 利率债券': {
                'keywords': ['央行利率', '各国央行利率', '美联储基准利率', '欧洲央行利率', '瑞士央行利率', '英国央行利率', '日本央行利率', '俄罗斯央行利率'],
                'description': '各国央行利率政策分析，包括主要央行基准利率走势对比',
                'files': []
            },
            '📋 综合分析': {
                'keywords': ['correlation', 'heatmap', 'overview', '相关性', '热力图', '汇总', 'performance'],
                'description': '跨资产类别的综合分析，包括相关性分析、绩效对比等',
                'files': []
            }
        }
        
        # 分类文件
        uncategorized_files = []
        
        for html_file in html_files:
            categorized = False
            for category, config in categories.items():
                for keyword in config['keywords']:
                    if keyword.lower() in html_file.name.lower():
                        config['files'].append(html_file)
                        categorized = True
                        break
                if categorized:
                    break
            
            if not categorized:
                uncategorized_files.append(html_file)
        
        # 如果有未分类文件，添加到"其他"分类
        if uncategorized_files:
            categories['📂 其他'] = {
                'keywords': [],
                'description': '其他未分类的技术分析图表',
                'files': uncategorized_files
            }
        
        # 生成HTML内容
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>宏观经济数据技术分析报告</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 10px;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px 0;
            border-bottom: 2px solid #e1e8ed;
        }}
        
        .header h1 {{
            color: #2c3e50;
            font-size: clamp(1.8rem, 4vw, 2.5rem);
            margin-bottom: 10px;
            font-weight: 700;
        }}
        
        .header .subtitle {{
            color: #7f8c8d;
            font-size: clamp(0.9rem, 2vw, 1.1rem);
            margin-bottom: 15px;
        }}
        
        .stats {{
            display: flex;
            justify-content: center;
            gap: 20px;
            flex-wrap: wrap;
            margin-top: 15px;
        }}
        
        .stat-item {{
            background: linear-gradient(45deg, #3498db, #2980b9);
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 0.9rem;
            font-weight: 500;
        }}
        
        .category {{
            margin-bottom: 25px;
            background: white;
            border-radius: 15px;
            padding: 20px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            border: 1px solid #e1e8ed;
        }}
        
        .category-header {{
            display: flex;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f8f9fa;
        }}
        
        .category-header h2 {{
            color: #2c3e50;
            font-size: clamp(1.2rem, 3vw, 1.5rem);
            margin-right: 10px;
            font-weight: 600;
        }}
        
        .category-description {{
            color: #7f8c8d;
            font-size: 0.9rem;
            margin-bottom: 15px;
            line-height: 1.5;
        }}
        
        .file-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 12px;
        }}
        
        .file-item {{
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 15px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }}
        
        .file-item::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: linear-gradient(45deg, #3498db, #2980b9);
            transform: scaleY(0);
            transition: transform 0.3s ease;
        }}
        
        .file-item:hover {{
            transform: translateY(-3px);
            box-shadow: 0 10px 25px rgba(0,0,0,0.15);
            background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        }}
        
        .file-item:hover::before {{
            transform: scaleY(1);
        }}
        
        .file-item a {{
            text-decoration: none;
            color: #2c3e50;
            font-weight: 500;
            font-size: clamp(0.9rem, 2vw, 1rem);
            display: block;
            transition: color 0.3s ease;
        }}
        
        .file-item:hover a {{
            color: #3498db;
        }}
        
        .footer {{
            text-align: center;
            margin-top: 30px;
            padding: 20px;
            background: rgba(52, 152, 219, 0.1);
            border-radius: 15px;
            border: 1px solid rgba(52, 152, 219, 0.2);
        }}
        
        .update-time {{
            color: #7f8c8d;
            font-size: 0.9rem;
            margin-bottom: 10px;
        }}
        
        .footer-note {{
            color: #95a5a6;
            font-size: 0.8rem;
            font-style: italic;
        }}
        
        /* 移动端优化 */
        @media (max-width: 768px) {{
            .container {{
                margin: 5px;
                padding: 15px;
                border-radius: 15px;
            }}
            
            .file-grid {{
                grid-template-columns: 1fr;
                gap: 10px;
            }}
            
            .stats {{
                gap: 10px;
            }}
            
            .stat-item {{
                padding: 6px 12px;
                font-size: 0.8rem;
            }}
            
            .category {{
                padding: 15px;
                margin-bottom: 20px;
            }}
        }}
        
        @media (max-width: 480px) {{
            body {{
                padding: 5px;
            }}
            
            .container {{
                padding: 10px;
            }}
            
            .header {{
                padding: 15px 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 宏观经济数据技术分析报告</h1>
            <div class="subtitle">全面的金融市场技术分析与趋势预测</div>
            <div class="stats">
                <div class="stat-item">📈 {len([f for config in categories.values() for f in config['files']])} 个图表</div>
                <div class="stat-item">🏷️ {len([cat for cat, config in categories.items() if config['files']])} 个分类</div>
                <div class="stat-item">📅 {datetime.now().strftime('%Y-%m-%d')}</div>
            </div>
        </div>
"""
        
        # 添加各分类的内容
        for category, config in categories.items():
            if config['files']:  # 只显示有文件的分类
                html_content += f"""
        <div class="category">
            <div class="category-header">
                <h2>{category}</h2>
            </div>
            <div class="category-description">{config.get('description', '')}</div>
            <div class="file-grid">
"""
                for html_file in sorted(config['files'], key=lambda x: x.name):
                    # 提取文件名（去掉扩展名和后缀）
                    display_name = html_file.stem.replace('_technical_analysis', '').replace('_overview', '')
                    
                    # 特殊符号映射
                    symbol_display_map = {
                        '^DJI': '道琼斯指数'
                    }
                    
                    # 如果是特殊符号，使用映射的显示名称
                    if display_name in symbol_display_map:
                        display_name = symbol_display_map[display_name]
                    
                    # 添加图表类型标识
                    chart_type = '📈 技术分析' if '_technical_analysis.html' in html_file.name else '📊 概览分析'
                    html_content += f"""
                <div class="file-item">
                    <a href="{html_file.name}" target="_blank">
                        {display_name}<br>
                        <small style="color: #95a5a6; font-size: 0.8em;">{chart_type}</small>
                    </a>
                </div>
"""
                html_content += """
            </div>
        </div>
"""
        
        html_content += f"""
        <div class="footer">
            <div class="update-time">
                📅 最后更新时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
            </div>
            <div class="footer-note">
                本报告基于最新市场数据生成，仅供参考，不构成投资建议
            </div>
        </div>
    </div>
</body>
</html>
        """
        
        # 写入index.html
        index_file = output_path / 'index.html'
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logging.info(f"已创建索引页面: {index_file}")
        
    except Exception as e:
        logging.error(f"创建index.html失败: {str(e)}")

def main():
    try:
        import argparse
        
        # 创建命令行参数解析器
        parser = argparse.ArgumentParser(description='生成技术分析图表')
        parser.add_argument('--symbols', nargs='+', help='要生成图表的符号列表，例如 "^DJI"')
        parser.add_argument('--all', action='store_true', help='生成所有图表')
        args = parser.parse_args()
        
        plotter = TechnicalAnalysisPlotter()
        
        # 根据命令行参数决定生成哪些图表
        if args.symbols:
            # 生成指定符号的图表
            plotter.generate_specific_charts(args.symbols)
            print(f"\n=== 已为指定符号生成技术分析图表 ===")
            print(f"符号列表: {', '.join(args.symbols)}")
        elif args.all:
            # 生成所有图表
            plotter.generate_all_charts()
        else:
            # 默认生成道琼斯指数的图表
            plotter.generate_specific_charts()
            print("\n=== 已为默认符号(^DJI)生成技术分析图表 ===")
        
        # 生成index.html索引页面
        create_index_html(plotter.output_dir)
        
        print("\n=== 技术分析图表生成完成 ===")
        print(f"所有图表已保存到 plot_html 目录下")
        print("\n生成的图表包括:")
        print("1. 各主要资产的技术分析图表 (K线图、移动平均线、布林带、RSI、MACD)")
        if not args.symbols and not args.all:
            print("   - 道琼斯指数(^DJI)")
        print("2. index.html索引页面")
        print("\n请在浏览器中打开相应的HTML文件查看动态图表")
        
    except Exception as e:
        logging.error(f"程序执行出错: {e}")
        print(f"错误: {e}")

if __name__ == "__main__":
    main()