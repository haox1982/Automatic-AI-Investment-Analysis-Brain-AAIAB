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
        self.output_dir = '../plot_html'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def get_data(self, symbol=None, type_name=None, days=365):
        """
        从数据库获取数据
        """
        try:
            cur = self.conn.cursor()
            
            # 构建查询条件
            where_conditions = []
            params = []
            
            if symbol:
                where_conditions.append("md.symbol = %s")
                params.append(symbol)
            
            if type_name:
                where_conditions.append("mdt.type_name = %s")
                params.append(type_name)
            
            # 添加日期限制
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            where_conditions.append("md.data_date >= %s")
            params.append(start_date)
            
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
                mdt.type_name,
                mdt.type_code
            FROM macro_data md
            JOIN macro_data_types mdt ON md.type_id = mdt.id
            WHERE {where_clause}
            ORDER BY md.symbol, md.data_date
            """
            
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            
            df = pd.DataFrame(data, columns=columns)
            df['data_date'] = pd.to_datetime(df['data_date'])
            
            return df
            
        except Exception as e:
            logging.error(f"获取数据时出错: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df):
        """
        计算技术指标
        """
        if df.empty:
            return df
        
        # 使用close_price或value作为价格
        price_col = 'close_price' if 'close_price' in df.columns and df['close_price'].notna().any() else 'value'
        
        # 移动平均线
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
    
    def create_candlestick_chart(self, df, symbol, title_suffix=""):
        """
        创建K线图
        """
        if df.empty:
            return None
        
        # 检查是否有OHLC数据
        has_ohlc = all(col in df.columns and df[col].notna().any() 
                      for col in ['open_price', 'high_price', 'low_price', 'close_price'])
        
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=[f'{symbol} 价格走势', 'RSI', 'MACD', '成交量'],
            row_heights=[0.5, 0.2, 0.2, 0.1]
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
        
        # RSI
        if 'RSI' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['RSI'],
                    mode='lines',
                    name='RSI',
                    line=dict(color='purple')
                ),
                row=2, col=1
            )
            # RSI超买超卖线
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        
        # MACD
        if all(col in df.columns for col in ['MACD', 'MACD_signal', 'MACD_histogram']):
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['MACD'],
                    mode='lines',
                    name='MACD',
                    line=dict(color='blue')
                ),
                row=3, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=df['data_date'],
                    y=df['MACD_signal'],
                    mode='lines',
                    name='MACD信号线',
                    line=dict(color='red')
                ),
                row=3, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=df['data_date'],
                    y=df['MACD_histogram'],
                    name='MACD柱状图',
                    marker_color='gray'
                ),
                row=3, col=1
            )
        
        # 成交量
        if 'volume' in df.columns and df['volume'].notna().any():
            fig.add_trace(
                go.Bar(
                    x=df['data_date'],
                    y=df['volume'],
                    name='成交量',
                    marker_color='lightblue'
                ),
                row=4, col=1
            )
        
        # 更新布局
        fig.update_layout(
            title=f'{symbol} 技术分析图表{title_suffix}',
            xaxis_rangeslider_visible=False,
            height=800,
            showlegend=True
        )
        
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
        
        fig.update_layout(
            title='资产相关性热力图',
            xaxis_title='资产',
            yaxis_title='资产',
            height=600
        )
        
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
        
        fig.update_layout(
            title='资产绩效对比图',
            xaxis_title='日期',
            yaxis_title='累计收益率 (%)',
            height=600,
            hovermode='x unified'
        )
        
        return fig
    
    def generate_all_charts(self):
        """
        生成所有图表
        """
        logging.info("开始生成技术分析图表...")
        
        # 获取所有可用的资产
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT DISTINCT md.symbol, mdt.type_name, COUNT(*) as record_count
                FROM macro_data md
                JOIN macro_data_types mdt ON md.type_id = mdt.id
                WHERE md.data_date >= %s
                GROUP BY md.symbol, mdt.type_name
                HAVING COUNT(*) >= 30
                ORDER BY record_count DESC
            """, (datetime.now() - timedelta(days=365),))
            
            assets = cur.fetchall()
            
        except Exception as e:
            logging.error(f"获取资产列表时出错: {e}")
            return
        
        # 为每个主要资产生成技术分析图表
        major_assets = [asset[0] for asset in assets[:10]]  # 取前10个数据最多的资产
        
        for symbol, type_name, count in assets[:15]:  # 为前15个资产生成图表
            try:
                logging.info(f"正在生成 {symbol} 的技术分析图表...")
                
                df = self.get_data(symbol=symbol, days=365)
                if df.empty:
                    continue
                
                df = self.calculate_technical_indicators(df)
                fig = self.create_candlestick_chart(df, symbol, f" ({type_name})")
                
                if fig:
                    filename = f"{self.output_dir}/{symbol}_technical_analysis.html"
                    fig.write_html(filename)
                    logging.info(f"已保存: {filename}")
                
            except Exception as e:
                logging.error(f"生成 {symbol} 图表时出错: {e}")
        
        # 生成相关性热力图
        try:
            logging.info("正在生成相关性热力图...")
            fig = self.create_correlation_heatmap(major_assets)
            if fig:
                filename = f"{self.output_dir}/correlation_heatmap.html"
                fig.write_html(filename)
                logging.info(f"已保存: {filename}")
        except Exception as e:
            logging.error(f"生成相关性热力图时出错: {e}")
        
        # 生成绩效对比图
        try:
            logging.info("正在生成绩效对比图...")
            fig = self.create_performance_comparison(major_assets)
            if fig:
                filename = f"{self.output_dir}/performance_comparison.html"
                fig.write_html(filename)
                logging.info(f"已保存: {filename}")
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
                
                fig.update_layout(
                    title=f'{data_type} 类型资产价格走势',
                    xaxis_title='日期',
                    yaxis_title='价格',
                    height=600,
                    hovermode='x unified'
                )
                
                filename = f"{self.output_dir}/{data_type}_overview.html"
                fig.write_html(filename)
                logging.info(f"已保存: {filename}")
                
        except Exception as e:
            logging.error(f"生成类型汇总图表时出错: {e}")
        
        logging.info("所有图表生成完成!")
    
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
        
        # 按文件名排序
        html_files.sort(key=lambda x: x.name)
        
        # 生成HTML内容
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>投资分析图表 - {datetime.now().strftime('%Y-%m-%d')}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 3px solid #007bff;
            padding-bottom: 10px;
        }}
        .update-time {{
            text-align: center;
            color: #666;
            margin-bottom: 30px;
            font-size: 14px;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .chart-card {{
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            background-color: #fafafa;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        .chart-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }}
        .chart-link {{
            text-decoration: none;
            color: #007bff;
            font-weight: 500;
            display: block;
            padding: 10px;
            border-radius: 5px;
            transition: background-color 0.2s;
        }}
        .chart-link:hover {{
            background-color: #e3f2fd;
            color: #0056b3;
        }}
        .category {{
            margin-bottom: 30px;
        }}
        .category-title {{
            font-size: 18px;
            font-weight: bold;
            color: #333;
            margin-bottom: 15px;
            padding-left: 10px;
            border-left: 4px solid #007bff;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 投资分析图表</h1>
        <div class="update-time">
            最后更新时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
        </div>
        
        <div class="category">
            <div class="category-title">📈 技术分析图表</div>
            <div class="chart-grid">
"""
        
        # 分类显示图表
        categories = {
            '指数': [],
            '货币汇率': [],
            '大宗商品': [],
            '加密货币': [],
            '其他': []
        }
        
        for html_file in html_files:
            name = html_file.stem
            if any(keyword in name for keyword in ['指数', '上证', '深证', '沪深', '纳斯达克', '标普', '道琼斯']):
                categories['指数'].append(html_file)
            elif any(keyword in name for keyword in ['美元', '欧元', '英镑', '日元', '汇率']):
                categories['货币汇率'].append(html_file)
            elif any(keyword in name for keyword in ['黄金', '白银', '原油', '期货']):
                categories['大宗商品'].append(html_file)
            elif any(keyword in name for keyword in ['比特币', '加密']):
                categories['加密货币'].append(html_file)
            else:
                categories['其他'].append(html_file)
        
        # 生成各分类的HTML
        for category, files in categories.items():
            if files:
                html_content += f'<div class="category-title">{category}</div><div class="chart-grid">'
                for html_file in files:
                    display_name = html_file.stem.replace('_technical_analysis', '').replace('_overview', '')
                    html_content += f'''
                    <div class="chart-card">
                        <a href="{html_file.name}" class="chart-link" target="_blank">
                            {display_name}
                        </a>
                    </div>
                    '''
                html_content += '</div>'
        
        html_content += """
            </div>
        </div>
        
        <div style="text-align: center; margin-top: 40px; color: #666; font-size: 12px;">
            <p>🤖 由Backtrader投资分析系统自动生成</p>
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
        plotter = TechnicalAnalysisPlotter()
        plotter.generate_all_charts()
        
        # 生成index.html索引页面
        create_index_html(plotter.output_dir)
        
        print("\n=== 技术分析图表生成完成 ===")
        print(f"所有图表已保存到 plot_html 目录下")
        print("\n生成的图表包括:")
        print("1. 各主要资产的技术分析图表 (K线图、移动平均线、布林带、RSI、MACD)")
        print("2. 资产相关性热力图")
        print("3. 资产绩效对比图")
        print("4. 按数据类型分组的汇总图表")
        print("5. index.html索引页面")
        print("\n请在浏览器中打开相应的HTML文件查看动态图表")
        
    except Exception as e:
        logging.error(f"程序执行出错: {e}")
        print(f"错误: {e}")

if __name__ == "__main__":
    main()