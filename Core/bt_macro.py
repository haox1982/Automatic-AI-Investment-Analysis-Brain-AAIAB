import datetime
import logging
import os
import sys
import backtrader as bt
import pandas as pd
import pandas_ta as ta
import numpy as np
from dateutil.relativedelta import relativedelta

# 相对路径导入我们的数据库工具
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
from DB.db_utils import get_db_connection, insert_macro_analysis_report
from pathlib import Path

# 加载环境变量
load_dotenv('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader/.env')

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_from_db(symbol, conn, days=365*3):
    """从数据库获取指定资产的数据"""
    logging.info(f"从数据库获取资产 {symbol} 的数据")
    try:
        end_date = datetime.date.today()
        start_date = end_date - relativedelta(years=3)
        
        query = """
        SELECT 
            data_date,
            open_price as open,
            high_price as high,
            low_price as low,
            close_price as close,
            volume
        FROM macro_data
        WHERE symbol = %s AND data_date >= %s
        ORDER BY data_date
        """
        
        df = pd.read_sql(query, conn, params=[symbol, start_date])
        
        if df.empty:
            logging.warning(f"数据库中未找到 {symbol} 的数据")
            return pd.DataFrame()
            
        df['data_date'] = pd.to_datetime(df['data_date'])
        df.set_index('data_date', inplace=True)
        
        # Backtrader需要列名为小写
        df.columns = [col.lower() for col in df.columns]
        
        # 确保数据类型正确
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 填充缺失的OHLC数据
        # 如果有close但没有open/high/low，则用close填充
        if 'close' in df.columns and 'open' not in df.columns:
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']

        # 如果连close都没有，则无法处理
        if 'close' not in df.columns:
             logging.warning(f"{symbol} 没有 'close' 数据，无法处理")
             return pd.DataFrame()

        # 填充缺失的volume
        if 'volume' not in df.columns:
            df['volume'] = 0
            
        df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

        logging.info(f"成功获取 {len(df)} 条数据: {symbol}")
        return df

    except Exception as e:
        logging.error(f"从数据库获取数据失败 {symbol}: {e}")
        return pd.DataFrame()


class EnhancedTechnicalStrategy(bt.Strategy):
    """增强的技术分析策略类，提供多维度技术指标分析"""
    params = (
        ('sma_short', 20),   # 短期均线
        ('sma_medium', 50),  # 中期均线
        ('sma_long', 200),   # 长期均线
        ('rsi_period', 14),  # RSI周期
        ('macd_fast', 12),   # MACD快线
        ('macd_slow', 26),   # MACD慢线
        ('macd_signal', 9),  # MACD信号线
    )

    def __init__(self):
        # 移动平均线
        self.sma_short = bt.indicators.SimpleMovingAverage(self.data.close, period=self.p.sma_short)
        self.sma_medium = bt.indicators.SimpleMovingAverage(self.data.close, period=self.p.sma_medium)
        self.sma_long = bt.indicators.SimpleMovingAverage(self.data.close, period=self.p.sma_long)
        self.ema_short = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.p.sma_short)
        
        # 技术指标
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)
        self.macd = bt.indicators.MACD(self.data.close, 
                                      period_me1=self.p.macd_fast,
                                      period_me2=self.p.macd_slow,
                                      period_signal=self.p.macd_signal)
        self.stochastic = bt.indicators.Stochastic(self.data)
        self.atr = bt.indicators.ATR(self.data, period=14)
        self.bollinger = bt.indicators.BollingerBands(self.data.close, period=20)
        
        # 成交量指标
        self.volume_sma = bt.indicators.SimpleMovingAverage(self.data.volume, period=20)

    def next(self):
        # 我们只关心最后一根K线的数据用于生成报告
        if len(self.data) == self.data.buflen():
            self.generate_enhanced_analysis()

    def generate_enhanced_analysis(self):
        """生成增强的技术分析报告"""
        current_price = self.data.close[0]
        current_volume = self.data.volume[0]
        
        # 1. 趋势分析
        trend_analysis = self._analyze_trend(current_price)
        
        # 2. 动量分析
        momentum_analysis = self._analyze_momentum()
        
        # 3. 波动率分析
        volatility_analysis = self._analyze_volatility(current_price)
        
        # 4. 成交量分析
        volume_analysis = self._analyze_volume(current_volume)
        
        # 5. 支撑阻力分析
        support_resistance = self._analyze_support_resistance(current_price)
        
        # 6. 综合评分
        technical_score = self._calculate_technical_score()
        
        # 生成综合摘要
        summary = self._generate_summary(trend_analysis, momentum_analysis, technical_score)
        
        # 将分析结果存储在cerebro中
        self.cerebro.analysis_result = {
            'summary': summary,
            'details': {
                'current_price': f"{current_price:.4f}",
                'trend_analysis': trend_analysis,
                'momentum_analysis': momentum_analysis,
                'volatility_analysis': volatility_analysis,
                'volume_analysis': volume_analysis,
                'support_resistance': support_resistance,
                'technical_score': technical_score,
                'key_levels': {
                    'sma_20': f"{self.sma_short[0]:.4f}",
                    'sma_50': f"{self.sma_medium[0]:.4f}",
                    'sma_200': f"{self.sma_long[0]:.4f}",
                    'bollinger_upper': f"{self.bollinger.top[0]:.4f}",
                    'bollinger_lower': f"{self.bollinger.bot[0]:.4f}"
                }
            }
        }
    
    def _analyze_trend(self, current_price):
        """分析趋势状态"""
        trend_signals = []
        
        # 短期趋势 (20日均线)
        if current_price > self.sma_short[0]:
            short_trend = "上升"
            trend_signals.append(1)
        else:
            short_trend = "下降"
            trend_signals.append(-1)
            
        # 中期趋势 (50日均线)
        if current_price > self.sma_medium[0]:
            medium_trend = "上升"
            trend_signals.append(1)
        else:
            medium_trend = "下降"
            trend_signals.append(-1)
            
        # 长期趋势 (200日均线)
        if current_price > self.sma_long[0]:
            long_trend = "上升"
            trend_signals.append(1)
        else:
            long_trend = "下降"
            trend_signals.append(-1)
            
        # 均线排列
        if self.sma_short[0] > self.sma_medium[0] > self.sma_long[0]:
            ma_alignment = "多头排列"
        elif self.sma_short[0] < self.sma_medium[0] < self.sma_long[0]:
            ma_alignment = "空头排列"
        else:
            ma_alignment = "震荡排列"
            
        trend_strength = sum(trend_signals) / len(trend_signals)
        
        return {
            'short_term': short_trend,
            'medium_term': medium_trend,
            'long_term': long_trend,
            'ma_alignment': ma_alignment,
            'trend_strength': f"{trend_strength:.2f}",
            'description': f"短期{short_trend}，中期{medium_trend}，长期{long_trend}，均线呈{ma_alignment}"
        }
    
    def _analyze_momentum(self):
        """分析动量指标"""
        rsi_value = self.rsi[0]
        macd_value = self.macd.macd[0]
        macd_signal = self.macd.signal[0]
        macd_histogram = self.macd.macd[0] - self.macd.signal[0]  # 计算MACD柱状图
        stoch_k = self.stochastic.percK[0]
        stoch_d = self.stochastic.percD[0]
        
        # RSI状态
        if rsi_value > 70:
            rsi_status = "超买"
        elif rsi_value < 30:
            rsi_status = "超卖"
        else:
            rsi_status = "正常"
            
        # MACD状态
        if macd_value > macd_signal:
            macd_status = "金叉"
        else:
            macd_status = "死叉"
            
        # 随机指标状态
        if stoch_k > 80:
            stoch_status = "超买"
        elif stoch_k < 20:
            stoch_status = "超卖"
        else:
            stoch_status = "正常"
            
        return {
            'rsi': {
                'value': f"{rsi_value:.2f}",
                'status': rsi_status
            },
            'macd': {
                'value': f"{macd_value:.4f}",
                'signal': f"{macd_signal:.4f}",
                'histogram': f"{macd_histogram:.4f}",
                'status': macd_status
            },
            'stochastic': {
                'k': f"{stoch_k:.2f}",
                'd': f"{stoch_d:.2f}",
                'status': stoch_status
            },
            'description': f"RSI {rsi_value:.1f}({rsi_status})，MACD {macd_status}，随机指标{stoch_status}"
        }
    
    def _analyze_volatility(self, current_price):
        """分析波动率"""
        atr_value = self.atr[0]
        bb_upper = self.bollinger.top[0]
        bb_lower = self.bollinger.bot[0]
        bb_middle = (bb_upper + bb_lower) / 2
        
        # 布林带位置
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
        
        if bb_position > 0.8:
            bb_status = "接近上轨"
        elif bb_position < 0.2:
            bb_status = "接近下轨"
        else:
            bb_status = "中轨附近"
            
        # ATR相对价格的百分比
        atr_percentage = (atr_value / current_price) * 100
        
        return {
            'atr': {
                'value': f"{atr_value:.4f}",
                'percentage': f"{atr_percentage:.2f}%"
            },
            'bollinger': {
                'upper': f"{bb_upper:.4f}",
                'middle': f"{bb_middle:.4f}",
                'lower': f"{bb_lower:.4f}",
                'position': f"{bb_position:.2f}",
                'status': bb_status
            },
            'description': f"ATR {atr_percentage:.1f}%，布林带{bb_status}"
        }
    
    def _analyze_volume(self, current_volume):
        """分析成交量"""
        volume_sma = self.volume_sma[0]
        volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1
        
        if volume_ratio > 1.5:
            volume_status = "放量"
        elif volume_ratio < 0.5:
            volume_status = "缩量"
        else:
            volume_status = "正常"
            
        return {
            'current': f"{current_volume:.0f}",
            'average': f"{volume_sma:.0f}",
            'ratio': f"{volume_ratio:.2f}",
            'status': volume_status,
            'description': f"成交量{volume_status}({volume_ratio:.1f}倍均量)"
        }
    
    def _analyze_support_resistance(self, current_price):
        """分析支撑阻力位"""
        # 使用均线作为关键支撑阻力位
        levels = {
            'resistance': [],
            'support': []
        }
        
        ma_levels = [
            ('SMA20', self.sma_short[0]),
            ('SMA50', self.sma_medium[0]),
            ('SMA200', self.sma_long[0]),
            ('布林上轨', self.bollinger.top[0]),
            ('布林下轨', self.bollinger.bot[0])
        ]
        
        for name, level in ma_levels:
            if level > current_price:
                levels['resistance'].append(f"{name}: {level:.4f}")
            else:
                levels['support'].append(f"{name}: {level:.4f}")
                
        return {
            'resistance_levels': levels['resistance'][:3],  # 最近的3个阻力位
            'support_levels': levels['support'][:3],        # 最近的3个支撑位
            'description': f"主要阻力位{len(levels['resistance'])}个，支撑位{len(levels['support'])}个"
        }
    
    def _calculate_technical_score(self):
        """计算技术面综合评分 (1-10分)"""
        score = 5.0  # 基础分
        
        # 趋势得分 (±2分)
        current_price = self.data.close[0]
        if current_price > self.sma_long[0]:
            score += 1
        if current_price > self.sma_medium[0]:
            score += 0.5
        if current_price > self.sma_short[0]:
            score += 0.5
            
        # 动量得分 (±1.5分)
        rsi_value = self.rsi[0]
        if 40 <= rsi_value <= 60:
            score += 0.5  # RSI在健康区间
        elif rsi_value > 70 or rsi_value < 30:
            score -= 0.5  # 超买超卖扣分
            
        if self.macd.macd[0] > self.macd.signal[0]:
            score += 0.5  # MACD金叉加分
        else:
            score -= 0.5  # MACD死叉扣分
            
        # 成交量得分 (±0.5分)
        volume_ratio = self.data.volume[0] / self.volume_sma[0] if self.volume_sma[0] > 0 else 1
        if 0.8 <= volume_ratio <= 2.0:
            score += 0.5  # 成交量适中
            
        # 确保分数在1-10范围内
        score = max(1.0, min(10.0, score))
        
        # 评级
        if score >= 8:
            rating = "强烈看多"
        elif score >= 6.5:
            rating = "看多"
        elif score >= 4.5:
            rating = "中性"
        elif score >= 3:
            rating = "看空"
        else:
            rating = "强烈看空"
            
        return {
            'score': f"{score:.1f}",
            'rating': rating,
            'description': f"技术面评分{score:.1f}/10，{rating}"
        }
    
    def _generate_summary(self, trend_analysis, momentum_analysis, technical_score):
        """生成综合摘要"""
        return (f"技术分析摘要: {trend_analysis['description']}。"
                f"{momentum_analysis['description']}。"
                f"{technical_score['description']}。")

def run_analysis_for_symbol(symbol, dataframe):
    """对单个资产进行分析并存入数据库"""
    logging.info(f"开始分析资产: {symbol}")
    cerebro = bt.Cerebro()
    try:
        data = bt.feeds.PandasData(dataname=dataframe)
        cerebro.adddata(data)
        cerebro.addstrategy(EnhancedTechnicalStrategy)
        
        cerebro.run()

        # 准备数据并存入数据库
        today = datetime.date.today()
        
        # 插入分析报告到新的表结构
        if hasattr(cerebro, 'analysis_result'):
            try:
                insert_macro_analysis_report({
                    'type_code': 'MACRO',
                    'symbol': symbol,
                    'report_date': today,
                    'analysis_period': 'daily',
                    'summary': cerebro.analysis_result['summary'],
                    'key_metrics': cerebro.analysis_result['details'],
                    'chart_path': None,
                    'backtest_results': {}
                })
                logging.info(f"资产 {symbol} 的分析报告已存入数据库")
                # 返回分析结果供文件输出使用
                return cerebro.analysis_result
            except Exception as e:
                logging.error(f"存入数据库失败 {symbol}: {e}")
                return None
        else:
            logging.warning(f"资产 {symbol} 未生成分析结果")
            return None

    except Exception as e:
        logging.error(f"分析资产 {symbol} 时发生错误: {e}", exc_info=True)
        return None

def save_analysis_to_file(analysis_results):
    """将分析结果保存到plot_html文件夹下的文本文件"""
    try:
        # 获取当前日期用于文件命名
        current_date = datetime.date.today().strftime('%Y%m%d')
        
        # 确保plot_html目录存在
        # 使用基于当前文件位置的绝对路径，确保路径的稳定性
        plot_html_dir = Path(__file__).resolve().parent.parent / 'plot_html'
        plot_html_dir.mkdir(exist_ok=True)
        
        # 生成文件名
        filename = f"macro_technical_analysis_{current_date}.txt"
        filepath = plot_html_dir / filename
        
        # 写入分析结果
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"宏观资产技术分析报告\n")
            f.write(f"生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            for symbol, result in analysis_results.items():
                f.write(f"【{symbol}】技术分析\n")
                f.write("-" * 40 + "\n")
                f.write(f"摘要: {result['summary']}\n\n")
                
                details = result['details']
                f.write(f"当前价格: {details['current_price']}\n")
                f.write(f"趋势分析: {details['trend_analysis']['description']}\n")
                f.write(f"动量分析: {details['momentum_analysis']['description']}\n")
                f.write(f"波动率分析: {details['volatility_analysis']['description']}\n")
                f.write(f"成交量分析: {details['volume_analysis']['description']}\n")
                f.write(f"支撑阻力: {details['support_resistance']['description']}\n")
                f.write(f"技术评分: {details['technical_score']['description']}\n")
                
                # 关键技术位
                f.write(f"\n关键技术位:\n")
                key_levels = details['key_levels']
                f.write(f"  SMA20: {key_levels['sma_20']}\n")
                f.write(f"  SMA50: {key_levels['sma_50']}\n")
                f.write(f"  SMA200: {key_levels['sma_200']}\n")
                f.write(f"  布林上轨: {key_levels['bollinger_upper']}\n")
                f.write(f"  布林下轨: {key_levels['bollinger_lower']}\n")
                
                f.write("\n" + "=" * 80 + "\n\n")
        
        logging.info(f"分析结果已保存到: {filepath}")
        return str(filepath)
        
    except Exception as e:
        logging.error(f"保存分析结果失败: {e}", exc_info=True)
        return None

def main():
    logging.info("启动宏观数据文本分析脚本")
    conn = get_db_connection()
    if not conn:
        logging.error("无法连接到数据库，脚本终止")
        return

    analysis_results = {}  # 存储所有分析结果
    
    try:
        # 获取所有有OHLC数据的资产
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT symbol FROM macro_data
            WHERE (open_price IS NOT NULL AND close_price IS NOT NULL AND high_price IS NOT NULL AND low_price IS NOT NULL)
              AND data_date >= %s
            GROUP BY symbol
            HAVING count(*) > 50 -- 至少有50天的数据才进行分析
        """, (datetime.date.today() - relativedelta(years=3),))
        assets = [row[0] for row in cur.fetchall()]
        cur.close()

        logging.info(f"发现 {len(assets)} 个资产需要生成分析: {assets}")
        for symbol in assets:
            df = get_data_from_db(symbol, conn)
            if not df.empty and len(df) > 200: # 确保有足够数据计算200日均线
                result = run_analysis_for_symbol(symbol, df)
                if result:  # 如果分析成功，保存结果
                    analysis_results[symbol] = result
            elif not df.empty:
                logging.warning(f"资产 {symbol} 数据量不足 ({len(df)}条)，跳过分析")
        
        # 保存所有分析结果到文件
        if analysis_results:
            saved_file = save_analysis_to_file(analysis_results)
            if saved_file:
                logging.info(f"共分析了 {len(analysis_results)} 个资产，结果已保存")
            else:
                logging.error("保存分析结果失败")
        else:
            logging.warning("没有生成任何分析结果")

    except Exception as e:
        logging.error(f"主流程发生错误: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
    logging.info("宏观数据文本分析脚本执行完毕")

if __name__ == '__main__':
    main()