import backtrader as bt
import yfinance as yf
import pandas as pd
import json
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import os
import shutil  # 新增：用于文件复制

# --- 新增：获取脚本所在目录的绝对路径 ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- 新增代码：打印环境变量 --- (这部分可以保留用于调试)
def print_proxy_env_vars():
    print("--- Checking Proxy Environment Variables ---")
    for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
        value = os.environ.get(var)
        if value:
            print(f"{var}: {value}")
    print("----------------------------------------")

print_proxy_env_vars()
# --- 新增代码结束 ---

class SmaCross(bt.SignalStrategy):
    def __init__(self):
        self.sma10 = bt.ind.SMA(period=10)
        self.sma30 = bt.ind.SMA(period=30)
        self.signal_add(bt.SIGNAL_LONG, self.sma10 > self.sma30)
        self.portfolio_values = []
        self.dates = []
        self.sma10_values = []
        self.sma30_values = []
        self.buy_signals = []
        self.sell_signals = []
    
    def next(self):
        self.portfolio_values.append(self.broker.getvalue())
        self.dates.append(self.datas[0].datetime.date(0))
        self.sma10_values.append(self.sma10[0])
        self.sma30_values.append(self.sma30[0])
        if self.position.size == 0 and self.sma10[0] > self.sma30[0]:
            self.buy_signals.append((self.datas[0].datetime.date(0), self.datas[0].close[0]))
        elif self.position.size > 0 and self.sma10[0] < self.sma30[0]:
            self.sell_signals.append((self.datas[0].datetime.date(0), self.datas[0].close[0]))

def get_report_filename(symbol, fromdate, todate, outdir_name="plot_html"):
    # --- 修改：使用脚本目录构建绝对路径 ---
    outdir = os.path.join(SCRIPT_DIR, outdir_name)
    # 确保输出目录存在
    os.makedirs(outdir, exist_ok=True)
    
    base = f"{symbol}_{fromdate}_{todate}_"
    idx = 1
    while True:
        fname = os.path.join(outdir, f"{base}{idx:02d}.html")
        if not os.path.exists(fname):
            return fname
        idx += 1

# --- 新增：复制文件到 HTTP 服务器目录的函数 ---
def copy_to_http_server(source_file, symbol, fromdate, todate):
    """
    将生成的 HTML 文件复制到 HTTP 服务器目录
    """
    try:
        # 构建目标目录路径
        http_base_dir = os.path.join(SCRIPT_DIR, "..", "http", "backtrader")
        http_base_dir = os.path.abspath(http_base_dir)  # 转换为绝对路径
        
        # 确保目标目录存在
        os.makedirs(http_base_dir, exist_ok=True)
        
        # 构建目标文件路径
        filename = os.path.basename(source_file)
        target_file = os.path.join(http_base_dir, filename)
        
        # 复制文件
        shutil.copy2(source_file, target_file)
        
        print(f"HTML 文件已复制到 HTTP 服务器目录: {target_file}")
        
        # 返回相对于 http 根目录的路径
        return f"backtrader/{filename}"
        
    except Exception as e:
        print(f"复制文件到 HTTP 服务器目录时出错: {e}")
        return None

def run_backtest(symbol, fromdate, todate, cash=100000, plot_filename=None):
    try:
        # 1. 先下载数据
        print(f"正在下载 {symbol} 数据...")
        # 在数据下载后，修改列名处理部分
        df = yf.download(symbol, fromdate, todate, auto_adjust=True, progress=False)
        
        if df.empty:
            print(f"Error: No data found for symbol {symbol} in the given date range.")
            return
        
        # 处理多级列名
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)  # 移除第二级索引（Ticker）
        
        # 确保列名为小写
        df.columns = [col.lower() for col in df.columns]
        
        print(f"数据行数: {len(df)}")
        print(df.head())
        
        if len(df) < 30:
            print(f"Error: Not enough data ({len(df)} rows) for the longest period (30). Please provide a longer date range.")
            return
        
        # 2. 设置回测引擎
        cerebro = bt.Cerebro()
        cerebro.addstrategy(SmaCross)
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        
        # 3. 添加数据
        data = bt.feeds.PandasData(dataname=df, name=symbol)
        cerebro.adddata(data)
        cerebro.broker.setcash(float(cash))
        
        # 4. 运行回测
        print("Starting backtest...")
        result = cerebro.run()
        
        if not result:
            raise RuntimeError("回测未能正常运行，可能是数据或参数问题")
        
        strat = result[0]
        
        # 5. 分析结果
        final_value = cerebro.broker.getvalue()
        sharpe_ratio = strat.analyzers.sharpe.get_analysis().get('sharperatio')
        max_drawdown = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown')
        
        # 6. 生成图表并获取文件路径
        html_file_path = None
        http_relative_path = None  # 新增：HTTP 服务器相对路径
        
        if plot_filename or True:  # 默认总是生成图表
            html_file_path = get_report_filename(symbol, fromdate, todate)
            
            # 修改绘图部分，使用双Y轴
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # 主Y轴：股价和均线
            fig.add_trace(go.Scatter(x=strat.dates, y=strat.sma10_values, mode='lines', name='SMA10'), secondary_y=False)
            fig.add_trace(go.Scatter(x=strat.dates, y=strat.sma30_values, mode='lines', name='SMA30'), secondary_y=False)
            
            # 次Y轴：净值
            fig.add_trace(go.Scatter(x=strat.dates, y=strat.portfolio_values, mode='lines', name='净值'), secondary_y=True)
            
            # 设置Y轴标题
            fig.update_yaxes(title_text="股价/均线 ($)", secondary_y=False)
            fig.update_yaxes(title_text="账户净值 ($)", secondary_y=True)
            
            # 买卖点
            if strat.buy_signals:
                buy_x, buy_y = zip(*strat.buy_signals)
                fig.add_trace(go.Scatter(x=buy_x, y=buy_y, mode='markers', marker_symbol='triangle-up', marker_color='green', name='买入'))
            if strat.sell_signals:
                sell_x, sell_y = zip(*strat.sell_signals)
                fig.add_trace(go.Scatter(x=sell_x, y=sell_y, mode='markers', marker_symbol='triangle-down', marker_color='red', name='卖出'))
            
            # 分析器结果
            annotation = f"Sharpe: {sharpe_ratio:.2f}  MaxDrawdown: {max_drawdown:.2f}" if sharpe_ratio and max_drawdown else ""
            fig.update_layout(title=f"回测结果 {annotation}")
            
            fig.write_html(html_file_path)
            print(f"Plotly 动态净值曲线已保存到 {html_file_path}")
            
            # --- 新增：复制文件到 HTTP 服务器目录 ---
            http_relative_path = copy_to_http_server(html_file_path, symbol, fromdate, todate)
        
        # 7. 构建结构化的 JSON 输出
        result_json = {
            "status": "success",
            "symbol": symbol,
            "date_range": {
                "from_date": fromdate,
                "to_date": todate
            },
            "backtest_results": {
                "initial_cash": float(cash),
                "final_value": float(final_value),  # 确保是数字类型
                "total_return": float(((final_value - cash) / cash) * 100),  # 确保是数字类型
                "sharpe_ratio": float(sharpe_ratio) if sharpe_ratio is not None else None,
                "max_drawdown": float(max_drawdown) if max_drawdown is not None else None
            },
            "output_files": {
                # --- 修改：返回相对于 plot_html 的路径 --- #
                "html_report_path": os.path.join("plot_html", os.path.basename(html_file_path)) if html_file_path else None,
                "html_filename": os.path.basename(html_file_path) if html_file_path else None,
                # --- 新增：HTTP 服务器路径 ---
                "http_relative_path": http_relative_path
            }
        }
        
        # 输出 JSON 格式结果
        print(json.dumps(result_json, indent=2, ensure_ascii=False))
        
        return result_json
        
    except Exception as e:
        error_result = {
            "status": "error",
            "error_message": str(e),
            "symbol": symbol,
            "date_range": {
                "from_date": fromdate,
                "to_date": todate
            }
        }
        print(json.dumps(error_result, indent=2, ensure_ascii=False))
        return error_result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--fromdate", type=str, required=True)
    parser.add_argument("--todate", type=str, required=True)
    parser.add_argument("--cash", type=float, default=100000)
    parser.add_argument("--plot", type=str, help="Filename to save the interactive plot to (e.g., report.html).")
    args = parser.parse_args()
    run_backtest(args.symbol, args.fromdate, args.todate, args.cash, args.plot)