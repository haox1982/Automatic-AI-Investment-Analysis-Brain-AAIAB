#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度器
每天上午10:00执行数据更新，10:30执行图表生成
"""

import os
import sys
import time
import shutil
import logging
import schedule
import subprocess
from datetime import datetime
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader/scheduler.log')
    ]
)
logger = logging.getLogger(__name__)

# 路径配置
BASE_DIR = Path('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader')
CORE_DIR = BASE_DIR / 'Core'
PLOT_HTML_DIR = BASE_DIR / 'plot_html'
HTTP_BT_DIR = Path('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/http/bt')
HTTP_PORTFOLIO_DIR = Path('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/http/backtrader/portfolio')

# 脚本路径
WRITE_MACRO_SCRIPT = CORE_DIR / 'write_macro_data.py'
PLOT_ANALYSIS_SCRIPT = CORE_DIR / 'plot_technical_analysis.py'
PORTFOLIO_SCRIPT = CORE_DIR / 'bt_portfolio_get.py'
DATA_VALIDATOR_SCRIPT = CORE_DIR / 'data_validator.py'

def run_script(script_path, description):
    """执行Python脚本"""
    try:
        logger.info(f"开始执行: {description}")
        logger.info(f"脚本路径: {script_path}")
        
        # 切换到脚本所在目录
        os.chdir(script_path.parent)
        
        # 执行脚本
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=3600  # 1小时超时
        )
        
        if result.returncode == 0:
            logger.info(f"{description} 执行成功")
            if result.stdout:
                logger.info(f"输出: {result.stdout}")
            return True
        else:
            logger.error(f"{description} 执行失败，返回码: {result.returncode}")
            if result.stderr:
                logger.error(f"错误信息: {result.stderr}")
            if result.stdout:
                logger.info(f"输出: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"{description} 执行超时")
        return False
    except Exception as e:
        logger.error(f"{description} 执行异常: {str(e)}")
        return False

def daily_data_validation():
    """每日数据验证任务"""
    logger.info("=== 开始每日数据交叉验证任务 ===")
    success = run_script(DATA_VALIDATOR_SCRIPT, "数据交叉验证")
    if success:
        logger.info("数据验证完成")
    else:
        logger.error("数据验证失败")

def copy_html_files():
    """简单复制HTML文件到HTTP目录"""
    try:
        logger.info("开始复制HTML文件到HTTP目录")
        
        # 确保目标目录存在
        HTTP_BT_DIR.mkdir(parents=True, exist_ok=True)
        
        # 复制所有HTML文件
        if PLOT_HTML_DIR.exists():
            for html_file in PLOT_HTML_DIR.glob('*.html'):
                target_file = HTTP_BT_DIR / html_file.name
                shutil.copy2(html_file, target_file)
                logger.info(f"已复制: {html_file.name}")
            
            logger.info("HTML文件复制完成")
            return True
        else:
            logger.warning(f"源目录不存在: {PLOT_HTML_DIR}")
            return False
            
    except Exception as e:
        logger.error(f"复制HTML文件失败: {str(e)}")
        return False

def copy_portfolio_files():
    """复制投资组合报告文件到HTTP目录"""
    try:
        logger.info("开始复制投资组合文件到HTTP目录")
        
        # 确保目标目录存在
        HTTP_PORTFOLIO_DIR.mkdir(parents=True, exist_ok=True)
        
        # 复制Markdown报告文件
        if PLOT_HTML_DIR.exists():
            for md_file in PLOT_HTML_DIR.glob('portfolio_tracking_report_*.md'):
                target_file = HTTP_PORTFOLIO_DIR / md_file.name
                shutil.copy2(md_file, target_file)
                logger.info(f"已复制投资组合报告: {md_file.name}")
        
        # 复制日志文件
        portfolio_log = BASE_DIR / 'portfolio_tracking.log'
        if portfolio_log.exists():
            target_log = HTTP_PORTFOLIO_DIR / 'portfolio_tracking.log'
            shutil.copy2(portfolio_log, target_log)
            logger.info("已复制投资组合日志文件")
        
        logger.info("投资组合文件复制完成")
        return True
            
    except Exception as e:
        logger.error(f"复制投资组合文件失败: {str(e)}")
        return False

def daily_data_update():
    """每日数据更新任务"""
    logger.info("=== 开始每日数据更新任务 ===")
    
    success = run_script(WRITE_MACRO_SCRIPT, "宏观数据更新")
    if success:
        logger.info("数据更新完成")
        # 数据更新成功后立即进行验证
        daily_data_validation()
    else:
        logger.error("数据更新失败")

def daily_chart_generation():
    """每日图表生成任务"""
    logger.info("=== 开始每日图表生成任务 ===")
    
    # 执行图表生成脚本（plot_technical_analysis.py负责生成图表和index.html）
    success = run_script(PLOT_ANALYSIS_SCRIPT, "技术分析图表生成")
    if not success:
        logger.error("图表生成失败")
        return
    
    # 简单复制HTML文件
    success = copy_html_files()
    if success:
        logger.info("=== 每日任务完成 ===")
        logger.info("图表已更新，可通过 http://files.nltech.ggff.net/bt/index.html 访问")
    else:
        logger.error("HTML文件复制失败")

def daily_macro_analysis():
    """每日宏观数据文本分析任务"""
    logger.info("=== 开始每日宏观数据文本分析任务 ===")
    
    # 执行宏观数据文本分析脚本
    success = run_script(CORE_DIR / 'bt_macro.py', "宏观数据文本分析")
    if success:
        logger.info("=== 宏观数据文本分析完成 ===")
        logger.info("分析结果已保存到数据库和plot_html文件夹")
    else:
        logger.error("宏观数据文本分析失败")

def weekly_portfolio_tracking():
    """每周投资组合跟踪任务"""
    logger.info("=== 开始每周投资组合跟踪任务 ===")
    
    # 执行投资组合跟踪脚本
    success = run_script(PORTFOLIO_SCRIPT, "投资大佬风向标数据获取")
    if not success:
        logger.error("投资组合跟踪失败")
        return
    
    # 复制投资组合文件
    success = copy_portfolio_files()
    if success:
        logger.info("=== 每周投资组合跟踪完成 ===")
        logger.info("投资组合报告已更新，可通过 http://files.nltech.ggff.net/backtrader/portfolio/ 访问")
    else:
        logger.error("投资组合文件复制失败")

def main():
    """主函数"""
    logger.info("投资分析定时任务调度器启动")
    logger.info(f"数据更新脚本: {WRITE_MACRO_SCRIPT}")
    logger.info(f"数据验证脚本: {DATA_VALIDATOR_SCRIPT}")
    logger.info(f"图表生成脚本: {PLOT_ANALYSIS_SCRIPT}")
    logger.info(f"投资组合脚本: {PORTFOLIO_SCRIPT}")
    logger.info(f"图表输出目录: {HTTP_BT_DIR}")
    logger.info(f"投资组合输出目录: {HTTP_PORTFOLIO_DIR}")
    
    # 检查脚本是否存在
    if not WRITE_MACRO_SCRIPT.exists():
        logger.error(f"数据更新脚本不存在: {WRITE_MACRO_SCRIPT}")
        return
    
    if not DATA_VALIDATOR_SCRIPT.exists():
        logger.error(f"数据验证脚本不存在: {DATA_VALIDATOR_SCRIPT}")
        return
    
    if not PLOT_ANALYSIS_SCRIPT.exists():
        logger.error(f"图表生成脚本不存在: {PLOT_ANALYSIS_SCRIPT}")
        return
    
    if not PORTFOLIO_SCRIPT.exists():
        logger.error(f"投资组合脚本不存在: {PORTFOLIO_SCRIPT}")
        return
    
    # 设置定时任务
    schedule.every().day.at("10:10").do(daily_data_update)
    schedule.every().day.at("10:40").do(daily_chart_generation)
    schedule.every().day.at("10:50").do(daily_macro_analysis)
    schedule.every().monday.at("10:20").do(weekly_portfolio_tracking)
    
    logger.info("定时任务已设置:")
    logger.info("- 每日10:10: 数据更新与验证")
    logger.info("- 每日10:40: 图表生成和文件复制")
    logger.info("- 每日10:50: 宏观数据文本分析")
    logger.info("- 每周一10:20: 投资组合跟踪")
    logger.info("调度器运行中，按Ctrl+C停止...")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("调度器已停止")

if __name__ == "__main__":
    main()