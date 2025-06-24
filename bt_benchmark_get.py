#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主流媒体观点聚合分析工具
基于FinBERT和关键词分析，聚合中英文财经媒体观点
适用于中长线投资决策支持
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import time

# 数据处理
import pandas as pd
import numpy as np

# HTTP请求
import requests
from urllib.parse import quote

# RSS解析
import feedparser

# 中文分词
import jieba

# 环境变量
from dotenv import load_dotenv

# 数据库
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# 尝试导入transformers（FinBERT）
try:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch
    FINBERT_AVAILABLE = True
except ImportError:
    FINBERT_AVAILABLE = False
    print("警告: transformers库未安装，将使用简化的关键词分析方法")

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('media_sentiment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MediaSentimentAnalyzer:
    """媒体情感分析器"""
    
    def __init__(self):
        self.db_engine = None
        self.finbert_model = None
        self.finbert_tokenizer = None
        
        # 初始化数据库连接
        self._init_database()
        
        # 初始化FinBERT模型（如果可用）
        if FINBERT_AVAILABLE:
            self._init_finbert()
        
        # 中英文媒体源配置
        self.media_sources = {
            'english': {
                'yahoo_finance': 'https://feeds.finance.yahoo.com/rss/2.0/headline',
                'reuters_business': 'https://www.reuters.com/business/finance/rss',
                'marketwatch': 'https://feeds.marketwatch.com/marketwatch/topstories/',
            },
            'chinese': {
                'sina_finance': 'https://feed.sina.com.cn/api/roll/get?pageid=153&lid=1686&k=&num=50&page=1',
                'eastmoney': 'https://api.eastmoney.com/news/list?type=1&pageSize=50&pageIndex=1',
            }
        }
        
        # 情感分析关键词（中文）
        self.sentiment_keywords = {
            'positive': ['上涨', '增长', '利好', '看好', '乐观', '突破', '创新高', '强势', '回升', '反弹'],
            'negative': ['下跌', '下滑', '利空', '看空', '悲观', '跌破', '创新低', '疲软', '回落', '暴跌'],
            'neutral': ['持平', '震荡', '观望', '谨慎', '稳定', '维持', '预期', '关注']
        }
        
        # 资产类别关键词
        self.asset_keywords = {
            '股票': ['股票', '股市', '上证', '深证', '创业板', '科创板', 'A股', '港股', '美股'],
            '债券': ['债券', '国债', '企业债', '可转债', '利率', '收益率'],
            '商品': ['黄金', '原油', '铜', '铁矿石', '大豆', '玉米', '商品'],
            '汇率': ['人民币', '美元', '欧元', '日元', '汇率', '外汇'],
            '房地产': ['房地产', '房价', '地产', '住房', '楼市']
        }
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            self.db_engine = create_engine(db_url)
            logger.info("数据库连接初始化成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            self.db_engine = None
    
    def _init_finbert(self):
        """初始化FinBERT模型"""
        try:
            model_name = "ProsusAI/finbert"
            self.finbert_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.finbert_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            logger.info("FinBERT模型加载成功")
        except Exception as e:
            logger.error(f"FinBERT模型加载失败: {e}")
            self.finbert_model = None
            self.finbert_tokenizer = None
    
    def get_news_from_rss(self, url: str, max_items: int = 20) -> List[Dict]:
        """从RSS源获取新闻"""
        try:
            feed = feedparser.parse(url)
            news_items = []
            
            for entry in feed.entries[:max_items]:
                news_item = {
                    'title': entry.get('title', ''),
                    'summary': entry.get('summary', ''),
                    'link': entry.get('link', ''),
                    'published': entry.get('published', ''),
                    'source': url
                }
                news_items.append(news_item)
            
            return news_items
        except Exception as e:
            logger.error(f"RSS获取失败 {url}: {e}")
            return []
    
    def get_chinese_news(self, source: str) -> List[Dict]:
        """获取中文新闻（模拟API调用）"""
        # 这里是示例实现，实际需要根据具体API调整
        try:
            # 模拟新闻数据
            sample_news = [
                {
                    'title': 'A股三大指数集体上涨，科技股表现强势',
                    'summary': '今日A股市场表现良好，上证指数上涨1.2%，深证成指上涨1.5%，创业板指上涨2.1%。科技股领涨，新能源板块也有不错表现。',
                    'link': 'https://finance.sina.com.cn/example1',
                    'published': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'source': source
                },
                {
                    'title': '央行维持利率不变，市场预期稳定',
                    'summary': '中国人民银行今日宣布维持基准利率不变，符合市场预期。分析师认为这有利于维护市场稳定。',
                    'link': 'https://finance.sina.com.cn/example2',
                    'published': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'source': source
                }
            ]
            return sample_news
        except Exception as e:
            logger.error(f"中文新闻获取失败 {source}: {e}")
            return []
    
    def analyze_sentiment_finbert(self, text: str) -> Dict[str, float]:
        """使用FinBERT进行情感分析"""
        if not self.finbert_model or not self.finbert_tokenizer:
            return self.analyze_sentiment_keywords(text)
        
        try:
            inputs = self.finbert_tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
            
            with torch.no_grad():
                outputs = self.finbert_model(**inputs)
                predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
            
            # FinBERT输出: [negative, neutral, positive]
            scores = predictions[0].tolist()
            
            return {
                'negative': scores[0],
                'neutral': scores[1],
                'positive': scores[2],
                'method': 'finbert'
            }
        except Exception as e:
            logger.error(f"FinBERT分析失败: {e}")
            return self.analyze_sentiment_keywords(text)
    
    def analyze_sentiment_keywords(self, text: str) -> Dict[str, float]:
        """基于关键词的情感分析（中文适用）"""
        # 中文分词
        words = list(jieba.cut(text.lower()))
        
        positive_count = sum(1 for word in words if any(keyword in word for keyword in self.sentiment_keywords['positive']))
        negative_count = sum(1 for word in words if any(keyword in word for keyword in self.sentiment_keywords['negative']))
        neutral_count = sum(1 for word in words if any(keyword in word for keyword in self.sentiment_keywords['neutral']))
        
        total_sentiment_words = positive_count + negative_count + neutral_count
        
        if total_sentiment_words == 0:
            return {'positive': 0.33, 'neutral': 0.34, 'negative': 0.33, 'method': 'keywords'}
        
        return {
            'positive': positive_count / total_sentiment_words,
            'neutral': neutral_count / total_sentiment_words,
            'negative': negative_count / total_sentiment_words,
            'method': 'keywords'
        }
    
    def extract_asset_categories(self, text: str) -> List[str]:
        """提取文本中涉及的资产类别"""
        categories = []
        text_lower = text.lower()
        
        for category, keywords in self.asset_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                categories.append(category)
        
        return categories if categories else ['综合']
    
    def collect_all_news(self, days: int = 7) -> List[Dict]:
        """收集所有媒体源的新闻"""
        all_news = []
        
        # 收集英文新闻
        for source_name, url in self.media_sources['english'].items():
            logger.info(f"正在获取 {source_name} 新闻...")
            news_items = self.get_news_from_rss(url)
            for item in news_items:
                item['language'] = 'english'
                item['source_name'] = source_name
            all_news.extend(news_items)
            time.sleep(1)  # 避免请求过快
        
        # 收集中文新闻
        for source_name in self.media_sources['chinese'].keys():
            logger.info(f"正在获取 {source_name} 新闻...")
            news_items = self.get_chinese_news(source_name)
            for item in news_items:
                item['language'] = 'chinese'
                item['source_name'] = source_name
            all_news.extend(news_items)
            time.sleep(1)  # 避免请求过快
        
        logger.info(f"总共收集到 {len(all_news)} 条新闻")
        return all_news
    
    def analyze_news_batch(self, news_list: List[Dict]) -> List[Dict]:
        """批量分析新闻情感"""
        analyzed_news = []
        
        for news in news_list:
            try:
                # 合并标题和摘要进行分析
                text = f"{news.get('title', '')} {news.get('summary', '')}"
                
                # 根据语言选择分析方法
                if news.get('language') == 'english' and FINBERT_AVAILABLE:
                    sentiment = self.analyze_sentiment_finbert(text)
                else:
                    sentiment = self.analyze_sentiment_keywords(text)
                
                # 提取资产类别
                asset_categories = self.extract_asset_categories(text)
                
                # 添加分析结果
                news['sentiment'] = sentiment
                news['asset_categories'] = asset_categories
                news['analyzed_at'] = datetime.now().isoformat()
                
                analyzed_news.append(news)
                
            except Exception as e:
                logger.error(f"新闻分析失败: {e}")
                continue
        
        return analyzed_news
    
    def generate_aggregated_report(self, analyzed_news: List[Dict]) -> Dict:
        """生成聚合报告"""
        if not analyzed_news:
            return {'error': '没有可分析的新闻数据'}
        
        # 按资产类别聚合
        asset_sentiment = {}
        for news in analyzed_news:
            for asset in news.get('asset_categories', ['综合']):
                if asset not in asset_sentiment:
                    asset_sentiment[asset] = {'positive': [], 'neutral': [], 'negative': []}
                
                sentiment = news.get('sentiment', {})
                asset_sentiment[asset]['positive'].append(sentiment.get('positive', 0))
                asset_sentiment[asset]['neutral'].append(sentiment.get('neutral', 0))
                asset_sentiment[asset]['negative'].append(sentiment.get('negative', 0))
        
        # 计算平均情感
        aggregated_sentiment = {}
        for asset, sentiments in asset_sentiment.items():
            aggregated_sentiment[asset] = {
                'positive': np.mean(sentiments['positive']) if sentiments['positive'] else 0,
                'neutral': np.mean(sentiments['neutral']) if sentiments['neutral'] else 0,
                'negative': np.mean(sentiments['negative']) if sentiments['negative'] else 0,
                'news_count': len(sentiments['positive'])
            }
        
        # 整体市场情感
        all_positive = [news['sentiment']['positive'] for news in analyzed_news if 'sentiment' in news]
        all_neutral = [news['sentiment']['neutral'] for news in analyzed_news if 'sentiment' in news]
        all_negative = [news['sentiment']['negative'] for news in analyzed_news if 'sentiment' in news]
        
        overall_sentiment = {
            'positive': np.mean(all_positive) if all_positive else 0,
            'neutral': np.mean(all_neutral) if all_neutral else 0,
            'negative': np.mean(all_negative) if all_negative else 0
        }
        
        # 生成报告
        report = {
            'generated_at': datetime.now().isoformat(),
            'analysis_period': '7天',
            'total_news_analyzed': len(analyzed_news),
            'overall_market_sentiment': overall_sentiment,
            'asset_sentiment_breakdown': aggregated_sentiment,
            'key_insights': self._generate_insights(overall_sentiment, aggregated_sentiment),
            'risk_alerts': self._generate_risk_alerts(aggregated_sentiment)
        }
        
        return report
    
    def _generate_insights(self, overall: Dict, by_asset: Dict) -> List[str]:
        """生成关键洞察"""
        insights = []
        
        # 整体市场情感判断
        if overall['positive'] > 0.4:
            insights.append("市场整体情感偏向乐观，投资者信心较强")
        elif overall['negative'] > 0.4:
            insights.append("市场整体情感偏向悲观，需要关注风险")
        else:
            insights.append("市场情感相对中性，处于观望状态")
        
        # 资产类别分析
        for asset, sentiment in by_asset.items():
            if sentiment['positive'] > 0.5:
                insights.append(f"{asset}板块情感积极，可能存在投资机会")
            elif sentiment['negative'] > 0.5:
                insights.append(f"{asset}板块情感消极，建议谨慎操作")
        
        return insights
    
    def _generate_risk_alerts(self, by_asset: Dict) -> List[str]:
        """生成风险预警"""
        alerts = []
        
        for asset, sentiment in by_asset.items():
            if sentiment['negative'] > 0.6:
                alerts.append(f"⚠️ {asset}板块负面情感过高({sentiment['negative']:.2%})，建议密切关注")
            
            if sentiment['news_count'] < 3:
                alerts.append(f"ℹ️ {asset}板块新闻数量较少({sentiment['news_count']}条)，分析结果可能不够全面")
        
        return alerts
    
    def save_to_database(self, report: Dict, analyzed_news: List[Dict]):
        """保存分析结果到数据库"""
        if not self.db_engine:
            logger.error("数据库连接不可用，无法保存数据")
            return False
        
        try:
            # 这里需要根据实际数据库表结构调整
            # 示例：保存到media_sentiment表
            with self.db_engine.connect() as conn:
                # 保存聚合报告
                insert_report_sql = text("""
                    INSERT INTO media_sentiment_reports 
                    (generated_at, report_data, news_count) 
                    VALUES (:generated_at, :report_data, :news_count)
                """)
                
                conn.execute(insert_report_sql, {
                    'generated_at': datetime.now(),
                    'report_data': json.dumps(report, ensure_ascii=False),
                    'news_count': len(analyzed_news)
                })
                
                conn.commit()
                logger.info("分析结果已保存到数据库")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"数据库保存失败: {e}")
            return False
    
    def run_analysis(self, days: int = 7, target_assets: Optional[List[str]] = None) -> Dict:
        """运行完整的媒体观点分析"""
        logger.info(f"开始媒体观点聚合分析，分析周期: {days}天")
        
        # 1. 收集新闻
        news_list = self.collect_all_news(days)
        if not news_list:
            return {'error': '未能获取到新闻数据'}
        
        # 2. 分析情感
        analyzed_news = self.analyze_news_batch(news_list)
        
        # 3. 过滤目标资产（如果指定）
        if target_assets:
            filtered_news = []
            for news in analyzed_news:
                if any(asset in news.get('asset_categories', []) for asset in target_assets):
                    filtered_news.append(news)
            analyzed_news = filtered_news
        
        # 4. 生成聚合报告
        report = self.generate_aggregated_report(analyzed_news)
        
        # 5. 保存到数据库
        self.save_to_database(report, analyzed_news)
        
        # 6. 输出报告
        self._print_report(report)
        
        return report
    
    def _print_report(self, report: Dict):
        """打印分析报告"""
        print("\n" + "="*60)
        print("📊 主流媒体观点聚合分析报告")
        print("="*60)
        
        if 'error' in report:
            print(f"❌ 错误: {report['error']}")
            return
        
        print(f"📅 生成时间: {report['generated_at']}")
        print(f"📰 分析新闻数量: {report['total_news_analyzed']}条")
        print(f"⏱️ 分析周期: {report['analysis_period']}")
        
        # 整体市场情感
        overall = report['overall_market_sentiment']
        print(f"\n🌍 整体市场情感:")
        print(f"   积极: {overall['positive']:.2%}")
        print(f"   中性: {overall['neutral']:.2%}")
        print(f"   消极: {overall['negative']:.2%}")
        
        # 各资产类别情感
        print(f"\n📈 各资产类别情感分析:")
        for asset, sentiment in report['asset_sentiment_breakdown'].items():
            print(f"   {asset}:")
            print(f"     积极: {sentiment['positive']:.2%} | 中性: {sentiment['neutral']:.2%} | 消极: {sentiment['negative']:.2%}")
            print(f"     新闻数量: {sentiment['news_count']}条")
        
        # 关键洞察
        if report.get('key_insights'):
            print(f"\n💡 关键洞察:")
            for insight in report['key_insights']:
                print(f"   • {insight}")
        
        # 风险预警
        if report.get('risk_alerts'):
            print(f"\n⚠️ 风险预警:")
            for alert in report['risk_alerts']:
                print(f"   {alert}")
        
        print("\n" + "="*60)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='主流媒体观点聚合分析工具')
    parser.add_argument('--days', type=int, default=7, help='分析天数（默认7天）')
    parser.add_argument('--assets', type=str, help='目标资产类别，用逗号分隔（如：股票,债券）')
    parser.add_argument('--output', type=str, help='输出文件路径（JSON格式）')
    
    args = parser.parse_args()
    
    # 解析目标资产
    target_assets = None
    if args.assets:
        target_assets = [asset.strip() for asset in args.assets.split(',')]
    
    # 创建分析器并运行分析
    analyzer = MediaSentimentAnalyzer()
    report = analyzer.run_analysis(days=args.days, target_assets=target_assets)
    
    # 保存输出文件
    if args.output and 'error' not in report:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"\n📁 报告已保存到: {args.output}")
        except Exception as e:
            logger.error(f"保存输出文件失败: {e}")

if __name__ == "__main__":
    main()