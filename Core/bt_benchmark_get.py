#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主流媒体观点聚合模块
基于FinBERT的金融新闻情感分析系统
支持中英文财经媒体观点聚合
"""

import os
import sys
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import sqlite3
from typing import List, Dict, Optional, Tuple
import feedparser
import re
from collections import Counter
import jieba
import jieba.analyse

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Core.DB.db_utils import get_db_connection

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('media_sentiment.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MediaSentimentAnalyzer:
    """
    媒体观点聚合与情感分析器
    """
    
    def __init__(self):
        self.finbert_available = False
        self.init_finbert()
        self.init_chinese_nlp()
        
        # 新闻源配置
        self.news_sources = {
            # 英文媒体 RSS 源
            'english': {
                'yahoo_finance': 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^DJI,^IXIC&region=US&lang=en-US',
                'reuters_business': 'https://feeds.reuters.com/reuters/businessNews',
                'bloomberg_markets': 'https://feeds.bloomberg.com/markets/news.rss',
                'marketwatch': 'https://feeds.marketwatch.com/marketwatch/marketpulse/',
                'cnbc_markets': 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069'
            },
            # 中文媒体 RSS 源
            'chinese': {
                'sina_finance': 'https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=1686&k=&num=50&page=1',
                'eastmoney': 'http://feed.eastmoney.com/rssapi/news?type=cjxw&pageSize=50',
                'caixin': 'https://www.caixin.com/rss/rss_finance.xml',
                'yicai': 'https://www.yicai.com/api/ajax/getlatest?page=1&pagesize=50&action=news',
                'wallstreetcn': 'https://api-prod.wallstreetcn.com/apiv1/content/articles?limit=50&cursor=0'
            }
        }
        
        # 情感词典（简化版）
        self.sentiment_dict = {
            'positive': ['上涨', '增长', '利好', '乐观', '强劲', '突破', '创新高', 'bullish', 'positive', 'growth', 'surge', 'rally'],
            'negative': ['下跌', '下滑', '利空', '悲观', '疲软', '跌破', '创新低', 'bearish', 'negative', 'decline', 'crash', 'fall']
        }
    
    def init_finbert(self):
        """
        初始化FinBERT模型（如果可用）
        """
        try:
            from transformers import AutoTokenizer, AutoModelForSequenceClassification
            import torch
            
            logger.info("正在加载FinBERT模型...")
            self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
            self.finbert_available = True
            logger.info("FinBERT模型加载成功")
            
        except ImportError:
            logger.warning("transformers库未安装，将使用简化情感分析")
            self.finbert_available = False
        except Exception as e:
            logger.error(f"FinBERT模型加载失败: {e}")
            self.finbert_available = False
    
    def init_chinese_nlp(self):
        """
        初始化中文NLP工具
        """
        try:
            # 设置jieba词典
            jieba.set_dictionary(None)  # 使用默认词典
            logger.info("中文NLP工具初始化成功")
        except Exception as e:
            logger.error(f"中文NLP工具初始化失败: {e}")
    
    def analyze_sentiment_finbert(self, texts: List[str]) -> List[Dict]:
        """
        使用FinBERT进行情感分析
        """
        if not self.finbert_available:
            return self.analyze_sentiment_simple(texts)
        
        try:
            import torch
            import torch.nn.functional as F
            
            results = []
            
            # 批量处理
            batch_size = 16
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i+batch_size]
                
                # 预处理文本
                inputs = self.tokenizer(batch_texts, 
                                       padding=True, 
                                       truncation=True, 
                                       max_length=512,
                                       return_tensors='pt')
                
                # 推理
                with torch.no_grad():
                    outputs = self.model(**inputs)
                    predictions = F.softmax(outputs.logits, dim=-1)
                
                # 解析结果
                for j, pred in enumerate(predictions):
                    scores = pred.numpy()
                    # FinBERT输出: [negative, neutral, positive]
                    sentiment_score = scores[2] - scores[0]  # positive - negative
                    
                    if sentiment_score > 0.1:
                        sentiment = 'positive'
                    elif sentiment_score < -0.1:
                        sentiment = 'negative'
                    else:
                        sentiment = 'neutral'
                    
                    results.append({
                        'text': batch_texts[j],
                        'sentiment': sentiment,
                        'score': float(sentiment_score),
                        'confidence': float(max(scores)),
                        'scores': {
                            'negative': float(scores[0]),
                            'neutral': float(scores[1]),
                            'positive': float(scores[2])
                        }
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"FinBERT分析失败: {e}")
            return self.analyze_sentiment_simple(texts)
    
    def analyze_sentiment_simple(self, texts: List[str]) -> List[Dict]:
        """
        简化版情感分析（基于关键词）
        """
        results = []
        
        for text in texts:
            text_lower = text.lower()
            
            # 计算正负面词汇数量
            positive_count = sum(1 for word in self.sentiment_dict['positive'] 
                               if word in text_lower)
            negative_count = sum(1 for word in self.sentiment_dict['negative'] 
                               if word in text_lower)
            
            # 计算情感得分
            if positive_count > negative_count:
                sentiment = 'positive'
                score = (positive_count - negative_count) / max(len(text.split()), 1)
            elif negative_count > positive_count:
                sentiment = 'negative'
                score = -(negative_count - positive_count) / max(len(text.split()), 1)
            else:
                sentiment = 'neutral'
                score = 0.0
            
            results.append({
                'text': text,
                'sentiment': sentiment,
                'score': score,
                'confidence': min(abs(score) * 2, 1.0),
                'method': 'keyword_based'
            })
        
        return results
    
    def fetch_rss_news(self, url: str, source_name: str) -> List[Dict]:
        """
        获取RSS新闻
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            feed = feedparser.parse(response.content)
            
            articles = []
            for entry in feed.entries[:20]:  # 限制每个源最多20条
                article = {
                    'title': entry.get('title', ''),
                    'description': entry.get('description', ''),
                    'link': entry.get('link', ''),
                    'published': entry.get('published', ''),
                    'source': source_name,
                    'language': 'chinese' if source_name in self.news_sources['chinese'] else 'english'
                }
                articles.append(article)
            
            logger.info(f"从 {source_name} 获取到 {len(articles)} 条新闻")
            return articles
            
        except Exception as e:
            logger.error(f"获取 {source_name} 新闻失败: {e}")
            return []
    
    def fetch_all_news(self) -> List[Dict]:
        """
        获取所有新闻源的新闻
        """
        all_articles = []
        
        # 获取英文新闻
        for source_name, url in self.news_sources['english'].items():
            articles = self.fetch_rss_news(url, source_name)
            all_articles.extend(articles)
            time.sleep(1)  # 避免请求过快
        
        # 获取中文新闻
        for source_name, url in self.news_sources['chinese'].items():
            articles = self.fetch_rss_news(url, source_name)
            all_articles.extend(articles)
            time.sleep(1)
        
        logger.info(f"总共获取到 {len(all_articles)} 条新闻")
        return all_articles
    
    def extract_keywords(self, text: str, language: str = 'english') -> List[str]:
        """
        提取关键词
        """
        if language == 'chinese':
            # 中文关键词提取
            keywords = jieba.analyse.extract_tags(text, topK=10, withWeight=False)
            return keywords
        else:
            # 英文关键词提取（简化版）
            words = re.findall(r'\b[A-Za-z]{3,}\b', text.lower())
            # 过滤常见停用词
            stop_words = {'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'man', 'new', 'now', 'old', 'see', 'two', 'way', 'who', 'boy', 'did', 'its', 'let', 'put', 'say', 'she', 'too', 'use'}
            keywords = [word for word in words if word not in stop_words]
            return list(Counter(keywords).most_common(10))
    
    def generate_report(self, articles: List[Dict], sentiment_results: List[Dict]) -> str:
        """
        生成媒体观点聚合报告
        """
        if not articles or not sentiment_results:
            return "# 媒体观点聚合报告\n\n暂无数据"
        
        # 统计情感分布
        sentiment_counts = Counter([r['sentiment'] for r in sentiment_results])
        total_articles = len(sentiment_results)
        
        positive_pct = (sentiment_counts.get('positive', 0) / total_articles) * 100
        neutral_pct = (sentiment_counts.get('neutral', 0) / total_articles) * 100
        negative_pct = (sentiment_counts.get('negative', 0) / total_articles) * 100
        
        # 计算平均情感得分
        avg_score = np.mean([r.get('score', 0) for r in sentiment_results])
        
        # 提取热点关键词
        all_keywords = []
        for article in articles:
            text = f"{article['title']} {article['description']}"
            keywords = self.extract_keywords(text, article['language'])
            all_keywords.extend(keywords)
        
        top_keywords = Counter(all_keywords).most_common(10)
        
        # 生成报告
        report = f"""# 📊 主流媒体观点聚合报告

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**数据来源**: Bloomberg, Reuters, Yahoo Finance, 新浪财经, 东方财富, 财新网等
**分析方法**: {'FinBERT深度学习模型' if self.finbert_available else '关键词情感分析'}

## 📈 市场情绪概览

- **整体情绪**: {'乐观' if avg_score > 0.1 else '悲观' if avg_score < -0.1 else '中性'} (得分: {avg_score:.3f})
- **情感分布**: 
  - 🟢 正面: {positive_pct:.1f}% ({sentiment_counts.get('positive', 0)}条)
  - 🟡 中性: {neutral_pct:.1f}% ({sentiment_counts.get('neutral', 0)}条)
  - 🔴 负面: {negative_pct:.1f}% ({sentiment_counts.get('negative', 0)}条)
- **样本数量**: {total_articles}条新闻

## 🔥 热点关键词

"""
        
        for i, (keyword, count) in enumerate(top_keywords[:8], 1):
            report += f"{i}. **{keyword}** ({count}次)\n"
        
        report += "\n## 📰 重点新闻分析\n\n"
        
        # 选择最具代表性的新闻
        sorted_results = sorted(sentiment_results, key=lambda x: abs(x.get('score', 0)), reverse=True)
        
        for i, result in enumerate(sorted_results[:6], 1):
            article = next((a for a in articles if f"{a['title']} {a['description']}" == result['text']), None)
            if article:
                sentiment_emoji = {'positive': '🟢', 'negative': '🔴', 'neutral': '🟡'}[result['sentiment']]
                confidence = result.get('confidence', 0)
                
                report += f"""### {sentiment_emoji} {article['source'].upper()}: "{article['title'][:50]}..."

- **情感**: {result['sentiment']} (得分: {result.get('score', 0):.3f}, 置信度: {confidence:.2f})
- **来源**: {article['source']}
- **链接**: [{article['title'][:30]}...]({article['link']})
- **摘要**: {article['description'][:100]}...

"""
        
        report += """## ⚠️ 免责声明

本报告基于公开媒体信息和AI情感分析生成，仅供参考，不构成投资建议。投资有风险，决策需谨慎。

---
*报告由AI自动生成 | 数据更新频率: 每日*
"""
        
        return report
    
    def save_to_database(self, articles: List[Dict], sentiment_results: List[Dict]):
        """
        保存数据到数据库
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 创建表（如果不存在）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS media_sentiment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    description TEXT,
                    source TEXT,
                    language TEXT,
                    link TEXT,
                    published TEXT,
                    sentiment TEXT,
                    sentiment_score REAL,
                    confidence REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 插入数据
            for article, sentiment in zip(articles, sentiment_results):
                cursor.execute("""
                    INSERT INTO media_sentiment 
                    (title, description, source, language, link, published, 
                     sentiment, sentiment_score, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    article['title'],
                    article['description'],
                    article['source'],
                    article['language'],
                    article['link'],
                    article['published'],
                    sentiment['sentiment'],
                    sentiment.get('score', 0),
                    sentiment.get('confidence', 0)
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"成功保存 {len(articles)} 条记录到数据库")
            
        except Exception as e:
            logger.error(f"保存数据库失败: {e}")
    
    def run_analysis(self) -> str:
        """
        运行完整的媒体观点分析
        """
        logger.info("开始媒体观点聚合分析...")
        
        # 1. 获取新闻
        articles = self.fetch_all_news()
        if not articles:
            logger.warning("未获取到任何新闻")
            return "# 媒体观点聚合报告\n\n暂无数据"
        
        # 2. 准备文本数据
        texts = [f"{article['title']} {article['description']}" for article in articles]
        
        # 3. 情感分析
        logger.info("开始情感分析...")
        sentiment_results = self.analyze_sentiment_finbert(texts)
        
        # 4. 生成报告
        report = self.generate_report(articles, sentiment_results)
        
        # 5. 保存到数据库
        self.save_to_database(articles, sentiment_results)
        
        # 6. 保存报告文件
        report_filename = f"media_sentiment_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        report_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plot_html', report_filename)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"报告已保存到: {report_path}")
        
        return report

def main():
    """
    主函数
    """
    analyzer = MediaSentimentAnalyzer()
    report = analyzer.run_analysis()
    print(report)

if __name__ == "__main__":
    main()