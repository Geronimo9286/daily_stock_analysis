# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 主调度程序 (增强版)
===================================

修改说明：
1. 新增自动抓取当日涨幅榜前10名个股的功能。
2. 动态合并环境变量 STOCK_LIST 与热门个股，去重后统一分析。
"""
import os

# 代理配置 - 仅在本地环境使用，GitHub Actions 不需要
if os.getenv("GITHUB_ACTIONS") != "true":
    pass

import argparse
import logging
import sys
import time
import akshare as ak  # 确保导入 akshare 用于获取热门股
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from feishu_doc import FeishuDocManager

from config import get_config, Config
from storage import get_db, DatabaseManager
from data_provider import DataFetcherManager
from data_provider.akshare_fetcher import AkshareFetcher, RealtimeQuote, ChipDistribution
from analyzer import GeminiAnalyzer, AnalysisResult, STOCK_NAME_MAP
from notification import NotificationService, NotificationChannel, send_daily_report
from search_service import SearchService, SearchResponse
from stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult
from market_analyzer import MarketAnalyzer

# 配置日志格式
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def setup_logging(debug: bool = False, log_dir: str = "./logs") -> None:
    """配置日志系统"""
    level = logging.DEBUG if debug else logging.INFO
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"stock_analysis_{today_str}.log"
    debug_log_file = log_path / f"stock_analysis_debug_{today_str}.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(console_handler)
    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(file_handler)
    debug_handler = RotatingFileHandler(debug_log_file, maxBytes=50 * 1024 * 1024, backupCount=3, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(debug_handler)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class StockAnalysisPipeline:
    def __init__(self, config: Optional[Config] = None, max_workers: Optional[int] = None):
        self.config = config or get_config()
        self.max_workers = max_workers or self.config.max_workers
        self.db = get_db()
        self.fetcher_manager = DataFetcherManager()
        self.akshare_fetcher = AkshareFetcher()
        self.trend_analyzer = StockTrendAnalyzer()
        self.analyzer = GeminiAnalyzer()
        self.notifier = NotificationService()
        self.search_service = SearchService(
            tavily_keys=self.config.tavily_api_keys,
            serpapi_keys=self.config.serpapi_keys,
        )
        logger.info(f"调度器初始化完成，最大并发数: {self.max_workers}")

    def get_top_n_hot_stocks(self, n: int = 10) -> List[str]:
        """从 AkShare 获取当日涨幅前 N 的 A 股股票代码"""
        try:
            logger.info(f"正在抓取全盘涨幅前 {n} 的热门股票...")
            df = ak.stock_zh_a_spot_em()
            # 过滤掉 ST、退市股和北交所(可选)，按涨幅排序
            hot_df = df[~df['名称'].str.contains("ST|退市")].sort_values(by="涨跌幅", ascending=False)
            top_n = hot_df.head(n)
            
            code_list = []
            for _, row in top_n.iterrows():
                code = str(row['代码'])
                prefix = "sh" if code.startswith(('60', '68')) else "sz"
                code_list.append(f"{prefix}{code}")
            return code_list
        except Exception as e:
            logger.error(f"抓取热门股票失败: {e}")
            return []

    def fetch_and_save_stock_data(self, code: str, force_refresh: bool = False) -> Tuple[bool, Optional[str]]:
        try:
            today = date.today()
            if not force_refresh and self.db.has_today_data(code, today):
                return True, None
            df, source_name = self.fetcher_manager.get_daily_data(code, days=30)
            if df is None or df.empty: return False, "数据为空"
            self.db.save_daily_data(df, code, source_name)
            return True, None
        except Exception as e:
            return False, str(e)

    def analyze_stock(self, code: str) -> Optional[AnalysisResult]:
        # ... (此处保持你原来 analyze_stock 的逻辑不变，为节省篇幅略去重复代码)
        try:
            stock_name = STOCK_NAME_MAP.get(code, '')
            realtime_quote = None
            try:
                realtime_quote = self.akshare_fetcher.get_realtime_quote(code)
                if realtime_quote and realtime_quote.name: stock_name = realtime_quote.name
            except: pass
            if not stock_name: stock_name = f'股票{code}'
            
            chip_data = None
            try: chip_data = self.akshare_fetcher.get_chip_distribution(code)
            except: pass
            
            trend_result = None
            try:
                context = self.db.get_analysis_context(code)
                if context and 'raw_data' in context:
                    import pandas as pd
                    df = pd.DataFrame(context['raw_data'])
                    trend_result = self.trend_analyzer.analyze(df, code)
            except: pass
            
            news_context = None
            if self.search_service.is_available:
                intel_results = self.search_service.search_comprehensive_intel(code, stock_name, max_searches=2)
                if intel_results: news_context = self.search_service.format_intel_report(intel_results, stock_name)
            
            context = self.db.get_analysis_context(code)
            if context is None: return None
            enhanced_context = self._enhance_context(context, realtime_quote, chip_data, trend_result, stock_name)
            return self.analyzer.analyze(enhanced_context, news_context=news_context)
        except Exception as e:
            logger.error(f"[{code}] 分析失败: {e}")
            return None

    def _enhance_context(self, context, realtime_quote, chip_data, trend_result, stock_name):
        enhanced = context.copy()
        enhanced['stock_name'] = stock_name
        if realtime_quote:
            enhanced['realtime'] = {'price': realtime_quote.price, 'volume_ratio': realtime_quote.volume_ratio, 'turnover_rate': realtime_quote.turnover_rate}
        if chip_data:
            enhanced['chip'] = {'profit_ratio': chip_data.profit_ratio, 'concentration_90': chip_data.concentration_90}
        if trend_result:
            enhanced['trend_analysis'] = {'trend_status': trend_result.trend_status.value, 'buy_signal': trend_result.buy_signal.value, 'signal_score': trend_result.signal_score}
        return enhanced

    def process_single_stock(self, code: str, skip_analysis: bool = False) -> Optional[AnalysisResult]:
        try:
            self.fetch_and_save_stock_data(code)
            if skip_analysis: return None
            return self.analyze_stock(code)
        except Exception as e:
            logger.error(f"[{code}] 异常: {e}")
            return None

    def run(self, stock_codes: Optional[List[str]] = None, dry_run: bool = False, send_notification: bool = True) -> List[AnalysisResult]:
        start_time = time.time()
        
        # --- 核心修改逻辑开始 ---
        # 1. 获取基础列表
        if stock_codes is None:
            stock_codes = self.config.stock_list
        
        # 2. 动态抓取当日涨幅榜前10只热门股
        hot_stocks = self.get_top_n_hot_stocks(n=10)
        
        # 3. 合并并去重
        final_stock_list = list(dict.fromkeys(stock_codes + hot_stocks)) 
        # --- 核心修改逻辑结束 ---

        if not final_stock_list:
            logger.error("待分析列表为空")
            return []
        
        logger.info(f"===== 分析开始，总计 {len(final_stock_list)} 只 (含热门股) =====")
        
        results: List[AnalysisResult] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_code = {executor.submit(self.process_single_stock, code, skip_analysis=dry_run): code for code in final_stock_list}
            for future in as_completed(future_to_code):
                try:
                    res = future.result()
                    if res: results.append(res)
                except Exception as e:
                    logger.error(f"执行失败: {e}")
        
        if results and send_notification and not dry_run:
            self._send_notifications(results)
        return results

    def _send_notifications(self, results: List[AnalysisResult]) -> None:
        try:
            report = self.notifier.generate_dashboard_report(results)
            self.notifier.save_report_to_file(report)
            if self.notifier.is_available():
                self.notifier.send(report)
        except Exception as e:
            logger.error(f"推送失败: {e}")

# ... (parse_arguments, run_market_review, run_full_analysis 等后续函数保持不变)

def run_market_review(notifier, analyzer=None, search_service=None) -> Optional[str]:
    logger.info("执行大盘复盘...")
    try:
        market_analyzer = MarketAnalyzer(search_service=search_service, analyzer=analyzer)
        review_report = market_analyzer.run_daily_review()
        if review_report:
            notifier.send(f"🎯 大盘复盘\n\n{review_report}")
            return review_report
    except Exception as e:
        logger.error(f"大盘复盘失败: {e}")
    return None

def run_full_analysis(config, args, stock_codes):
    try:
        pipeline = StockAnalysisPipeline(config=config, max_workers=args.workers)
        # 个股分析 (内部已包含自动抓取热门股逻辑)
        results = pipeline.run(stock_codes=stock_codes, dry_run=args.dry_run, send_notification=not args.no_notify)
        
        # 大盘复盘
        market_report = ""
        if config.market_review_enabled and not args.no_market_review:
            market_report = run_market_review(pipeline.notifier, pipeline.analyzer, pipeline.search_service)

        # 飞书云文档生成
        try:
            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                tz_cn = timezone(timedelta(hours=8))
                doc_title = f"{datetime.now(tz_cn).strftime('%Y-%m-%d %H:%M')} 综合复盘报告"
                full_content = ""
                if market_report: full_content += f"# 📈 大盘复盘\n\n{market_report}\n\n"
                if results: full_content += f"# 🚀 决策仪表盘\n\n{pipeline.notifier.generate_dashboard_report(results)}"
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url: pipeline.notifier.send(f"✅ 飞书文档已生成: {doc_url}")
        except Exception as e:
            logger.error(f"飞书同步失败: {e}")
    except Exception as e:
        logger.exception(f"主流程失败: {e}")

def main() -> int:
    args = parse_arguments()
    config = get_config()
    setup_logging(debug=args.debug, log_dir=config.log_dir)
    
    stock_codes = None
    if args.stocks:
        stock_codes = [code.strip() for code in args.stocks.split(',') if code.strip()]
    
    if args.market_review:
        run_market_review(NotificationService(), GeminiAnalyzer(api_key=config.gemini_api_key))
        return 0
    
    run_full_analysis(config, args, stock_codes)
    return 0

def parse_arguments():
    parser = argparse.ArgumentParser(description='A股智能分析系统')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--stocks', type=str)
    parser.add_argument('--no-notify', action='store_true')
    parser.add_argument('--workers', type=int)
    parser.add_argument('--market-review', action='store_true')
    parser.add_argument('--no-market-review', action='store_true')
    parser.add_argument('--schedule', action='store_true')
    return parser.parse_args()

if __name__ == "__main__":
    sys.exit(main())
