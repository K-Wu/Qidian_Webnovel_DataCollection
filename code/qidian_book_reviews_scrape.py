# -*- coding: utf-8 -*-
"""
抓取起点中文网指定书籍的所有画线评（段落评论）
基于 qidian_review_scrape.py 和 qidian_chapter_date_scrape.py 的实现

用法: python qidian_book_reviews_scrape.py <bookId>
示例: python qidian_book_reviews_scrape.py 1035420986
"""
import os
import sys
import io
import json
import time
import requests
import numpy as np
import pandas as pd
from requests.exceptions import ConnectionError

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("提示: 未安装selenium，请运行 pip install selenium")


class QidianScraper:
    """起点中文网爬虫，通过浏览器获取Token"""

    def __init__(self, debug=False):
        self.debug = debug
        self.session = requests.Session()
        self.csrf_token = None
        self.w_tsfp = None
        self.cookies = {}

        # 基础请求头
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def _debug_print(self, msg):
        if self.debug:
            print(f"  [DEBUG] {msg}")

    def init_tokens_via_browser(self, bookId):
        """
        打开浏览器窗口让用户操作，完成验证后获取Cookie
        """
        if not SELENIUM_AVAILABLE:
            print("错误: 需要安装selenium才能使用浏览器模式")
            print("请运行: pip install selenium")
            return False

        print("正在启动浏览器...")
        print("=" * 50)
        print("请在浏览器中完成以下操作：")
        print("1. 如果出现验证码，请完成验证")
        print("2. 等待页面正常加载完成")
        print("3. 脚本会自动检测并关闭浏览器")
        print("=" * 50)

        driver = None
        success = False
        try:
            # 配置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--start-maximized")
            # 禁用日志输出
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

            # 启动浏览器
            driver = webdriver.Chrome(options=chrome_options)

            # 执行CDP命令隐藏webdriver特征
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                """
            })

            # 访问书籍页面
            book_url = f"https://www.qidian.com/book/{bookId}/"
            print(f"\n正在访问: {book_url}")
            driver.get(book_url)

            # 等待页面加载，检测是否成功获取到_csrfToken
            print("等待页面加载和Cookie生成...")
            max_wait = 120
            start_time = time.time()

            while time.time() - start_time < max_wait:
                try:
                    # 获取当前cookies
                    selenium_cookies = driver.get_cookies()
                    cookie_dict = {c['name']: c['value'] for c in selenium_cookies}

                    self._debug_print(f"当前cookies: {list(cookie_dict.keys())}")

                    # 检查是否有_csrfToken
                    if '_csrfToken' in cookie_dict:
                        self.csrf_token = cookie_dict['_csrfToken']
                        self.w_tsfp = cookie_dict.get('w_tsfp', '')
                        self.cookies = cookie_dict

                        # 将cookies设置到requests session
                        for name, value in cookie_dict.items():
                            self.session.cookies.set(name, value)

                        print(f"\n成功获取Token!")
                        print(f"  _csrfToken: {self.csrf_token[:20]}...")
                        if self.w_tsfp:
                            print(f"  w_tsfp: {self.w_tsfp[:30]}...")

                        success = True
                        break

                    # 检查页面标题
                    title = driver.title
                    if "验证" in title or "安全" in title:
                        print(f"\r检测到验证页面，请完成验证... (已等待 {int(time.time() - start_time)}秒)", end="", flush=True)
                    else:
                        print(f"\r等待Cookie生成... (已等待 {int(time.time() - start_time)}秒)", end="", flush=True)
                except Exception as e:
                    self._debug_print(f"检查cookie时出错: {e}")

                time.sleep(2)

            if not success:
                print("\n\n超时：未能在规定时间内获取到Cookie")
                print("请确保页面已正常加载，并刷新重试")

        except Exception as e:
            print(f"\n浏览器操作出错: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
        finally:
            # 确保关闭浏览器
            if driver:
                print("\n正在关闭浏览器...", flush=True)
                try:
                    driver.quit()
                    print("driver.quit() 完成", flush=True)
                except Exception as e:
                    print(f"driver.quit() 出错: {e}", flush=True)
                driver = None
                print("浏览器已关闭", flush=True)

        print("init_tokens_via_browser 完成，返回:", success, flush=True)
        return success

    def refresh_tokens(self, bookId):
        """刷新Token - 重新打开浏览器"""
        print("\nToken可能已过期，需要重新获取...")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })
        return self.init_tokens_via_browser(bookId)

    def _get_api_headers(self, referer):
        """获取API请求头"""
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": referer,
            "X-Requested-With": "XMLHttpRequest",
        }

    def _make_request(self, url, params, referer, bookId, retry=True):
        """发起API请求，自动处理Token过期"""
        headers = self._get_api_headers(referer)

        # 添加token到参数
        if self.csrf_token:
            params['_csrfToken'] = self.csrf_token
        if self.w_tsfp:
            params['w_tsfp'] = self.w_tsfp

        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            resp.encoding = 'utf-8'  # 强制使用UTF-8编码

            self._debug_print(f"响应状态: {resp.status_code}, 长度: {len(resp.text)}")

            if not resp.text or resp.status_code == 202:
                if retry:
                    self._debug_print("响应为空或被拦截，尝试刷新Token...")
                    if self.refresh_tokens(bookId):
                        return self._make_request(url, params, referer, bookId, retry=False)
                return None

            data = json.loads(resp.text)

            # 检查是否需要刷新token
            if data.get('code') in [-1, 1001, 1002] and retry:
                self._debug_print(f"API返回错误码 {data.get('code')}，尝试刷新Token...")
                if self.refresh_tokens(bookId):
                    return self._make_request(url, params, referer, bookId, retry=False)
                return None

            return data

        except json.JSONDecodeError as e:
            self._debug_print(f"JSON解析错误: {e}")
            if retry:
                if self.refresh_tokens(bookId):
                    return self._make_request(url, params, referer, bookId, retry=False)
            return None
        except Exception as e:
            self._debug_print(f"请求出错: {e}")
            return None

    def get_all_chapters(self, bookId):
        """获取书籍的所有章节"""
        url = "https://www.qidian.com/ajax/book/category"
        params = {"bookId": bookId}
        referer = f"https://www.qidian.com/book/{bookId}/"

        data = self._make_request(url, params, referer, bookId)

        if not data or data.get('code') != 0:
            self._debug_print(f"获取章节失败: {data}")
            return []

        chapters = []
        for vol in data['data'].get('vs', []):
            for chapter in vol.get('cs', []):
                chapters.append({
                    'chapterId': chapter['id'],
                    'chapterName': chapter['cN'],
                    'updateTime': chapter.get('uT', ''),
                    'isFree': vol.get('vS', 1) == 0
                })
        return chapters

    def get_chapter_comment_summary(self, bookId, chapterId, referer):
        """获取章节评论摘要"""
        url = "https://www.qidian.com/ajax/chapterReview/reviewSummary"
        params = {"bookId": bookId, "chapterId": chapterId}

        data = self._make_request(url, params, referer, bookId)

        if data and data.get('code') == 0 and data.get('data', {}).get('list'):
            df = pd.DataFrame(data['data']['list'])
            self._debug_print(f"章节摘要列名: {df.columns.tolist()}")
            return df
        return pd.DataFrame()

    def get_segment_comments(self, bookId, chapterId, segmentId, referer):
        """获取段落的所有评论"""
        url = "https://www.qidian.com/ajax/chapterReview/reviewList"
        page = 1
        comments = []

        while True:
            params = {
                "bookId": bookId,
                "chapterId": chapterId,
                "segmentId": segmentId,
                "page": str(page),
                "pageSize": "20",
                "type": "2"
            }

            data = self._make_request(url, params, referer, bookId, retry=(page == 1))

            if data and data.get('code') == 0:
                review_list = data.get('data', {}).get('list', [])
                if review_list:
                    comments.extend(review_list)
                    page += 1
                    if page == 2:
                        self._debug_print(f"第一页评论示例: {json.dumps(review_list[0], ensure_ascii=False)[:200]}")
                else:
                    break
            else:
                break

        return comments


def scrape_book_reviews(bookId, output_dir=None, debug=False):
    """抓取整本书的所有画线评"""
    if output_dir is None:
        output_dir = "data/qidianBookReviews"

    # 创建书籍目录和章节子目录
    book_dir = os.path.join(output_dir, bookId)
    chapters_dir = os.path.join(book_dir, "chapters")
    os.makedirs(chapters_dir, exist_ok=True)

    # 创建爬虫实例
    scraper = QidianScraper(debug=debug)

    # 通过浏览器初始化Token
    print("准备初始化Token...", flush=True)
    if not scraper.init_tokens_via_browser(bookId):
        print("无法获取Token，退出")
        return []

    print("Token初始化完成，继续执行...", flush=True)

    # 获取章节列表
    print(f"\n正在获取书籍 {bookId} 的章节列表...", flush=True)
    chapters = scraper.get_all_chapters(bookId)

    if not chapters:
        print("无法获取章节列表")
        return []

    print(f"找到 {len(chapters)} 个章节")

    # 筛选免费章节
    free_chapters = [ch for ch in chapters if ch['isFree']]
    print(f"其中 {len(free_chapters)} 个免费章节")

    if not free_chapters:
        print("没有免费章节，尝试使用所有章节...")
        free_chapters = chapters

    # 检查已完成的章节（支持断点续传）
    completed_chapters = set()
    for f in os.listdir(chapters_dir):
        if f.endswith('.csv'):
            completed_chapters.add(f.replace('.csv', ''))

    if completed_chapters:
        print(f"发现 {len(completed_chapters)} 个已完成的章节，将跳过")

    referers = ['https://www.google.com', 'https://www.qidian.com', 'https://www.bing.com']
    total_reviews = 0

    for i, chapter in enumerate(free_chapters):
        chapterId = str(chapter['chapterId'])
        chapterName = chapter['chapterName']

        # 跳过已完成的章节
        if chapterId in completed_chapters:
            print(f"\n[{i+1}/{len(free_chapters)}] 跳过已完成章节: {chapterName} (ID: {chapterId})")
            continue

        print(f"\n[{i+1}/{len(free_chapters)}] 处理章节: {chapterName} (ID: {chapterId})")

        try:
            referer = referers[np.random.randint(0, len(referers))]

            # 获取章节评论摘要
            summary_df = scraper.get_chapter_comment_summary(bookId, chapterId, referer)

            if summary_df.empty:
                print(f"  该章节没有画线评")
                # 创建空文件标记已处理
                empty_df = pd.DataFrame()
                empty_df.to_csv(os.path.join(chapters_dir, f"{chapterId}.csv"), index=False, encoding='utf-8-sig')
                continue

            print(f"  发现 {len(summary_df)} 个有评论的段落")

            # 获取评论数字段名
            amount_col = None
            for col in ['reviewAmount', 'amount', 'count']:
                if col in summary_df.columns:
                    amount_col = col
                    break

            chapter_reviews = []

            for j, (_, segment) in enumerate(summary_df.iterrows()):
                segmentId = str(segment['segmentId'])
                reviewAmount = segment.get(amount_col, '?') if amount_col else '?'

                # 获取段落评论
                comments = scraper.get_segment_comments(bookId, chapterId, segmentId, referer)

                print(f"    段落 {segmentId}: 获取到 {len(comments)} 条评论")

                for comment in comments:
                    comment['chapterId'] = chapterId
                    comment['chapterName'] = chapterName
                    chapter_reviews.append(comment)

                time.sleep(0.3)

            # 保存章节文件
            if chapter_reviews:
                chapter_df = pd.DataFrame(chapter_reviews)
                chapter_file = os.path.join(chapters_dir, f"{chapterId}.csv")
                chapter_df.to_csv(chapter_file, index=False, encoding='utf-8-sig')
                print(f"  已保存章节文件: {chapter_file} ({len(chapter_reviews)} 条评论)")
                total_reviews += len(chapter_reviews)
            else:
                # 创建空文件标记已处理
                empty_df = pd.DataFrame()
                empty_df.to_csv(os.path.join(chapters_dir, f"{chapterId}.csv"), index=False, encoding='utf-8-sig')

            time.sleep(0.5)

        except ConnectionError as e:
            print(f"  连接错误: {e}，等待30秒后继续...")
            time.sleep(30)
            scraper.refresh_tokens(bookId)
        except Exception as e:
            print(f"  处理出错: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            continue

    # 合并所有章节文件为总文件
    print(f"\n正在合并所有章节文件...")
    all_reviews = []
    chapter_files = [f for f in os.listdir(chapters_dir) if f.endswith('.csv')]

    for chapter_file in chapter_files:
        file_path = os.path.join(chapters_dir, chapter_file)
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            if not df.empty:
                all_reviews.append(df)
        except Exception as e:
            print(f"  读取 {chapter_file} 出错: {e}")

    if all_reviews:
        merged_df = pd.concat(all_reviews, ignore_index=True)
        output_file = os.path.join(book_dir, f"{bookId}_all.csv")
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n抓取完成！共获取 {len(merged_df)} 条画线评")
        print(f"章节文件保存在: {chapters_dir}")
        print(f"汇总文件保存到: {output_file}")
        return merged_df.to_dict('records')
    else:
        print("\n未找到任何画线评")
        return []


if __name__ == "__main__":
    if len(sys.argv) < 2:
        bookId = "1035420986"
    else:
        bookId = sys.argv[1]

    debug = "--debug" in sys.argv

    print(f"开始抓取书籍 {bookId} 的画线评")
    if debug:
        print("[调试模式已启用]")
    print("=" * 50)

    reviews = scrape_book_reviews(bookId, debug=debug)
