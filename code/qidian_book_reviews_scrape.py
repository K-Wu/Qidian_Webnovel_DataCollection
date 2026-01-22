# -*- coding: utf-8 -*-
"""
抓取起点中文网指定书籍的所有画线评（段落评论）
基于 qidian_review_scrape.py 和 qidian_chapter_date_scrape.py 的实现

用法: python qidian_book_reviews_scrape.py <bookId>
示例: python qidian_book_reviews_scrape.py 1035420986
"""
import os
import re
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

    def _create_driver(self, headless=True):
        """创建浏览器驱动"""
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-infobars")

        if headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")
        else:
            chrome_options.add_argument("--start-maximized")

        driver = webdriver.Chrome(options=chrome_options)

        # 隐藏webdriver特征
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        return driver

    def _close_driver(self, driver):
        """安全关闭浏览器"""
        if driver:
            try:
                driver.quit()
            except:
                pass

    def _check_captcha(self, driver):
        """检查是否遇到验证码"""
        try:
            title = driver.title
            url = driver.current_url
            cookies = {c['name']: c['value'] for c in driver.get_cookies()}

            # 检查是否有验证码
            if "验证" in title or "安全" in title:
                return True
            # 检查是否被WAF拦截
            if 'x-waf-captcha-referer' in cookies and '_csrfToken' not in cookies:
                return True
            return False
        except:
            return False

    def _extract_tokens(self, driver):
        """从浏览器提取Token"""
        try:
            cookies = {c['name']: c['value'] for c in driver.get_cookies()}
            if '_csrfToken' in cookies:
                self.csrf_token = cookies['_csrfToken']
                self.w_tsfp = cookies.get('w_tsfp', '')
                self.cookies = cookies
                for name, value in cookies.items():
                    self.session.cookies.set(name, value)
                return True
        except:
            pass
        return False

    def init_tokens_via_browser(self, bookId):
        """
        获取Token，默认使用无头模式，遇到验证码时才显示浏览器
        """
        if not SELENIUM_AVAILABLE:
            print("错误: 需要安装selenium，请运行: pip install selenium")
            return False

        book_url = f"https://www.qidian.com/book/{bookId}/"

        # 第一步：尝试无头模式
        print("正在获取Token（后台模式）...", flush=True)
        driver = None
        try:
            driver = self._create_driver(headless=True)
            driver.get(book_url)
            time.sleep(3)  # 等待页面加载

            # 检查是否成功
            if self._extract_tokens(driver):
                print(f"成功获取Token: {self.csrf_token[:16]}...", flush=True)
                self._close_driver(driver)
                return True

            # 检查是否遇到验证码
            need_captcha = self._check_captcha(driver)
            self._close_driver(driver)
            driver = None

            if not need_captcha:
                # 再等一下重试
                print("等待重试...", flush=True)
                time.sleep(2)
                driver = self._create_driver(headless=True)
                driver.get(book_url)
                time.sleep(5)
                if self._extract_tokens(driver):
                    print(f"成功获取Token: {self.csrf_token[:16]}...", flush=True)
                    self._close_driver(driver)
                    return True
                need_captcha = self._check_captcha(driver)
                self._close_driver(driver)
                driver = None

        except Exception as e:
            self._debug_print(f"无头模式出错: {e}")
            self._close_driver(driver)
            driver = None
            need_captcha = True

        # 第二步：需要验证码，打开可见浏览器
        print("\n" + "=" * 50)
        print("检测到需要验证，正在打开浏览器...")
        print("请在浏览器中完成验证码验证")
        print("完成后脚本会自动继续")
        print("=" * 50 + "\n", flush=True)

        try:
            driver = self._create_driver(headless=False)
            driver.get(book_url)

            max_wait = 120
            start_time = time.time()

            while time.time() - start_time < max_wait:
                if self._extract_tokens(driver):
                    print(f"\n成功获取Token: {self.csrf_token[:16]}...", flush=True)
                    self._close_driver(driver)
                    return True

                elapsed = int(time.time() - start_time)
                print(f"\r等待验证完成... ({elapsed}秒)", end="", flush=True)
                time.sleep(2)

            print("\n超时：未能获取Token")

        except Exception as e:
            print(f"\n浏览器出错: {e}")
        finally:
            self._close_driver(driver)

        return False

    def refresh_tokens(self, bookId):
        """刷新Token"""
        print("\nToken可能已过期，正在刷新...", flush=True)
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })
        self.csrf_token = None
        self.w_tsfp = None
        self.cookies = {}
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

    def get_chapter_content(self, bookId, chapterId):
        """
        获取章节内容，返回 {segmentId: 原文内容} 的字典
        尝试多种方法获取
        """
        segment_content = {}

        # 方法1: 尝试使用API获取章节内容
        try:
            url = "https://www.qidian.com/ajax/chapter/chapterInfo"
            params = {"bookId": bookId, "chapterId": chapterId}
            headers = self._get_api_headers(f"https://www.qidian.com/chapter/{bookId}/{chapterId}/")

            if self.csrf_token:
                params['_csrfToken'] = self.csrf_token
            if self.w_tsfp:
                params['w_tsfp'] = self.w_tsfp

            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            resp.encoding = 'utf-8'

            if resp.status_code == 200 and resp.text:
                data = json.loads(resp.text)
                if data.get('code') == 0 and data.get('data'):
                    # 尝试从API响应中提取内容
                    chapter_data = data['data']
                    if 'content' in chapter_data:
                        content = chapter_data['content']
                        # 解析内容中的段落
                        pattern = r'<p[^>]*data-segid=["\']?(-?\d+)["\']?[^>]*>(.*?)</p>'
                        matches = re.findall(pattern, content, re.DOTALL)
                        for seg_id, text in matches:
                            clean_text = re.sub(r'<[^>]+>', '', text).strip()
                            if clean_text:
                                segment_content[seg_id] = clean_text

                    if not segment_content and 'contents' in chapter_data:
                        # 另一种格式: contents数组
                        for item in chapter_data['contents']:
                            if isinstance(item, dict):
                                seg_id = str(item.get('segmentId', item.get('id', '')))
                                text = item.get('content', item.get('text', ''))
                                if seg_id and text:
                                    clean_text = re.sub(r'<[^>]+>', '', text).strip()
                                    segment_content[seg_id] = clean_text
        except Exception as e:
            self._debug_print(f"API获取章节内容失败: {e}")

        # 方法2: 如果API失败，尝试用Selenium
        if not segment_content and SELENIUM_AVAILABLE:
            driver = None
            try:
                driver = self._create_driver(headless=True)
                url = f"https://www.qidian.com/chapter/{bookId}/{chapterId}/"
                driver.get(url)
                time.sleep(3)

                # 尝试从页面提取
                from selenium.webdriver.common.by import By
                selectors = [
                    "div.read-content p[data-segid]",
                    "div.main-text-wrap p[data-segid]",
                    "p[data-segid]",
                    "div.read-content p",
                    "main p"
                ]
                for selector in selectors:
                    try:
                        paragraphs = driver.find_elements(By.CSS_SELECTOR, selector)
                        if paragraphs:
                            for idx, p in enumerate(paragraphs):
                                seg_id = p.get_attribute('data-segid') or str(idx + 1)
                                text = p.text.strip()
                                if text and len(text) > 1:
                                    segment_content[seg_id] = text
                            if segment_content:
                                break
                    except:
                        continue
            except Exception as e:
                self._debug_print(f"Selenium获取章节内容失败: {e}")
            finally:
                self._close_driver(driver)

        self._debug_print(f"获取到 {len(segment_content)} 个段落内容")
        return segment_content


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
    if not scraper.init_tokens_via_browser(bookId):
        print("无法获取Token，退出")
        return []

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

            # 获取章节内容（原文）
            print(f"  正在获取章节原文...")
            segment_contents = scraper.get_chapter_content(bookId, chapterId)
            if segment_contents:
                print(f"  获取到 {len(segment_contents)} 个段落原文")
            else:
                print(f"  未能获取段落原文")

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
                # 获取对应的原文
                original_text = segment_contents.get(segmentId, '')

                # 获取段落评论
                comments = scraper.get_segment_comments(bookId, chapterId, segmentId, referer)

                if original_text:
                    print(f"    段落 {segmentId}: 获取到 {len(comments)} 条评论 | 原文: {original_text[:30]}...")
                else:
                    print(f"    段落 {segmentId}: 获取到 {len(comments)} 条评论")

                for comment in comments:
                    comment['chapterId'] = chapterId
                    comment['chapterName'] = chapterName
                    comment['originalText'] = original_text  # 添加原文
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
