import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
import re
import hashlib


# 配置部分
URLS = [
    f'https://news.nankai.edu.cn/nkrw/system/count//0008000/000000000000/000/000/c0008000000000000000_0000000{i}.shtml'
    for i in range(10,68)
]
URLS.append('https://news.nankai.edu.cn/nkrw/index.shtml')
OUTPUT_DIR = 'pages'
CSV_PATH = 'title2url.csv'

# 创建保存HTML的文件夹
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 初始化标题与URL映射表
title2url_df = pd.DataFrame(columns=['url', 'filename'])
title2url_df.index.name = 'title'


def crawlIndex(urls):
    """爬取索引页，提取新闻链接并保存内容"""
    cnt = 0
    for url in urls:
        cnt += 1
        print(f"{cnt}/{len(urls)}: Processing Index Page: {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # 主动检查HTTP错误

            soup = BeautifulSoup(response.content, 'html.parser')
            # 提取新闻链接
            links = soup.find_all('a', href=re.compile(r'/system/\d{4}/'))

            for link in links:
                href = link['href']
                if href.startswith('/'):
                    href = f"https://news.nankai.edu.cn{href}"
                text = link.get_text().strip()

                if len(text) > 0 and "index.shtml" not in href:
                    # 直接保存HTML并记录映射
                    save_article(href, text)

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error for index page {url}: {e}")
        except Exception as e:
            print(f"⚠️ Unexpected error processing index page {url}: {e}")


def save_article(url, title):
    """保存单篇新闻的HTML内容并记录标题与URL映射"""
    global title2url_df  # 确保使用全局DataFrame

    try:
        # 过滤标题中的非法字符
        valid_title = re.sub(r'[<>:"/\\|?*]', '_', title)
        # 使用URL的哈希值确保唯一性
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{valid_title}_{url_hash}.html"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # 检查文件是否已存在
        if os.path.exists(filepath):
            print(f"  ⏩ Skipping existing file: {filename}")
        else:
            # 获取文章内容
            response = requests.get(url, timeout=15)
            response.raise_for_status()  # 主动检查HTTP错误

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"  ✅ Saved: {filename}")

        # 记录标题、URL和文件名的映射关系（无论文件是否已存在）
        title2url_df.loc[valid_title] = {'url': url, 'filename': filename}

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ Request error for article {url}: {e}")
    except Exception as e:
        print(f"  ⚠️ Unexpected error saving article {url}: {e}")


def save_mapping_to_csv():
    """将标题与URL的映射保存到CSV文件"""
    try:
        if not title2url_df.empty:
            title2url_df.to_csv(CSV_PATH, encoding='utf-8-sig')
            print(f"📊 Saved {len(title2url_df)} article mappings to {CSV_PATH}")
            print(f"🔍 Example mapping: {title2url_df.head(1).to_dict('records')}")
        else:
            print("⚠️ No articles were processed. Check the log for errors.")
    except Exception as e:
        print(f"⚠️ Error saving CSV: {e}")
        # 保存失败时，输出映射表内容到控制台供调试
        if not title2url_df.empty:
            print("📋 Mapping table content:")
            print(title2url_df.head().to_csv(sep='\t', na_rep='nan'))


if __name__ == '__main__':
    try:
        print(f"🚀 Starting crawler. Saving HTML files to '{OUTPUT_DIR}', mapping to '{CSV_PATH}'")
        crawlIndex(URLS)
        save_mapping_to_csv()
        print("✅ Crawling completed.")
    except KeyboardInterrupt:
        print("\n🛑 Crawling interrupted by user.")
        save_mapping_to_csv()  # 即使中断也尝试保存已抓取的数据
    except Exception as e:
        print(f"⚠️ Fatal error: {e}")
        save_mapping_to_csv()  # 发生严重错误时也尝试保存