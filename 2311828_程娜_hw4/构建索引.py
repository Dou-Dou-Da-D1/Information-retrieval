import csv
import os
import warnings
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from urllib.parse import quote, urljoin

# 忽略安全警告
warnings.filterwarnings("ignore", category=ElasticsearchWarning)

# 配置参数
CSV_FILE = "D:/Projects/PycharmProjects/1/title2url.csv"  # CSV文件路径
HTML_DIR = "D:/Projects/PycharmProjects/pages"  # HTML文件目录
ES_HOST = "http://localhost:9200"  # Elasticsearch主机
INDEX_NAME = "web_pages"  # 索引名称

# 连接到Elasticsearch
es = Elasticsearch(hosts=[ES_HOST])


# 从URL中提取域名
def extract_domain(url):
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        return parsed_url.netloc
    except:
        return ""


# 检查索引是否存在，如果不存在则创建
if not es.indices.exists(index=INDEX_NAME):
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "ik_max_word": {
                        "type": "custom",
                        "tokenizer": "ik_max_word"
                    },
                    "ik_smart": {
                        "type": "custom",
                        "tokenizer": "ik_smart"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "ik_max_word"},
                "content": {"type": "text", "analyzer": "ik_max_word"},
                "pagerank": {"type": "float"},
                "domain": {"type": "keyword"},
                "date": {"type": "date"},
                "anchor_texts": {  # 新增锚文本字段映射
                    "type": "nested",
                    "properties": {
                        "text": {"type": "text", "analyzer": "ik_max_word"},
                        "url": {"type": "keyword"}
                    }
                }
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=index_settings)
    print(f"已创建新索引: {INDEX_NAME}")


# 解析HTML文件内容并提取锚文本
def parse_html(html_file, current_url):
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        soup = BeautifulSoup(content, 'html.parser')

        # 移除脚本和样式标签
        for script in soup(["script", "style"]):
            script.extract()

        # 获取文本内容
        text = soup.get_text(separator=' ')

        # 清理文本
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)

        # 提取所有锚文本及其链接
        anchor_texts = []
        for a_tag in soup.find_all('a'):
            href = a_tag.get('href', '')
            anchor_text = a_tag.get_text(strip=True)
            if href and anchor_text:
                # 规范化URL（处理相对路径）
                if href.startswith(('http://', 'https://')):
                    full_url = href
                else:
                    full_url = urljoin(current_url, href)  # 基于当前页面URL构建绝对路径

                anchor_texts.append({
                    'text': anchor_text,
                    'url': full_url
                })

        return text, anchor_texts  # 返回文本和锚文本
    except Exception as e:
        print(f"解析HTML文件 {html_file} 时出错: {e}")
        return "", []


# 处理单个文档的导入
def index_document(title, url, filename):
    # 构建HTML文件路径
    html_file = os.path.join(HTML_DIR, filename)

    # 检查HTML文件是否存在
    if not os.path.exists(html_file):
        print(f"HTML文件不存在: {html_file}")
        return False

    # 读取HTML内容并提取锚文本
    try:
        content, anchor_texts = parse_html(html_file, url)  # 传入当前URL处理相对路径
    except Exception as e:
        print(f"读取HTML文件失败: {e}")
        return False

    # 提取域名
    domain = extract_domain(url)

    # 从URL中提取日期
    date = None
    try:
        import re
        date_pattern = r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
        match = re.search(date_pattern, url)
        if match:
            year, month, day = match.groups()
            date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    except:
        pass

    # 索引文档（包含锚文本）
    try:
        response = es.index(
            index=INDEX_NAME,
            id=url,
            document={
                "url": url,
                "title": title,
                "content": content,
                "domain": domain,
                "date": date,
                "pagerank": 1.0,
                "anchor_texts": anchor_texts  # 新增锚文本字段
            }
        )
        print(f"成功索引文档: {url}，提取到 {len(anchor_texts)} 个锚文本")
        return True
    except Exception as e:
        print(f"索引文档时出错 {url}: {e}")
        return False


# 主程序：处理CSV文件中的所有记录
def main():
    success_count = 0
    failure_count = 0

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # 跳过标题行

        for i, row in enumerate(reader):
            if len(row) >= 3:
                title, url, filename = row
                print(f"处理第 {i + 1} 条记录: {title}")

                if index_document(title, url, filename):
                    success_count += 1
                else:
                    failure_count += 1

                # 每10条记录刷新一次索引
                if (i + 1) % 10 == 0:
                    es.indices.refresh(index=INDEX_NAME)
                    print(f"已处理 {i + 1} 条记录，成功 {success_count} 条，失败 {failure_count} 条")

    # 最终刷新索引
    es.indices.refresh(index=INDEX_NAME)

    # 获取文档总数
    count = es.count(index=INDEX_NAME)
    print(f"\n导入完成!")
    print(f"总记录数: {success_count + failure_count}")
    print(f"成功: {success_count}")
    print(f"失败: {failure_count}")
    print(f"索引中当前文档数: {count['count']}")


# 验证锚文本索引的辅助函数
def verify_anchor_text(url):
    """查询指定URL的文档，验证锚文本是否正确索引"""
    try:
        doc = es.get(index=INDEX_NAME, id=url)
        anchor_texts = doc['_source'].get('anchor_texts', [])
        print(f"文档 {url} 包含 {len(anchor_texts)} 个锚文本:")
        for i, anchor in enumerate(anchor_texts[:5]):
            print(f"  {i + 1}. 文本: '{anchor['text']}', 链接: {anchor['url']}")
        if len(anchor_texts) > 5:
            print("  ... 更多锚文本未显示")
        return anchor_texts
    except Exception as e:
        print(f"查询锚文本失败: {e}")
        return []


if __name__ == "__main__":
    main()

    # 示例：验证锚文本索引
    # verify_anchor_text("http://news.nankai.edu.cn/zhxw/system/2008/09/16/000018566.shtml")