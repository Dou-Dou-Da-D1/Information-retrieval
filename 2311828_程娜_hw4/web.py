import os
import streamlit as st
import re
from datetime import datetime
from elasticsearch import Elasticsearch
import json

# 配置页面
st.set_page_config(page_title='南开资源站', layout='wide')

# 隐藏Streamlit默认菜单和页脚
hide_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""
st.markdown(hide_style, unsafe_allow_html=True)

# 页面标题
st.header("欢迎来到南开资源站！")
st.subheader("查询你想要的南开资源信息")

# Elasticsearch 配置
ES_URL = "http://localhost:9200"
ES_USER = ('elastic', '123456')

# 尝试连接Elasticsearch
try:
    es = Elasticsearch(
        hosts=[ES_URL],
        basic_auth=ES_USER,
        request_timeout=10,
        max_retries=3,
        retry_on_timeout=True
    )
    if not es.ping():
        st.error(f"❌ 无法连接到Elasticsearch服务: {ES_URL}")
        st.info("请检查: 1) Elasticsearch服务是否已启动 2) 连接地址是否正确 3) 认证信息是否正确")
    else:
        st.success("✅ 已成功连接到Elasticsearch")
except Exception as e:
    st.error(f"❌ 连接Elasticsearch时出错: {str(e)}")
    st.info("请检查Elasticsearch服务状态和配置")

# 记录查询日志
def log_search(query_type, params):
    log_entry = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - {query_type}: {json.dumps(params)}\n"
    try:
        with open('./search_log.txt', 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        st.error(f"记录日志时出错: {str(e)}")

# 站内查询功能
def in_web_search(webtext, keytext=None):
    if not webtext:
        return {"hits": {"total": {"value": 0}, "hits": []}}

    # 构建查询体
    query = {
        "bool": {
            "must": [

                {"term": {"url.keyword": webtext}}
            ]
        }
    }

    # 如果提供了关键词，添加内容匹配条件
    if keytext:
        query["bool"]["must"].append({"match": {"content": keytext}})

    try:
        # 执行搜索
        result = es.search(index="web_pages", body={"query": query, "size": 20})
        return result
    except Exception as e:
        st.error(f"查询时出错: {str(e)}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

# 文档查询功能
def doc_search(itext):
    """文档查询：支持按文档ID、关键词、文档类型（扩展名）查询，适配网页附件下载链接场景"""
    if not itext:
        return {"hits": {"total": {"value": 0}, "hits": []}}

    # 尝试识别是否是文档扩展名（如pdf、docx等）查询
    doc_extensions = ["doc", "docx", "pdf", "xls", "xlsx"]
    ext_pattern = re.compile(r'\b(' + '|'.join(doc_extensions) + r')\b', re.IGNORECASE)
    ext_match = ext_pattern.search(itext)

    if re.match(r'^\d+$', itext):
        # 数字ID查询
        query = {
            "term": {
                "doc_id": itext
            }
        }
    elif ext_match:
        # 按文档扩展名（类型）查询
        file_ext = ext_match.group(1).lower()
        query = {
            "term": {
                "file_type": file_ext
            }
        }
    else:
        # 关键词查询，可结合文档内容、标题等字段，这里示例用content
        query = {
            "multi_match": {
                "query": itext,
                "fields": ["content", "title"],
                "analyzer": "ik_max_word"  # 中文分词，按实际需求换分词器
            }
        }

    try:

        return es.search(index="web_pages", body={"query": query, "size": 20})
    except Exception as e:
        st.error(f"查询时出错: {str(e)}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

# 短语查询功能
def phrase_search(keytexts, slop=2, fields=["content", "title"], analyzer="ik_max_word"):
    if not keytexts:
        return {"hits": {"total": {"value": 0}, "hits": []}}

    # 确保keytexts是列表
    if isinstance(keytexts, str):
        keytexts = [keytexts]

    # 构建查询体
    query = {
        "bool": {
            "should": []
        }
    }

    # 为每个字段和每个短语添加查询条件
    for keytext in keytexts:
        for field in fields:
            query["bool"]["should"].append({
                "match_phrase": {
                    field: {
                        "query": keytext,
                        "slop": slop,
                        "analyzer": analyzer
                    }
                }
            })

    # 至少匹配一个条件
    query["bool"]["minimum_should_match"] = 1

    try:
        # 高亮显示匹配的短语
        highlight = {
            "fields": {
                field: {"pre_tags": ["<b>"], "post_tags": ["</b>"]} for field in fields
            }
        }

        # 执行搜索，包含高亮
        return es.search(
            index="web_pages",
            body={"query": query, "highlight": highlight, "size": 20}
        )
    except Exception as e:
        st.error(f"查询时出错: {str(e)}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

# 通配查询功能
def wildcard_search(query_text, fields=["content", "title"], index_name="web_pages", max_results=20):

    if not query_text:
        return {"hits": {"total": {"value": 0}, "hits": []}}

    # 检查并优化通配符查询
    query_text = query_text.strip()

    # 警告前导通配符可能导致的性能问题
    if query_text.startswith('*'):
        st.warning("⚠️ 前导通配符可能导致查询性能下降，请尽量避免使用")

    # 构建多字段通配查询
    query = {
        "bool": {
            "should": []
        }
    }

    # 为每个字段添加通配查询条件
    for field in fields:
        query["bool"]["should"].append({
            "wildcard": {
                field: {
                    "value": query_text,
                    "boost": 2.0 if field == "title" else 1.0  # 标题匹配权重更高
                }
            }
        })

    # 至少匹配一个条件
    query["bool"]["minimum_should_match"] = 1

    try:
        # 执行查询并返回结果
        return es.search(
            index=index_name,
            body={
                "query": query,
                "size": max_results,
                "highlight": {
                    "fields": {field: {} for field in fields}
                }
            }
        )
    except Exception as e:
        st.error(f"查询时出错: {str(e)}")
        return {"hits": {"total": {"value": 0}, "hits": []}}

# 网页快照查询功能
def web_snapshot(url):
    """网页快照：根据URL查询存档内容"""
    if not url:
        return {"success": False, "message": "请输入URL"}

    query = {"term": {"url": url}}
    try:
        res = es.search(index="web_pages", body={"query": query, "size": 1})
        if res['hits']['total']['value'] > 0:
            return {
                "success": True,
                "timestamp": res['hits']['hits'][0]['_source'].get('timestamp', '未知'),
                "snapshot": res['hits']['hits'][0]['_source']
            }
        else:
            return {"success": False, "message": "未找到该URL的快照"}
    except Exception as e:
        st.error(f"查询时出错: {str(e)}")
        return {"success": False, "message": f"查询出错: {str(e)}"}

# 获取查询日志
def get_search_log():
    try:
        if not os.path.exists('./search_log.txt'):
            return []

        with open('./search_log.txt', 'r', encoding='utf-8') as f:
            logs = f.readlines()
        return logs
    except Exception as e:
        st.error(f"读取日志时出错: {str(e)}")
        return []

# 登录功能
def login(name, psw):

    return True

# 注册功能
def signup(name, psw):

    pass

name = st.text_input('用户名')

# 设置左侧导航栏
sidebar = st.sidebar.radio(
    "南开要闻",
    ("首页", "站内查询", "文档查询", "短语查询", "通配查询", "查询日志", "网页快照", "登录注册")
)

if sidebar == '站内查询':
    st.title('站内查询')
    # 创建一个表单
    with st.form('站内查询'):
        webtext = st.text_input('在网址中查询')
        keytext = st.text_input('关键词')
        confirm = st.form_submit_button('确认')
        # 点击按钮
    if confirm:
        res = in_web_search(webtext, keytext)
        st.success('查询到了' + str(res['hits']['total']['value']) + '条结果')
        for hit in res['hits']['hits']:
            st.write(hit['_source'])
        log_search("站内查询", {"webtext": webtext, "keytext": keytext})

elif sidebar == '文档查询':
    st.title('文档查询')
    # 创建一个表单
    with st.form('文档查询'):
        itext = st.text_input('查询文档的ID')
        confirm = st.form_submit_button('确认')
    if confirm:
        res = doc_search(itext)
        st.write('查询到了' + str(res['hits']['total']['value']) + '条结果')
        for hit in res['hits']['hits']:
            st.write(hit['_source'])
        log_search("文档查询", {"itext": itext})

elif sidebar == '短语查询':
    st.title('短语查询')
    # 创建一个表单
    with st.form('短语查询'):
        keytext = st.text_input('查询的短语')
        confirm = st.form_submit_button('确认')
    if confirm:
        res = phrase_search(keytext)
        st.write('查询到了' + str(res['hits']['total']['value']) + '条结果')
        for hit in res['hits']['hits']:
            st.write(hit['_source'])
        log_search("短语查询", {"keytext": keytext})

elif sidebar == '通配查询':
    st.title('通配查询')
    # 创建一个表单
    with st.form('通配查询'):
        keytext = st.text_input('通配查询关键词')
        confirm = st.form_submit_button('确认')
    if confirm:
        res = wildcard_search(keytext)
        st.write('查询到了' + str(res['hits']['total']['value']) + '条结果')
        for hit in res['hits']['hits']:
            st.write(hit['_source'])
        log_search("通配查询", {"keytext": keytext})

elif sidebar == '查询日志':
    st.title('查询日志')
    logs = get_search_log()
    if logs:
        st.write("### 最近查询记录：")
        for log in logs[-20:]:  # 显示最近20条记录
            st.write(log.strip())
    else:
        st.info("暂无查询记录")

elif sidebar == '网页快照':
    st.title('网页快照查询')
    with st.form('snapshot_search'):
        url = st.text_input('输入需要查询的URL（如\'http://nankai.edu.cn/news\'）')
        submit = st.form_submit_button('查询快照')

    if submit:
        res = web_snapshot(url)
        if res["success"]:
            st.success(f"找到快照，存档时间：{res['timestamp']}")
            st.write(f"### {res['snapshot']['title']}")
            st.write(res['snapshot']['content'])
        else:
            st.warning(res['message'])
        log_search("网页快照", {"url": url})

elif sidebar == '登录注册':
    log_in = st.button('登录')
    sign_up = st.button('注册')
    if log_in:
        with st.form('登录'):
            name = st.text_input('用户名')
            psw = st.text_input('密码')
            confirm = st.form_submit_button('登录')
            if confirm:
                if login(name, psw):
                    st.balloons()
                    st.success('Login successfully!')
                else:
                    st.write('Permission denied.')
    if sign_up:
        with st.form('注册'):
            name = st.text_input('用户名')
            psw = st.text_input('密码')
            confirm = st.form_submit_button('注册')
            if confirm:
                signup(name, psw)
                st.balloons()
                st.success('Sign up successfullly!')
else:
    st.header("欢迎来到南开资源站！")
    st.subheader("查询你想要的南开资源信息")
    st.image("nankai_logo.png", use_column_width=True)  # 假设存在校徽图片
    st.write("""
    本搜索引擎专注于南开大学校内资源的检索，支持站内查询、文档查询、短语查询、通配查询等功能，
    为师生提供便捷的信息获取渠道。系统已爬取超过10万篇校内网页，覆盖新闻、公告、学术资源等场景。
    """)
    st.info("请通过左侧菜单选择具体功能")