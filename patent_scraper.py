"""
中美人工智能领域专利爬取脚本 v6
架构：
  1. requests.Session 访问主页获取 cookies（NID / 1P_JAR 等）
  2. XHR query JSON 接口逐批拉取专利列表（含标题、申请人、IPC、日期）
  3. 若 XHR 返回信息不完整，再用 urllib 抓单页补全
  4. 按 国家 × 关键词 × IPC × 季度 分批，每批最多 1000 条
  5. 断点续传（checkpoint.json）
  6. 最终去重合并

目标：中美各 ~7.5 万条，合计 ~15 万条，2015-2025 年
"""

import urllib.request
import urllib.parse
import urllib.error
import requests
import time
import random
import json
import os
import re
import logging
import pandas as pd
from bs4 import BeautifulSoup

# ============================================================
# 路径配置
# ============================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'patent_data')
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoint.json')
LOG_FILE   = os.path.join(BASE_DIR, 'scraper.log')

# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 参数
# ============================================================
COUNTRIES      = ['CN', 'US']
START_YEAR     = 2015
END_YEAR       = 2025
RESULTS_PER_PAGE = 100          # Google Patents XHR 单次最多 100 条
MAX_PAGES        = 1            # XHR start 参数无效，只取第1页（100条/月查询）
_FAST = os.environ.get('GP_FAST_MODE') == '1'   # GitHub Actions 快速模式

SESSION_REFRESH  = 50
MIN_DELAY        = 2.0  if _FAST else 8.0    # 页间延迟
MAX_DELAY        = 5.0  if _FAST else 15.0
TASK_DELAY_MIN   = 8.0  if _FAST else 20.0   # 任务间延迟（快速~10s/任务→3960*10=11h）
TASK_DELAY_MAX   = 14.0 if _FAST else 35.0
DETAIL_DELAY_MIN = 1.5  if _FAST else 3.0
DETAIL_DELAY_MAX = 3.0  if _FAST else 6.0
MAX_RETRIES      = 3
COOL_THRESHOLD   = 4
COOL_WAIT        = 600  if _FAST else 1800   # 冷却等待时间（快速模式10分钟）

# AI 关键词组（每组独立查询）
KEYWORD_GROUPS = {
    'foundation': [
        'machine learning', 'deep learning', 'neural network',
        'convolutional neural network', 'recurrent neural network',
        'reinforcement learning', 'transfer learning',
        'generative adversarial network',
    ],
    'application': [
        'computer vision', 'natural language processing',
        'speech recognition', 'image recognition',
        'object detection', 'autonomous driving',
        'recommendation system', 'face recognition',
    ],
    'method': [
        'attention mechanism', 'transformer model',
        'feature extraction', 'knowledge graph',
        'semantic segmentation', 'pattern recognition',
        'gradient descent', 'federated learning',
    ],
}

# AI 核心 IPC（每个单独查询，增加覆盖）
AI_IPC_CODES = ['G06N3', 'G06N20', 'G06N5', 'G06V', 'G10L']

# XHR 接口请求头（模拟浏览器内部 XHR 调用）
HEADERS_PAGE = {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/124.0.0.0 Safari/537.36',
    'Accept':          'text/html,application/xhtml+xml,application/xml;'
                       'q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection':      'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

HEADERS_XHR = {
    'User-Agent':         HEADERS_PAGE['User-Agent'],
    'Accept':             'application/json, text/plain, */*',
    'Accept-Language':    'en-US,en;q=0.9',
    'Accept-Encoding':    'gzip, deflate, br',
    'Connection':         'keep-alive',
    'X-Requested-With':   'XMLHttpRequest',
    'Referer':            'https://patents.google.com/',
    'Origin':             'https://patents.google.com',
    'Sec-Fetch-Dest':     'empty',
    'Sec-Fetch-Mode':     'cors',
    'Sec-Fetch-Site':     'same-origin',
    'sec-ch-ua':          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile':   '?0',
    'sec-ch-ua-platform': '"Windows"',
}

# urllib 抓单个专利页面时用普通 UA
HEADERS_URLLIB = {
    'User-Agent': HEADERS_PAGE['User-Agent'],
    'Accept-Language': 'en-US,en;q=0.9',
}


# ============================================================
# Session 管理
# ============================================================

def init_session():
    """
    新建 requests.Session，访问首页拿到必要 cookies（NID 等），
    然后再访问一次搜索页预热。
    """
    session = requests.Session()
    session.headers.update(HEADERS_PAGE)

    # 只访问一次主页拿 cookies，不做多余预热
    try:
        r = session.get('https://patents.google.com/', timeout=30)
        logger.info(f"Session初始化: HTTP {r.status_code}，"
                    f"cookies: {list(session.cookies.keys())}")
        time.sleep(random.uniform(5, 10))
    except Exception as e:
        logger.warning(f"Session初始化失败: {e}")

    return session


# ============================================================
# 工具函数
# ============================================================

def random_sleep(lo=None, hi=None):
    lo = lo or MIN_DELAY
    hi = hi or MAX_DELAY
    time.sleep(random.uniform(lo, hi))


def get_months(start_year, end_year):
    """
    按月分割日期范围，确保每个查询结果数 << 100，
    避免因 XHR start 参数无效导致分页失效的问题。
    """
    import calendar
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            da = f'{year}{month:02d}01'
            last_day = calendar.monthrange(year, month)[1]
            # 下月1日作为 before 边界（不含）
            if month == 12:
                db = f'{year + 1}0101'
            else:
                db = f'{year}{month + 1:02d}01'
            months.append((da, db))
    return months


def task_key(country, group, ipc, da, db):
    return f'{country}__{group}__{ipc}__{da}__{db}'


# ============================================================
# XHR 查询
# ============================================================

def build_xhr_url(keywords, country, ipc_code, date_after, date_before,
                  start=0, num=100):
    """
    构建 Google Patents XHR query URL。

    实测：`start` 必须作为**外层**参数传入，不能编码在 url= 内部，否则分页无效。
    正确格式：
      https://patents.google.com/xhr/query?url=[ENCODED_INNER]&exp=&tags=&num=100&start=100
    """
    kw_str = '+OR+'.join(f'"{kw.replace(" ", "+")}"' for kw in keywords)

    # num 必须放在内层（实测：外层 num 只返回10条，内层 num 才能返回100条）
    # start 放外层（内层 start 无效，但外层也无效——pagination 不可用）
    inner_parts = [
        f'q={kw_str}',
        f'country={country}',
        f'ipc={ipc_code}',
        f'before=priority:{date_before}',
        f'after=priority:{date_after}',
        f'num={num}',
    ]
    inner_query = '&'.join(inner_parts)

    return (
        'https://patents.google.com/xhr/query?url='
        + urllib.parse.quote(inner_query, safe='')
        + '&exp=&tags='
    )


def parse_xhr_json(data):
    """
    解析 XHR JSON，返回 (records_list, total_num)。

    XHR patent 对象实际字段（实测）：
      title, snippet, priority_date, filing_date, grant_date,
      publication_date, inventor, assignee, publication_number,
      language, thumbnail, pdf, family_metadata
    注意：ipc 字段为 None；assignee 为单字符串（主申请人）。
    """
    records = []
    results = data.get('results', {})
    total   = int(results.get('total_num_results', 0))

    for cluster in results.get('cluster', []):
        for item in cluster.get('result', []):
            pat = item.get('patent', {})
            pub_num = pat.get('publication_number', '')
            if not pub_num:
                continue

            def _str(v):
                if v is None:
                    return ''
                if isinstance(v, list):
                    return '; '.join(str(x) for x in v if x)
                return str(v).strip()

            # 去除标题中的 HTML 高亮标签
            title_raw = pat.get('title', '') or ''
            title = re.sub(r'<[^>]+>', '', title_raw).strip()

            records.append({
                'id':            pub_num,
                'title':         title,
                'assignee':      _str(pat.get('assignee')),    # 主申请人（字符串）
                'inventor':      _str(pat.get('inventor')),
                'priority_date': _str(pat.get('priority_date')),
                'filing_date':   _str(pat.get('filing_date')),
                'grant_date':    _str(pat.get('grant_date')),
                'pub_date':      _str(pat.get('publication_date')),
                'language':      _str(pat.get('language')),
                'snippet':       (_str(pat.get('snippet')) or '')[:400],
                # IPC 留空，Stage-2 详情页抓取时补全
                'ipc':           '',
                # 标记是否需要详情页补全（默认需要，Stage-2 批量抓取后设为 True）
                'detail_fetched': False,
            })

    return records, total


# 全局连续失败计数（跨任务，用于触发冷却模式）
_consecutive_failures = 0

def xhr_query(session, keywords, country, ipc_code, da, db,
              start=0, num=100, retries=MAX_RETRIES):
    """
    调用一次 XHR 接口，返回 (records, total)；失败返回 (None, 0)。
    含全局冷却机制：连续失败 COOL_THRESHOLD 次后暂停 COOL_WAIT 秒并刷新 session。
    """
    global _consecutive_failures

    # 冷却检查
    if _consecutive_failures >= COOL_THRESHOLD:
        logger.warning(
            f"  连续失败 {_consecutive_failures} 次，进入冷却模式 "
            f"({COOL_WAIT//60} 分钟)..."
        )
        time.sleep(COOL_WAIT)
        _consecutive_failures = 0
        # 刷新 session
        try:
            session.get('https://patents.google.com/', timeout=30)
            time.sleep(random.uniform(3, 6))
        except Exception:
            pass

    url = build_xhr_url(keywords, country, ipc_code, da, db, start, num)
    session.headers.update(HEADERS_XHR)

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=40)

            if resp.status_code == 200:
                data = resp.json()
                records, total = parse_xhr_json(data)
                _consecutive_failures = 0   # 重置失败计数
                return records, total

            elif resp.status_code == 429:
                wait = 120 * attempt
                logger.warning(f"  XHR 429，等待 {wait}s（第{attempt}次）")
                time.sleep(wait)

            elif resp.status_code == 503:
                wait = 60 * attempt
                logger.warning(f"  XHR 503，等待 {wait}s（第{attempt}次）")
                time.sleep(wait)

            elif resp.status_code in (403, 401):
                logger.warning(f"  XHR {resp.status_code}，需要刷新 session")
                _consecutive_failures += 1
                return None, 0

            else:
                logger.warning(f"  XHR HTTP {resp.status_code}，等待 30s")
                time.sleep(30)

        except requests.exceptions.Timeout:
            logger.warning(f"  XHR 超时，等待 30s（第{attempt}次）")
            time.sleep(30)
        except Exception as e:
            logger.warning(f"  XHR 错误: {e}，等待 30s（第{attempt}次）")
            time.sleep(30)

    logger.error(f"  XHR {retries} 次均失败")
    _consecutive_failures += 1
    return None, 0


# ============================================================
# 单个专利详情页（fallback：当 XHR 数据不完整时使用）
# ============================================================

def fetch_patent_detail(patent_id):
    """urllib 抓取单个专利页面，解析 itemprop 数据。"""
    url = f'https://patents.google.com/patent/{patent_id}'
    req = urllib.request.Request(url, headers=HEADERS_URLLIB)

    for attempt in range(1, 4):
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            html = resp.read().decode('utf-8', errors='ignore')
            return _parse_detail_page(html, patent_id)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(60 * attempt)
            else:
                time.sleep(15)
        except Exception:
            time.sleep(15)
    return None


def _parse_detail_page(html, patent_id):
    soup = BeautifulSoup(html, 'lxml')
    r    = {'id': patent_id}

    def _text(tag):
        return tag.get_text(strip=True) if tag else ''

    r['title']       = _text(soup.find(itemprop='title') or
                              soup.find(itemprop='name'))
    r['assignee']    = '; '.join(
        _text(a) for a in soup.find_all(itemprop='assigneeOriginal')
    )
    r['inventor']    = '; '.join(
        _text(i) for i in soup.find_all(itemprop='inventor')
    )
    r['filing_date'] = _text(soup.find(itemprop='filingDate'))
    r['pub_date']    = _text(soup.find(itemprop='publicationDate'))

    # IPC codes
    ipc_tags = (soup.find_all('span', itemprop='Code') or
                soup.find_all(itemprop='ipcCode'))
    r['ipc']         = '; '.join(_text(t) for t in ipc_tags)

    abstract = soup.find(itemprop='abstract')
    r['abstract']    = _text(abstract)[:400] if abstract else ''

    return r


# ============================================================
# 断点续传
# ============================================================

def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'completed': [], 'stats': {'CN': 0, 'US': 0}}


def save_checkpoint(cp):
    with open(CHECKPOINT, 'w', encoding='utf-8') as f:
        json.dump(cp, f, indent=2, ensure_ascii=False)


# ============================================================
# 单任务爬取
# ============================================================

def scrape_task(session, keywords, country, ipc_code, da, db, task_name):
    """
    爬取一个任务的所有专利。
    返回：
      list[dict]  — 成功且有数据
      []          — 成功但无匹配专利（该时段确实没有）
      None        — API 失败（503/429），需要下次重试
    """
    all_records = []
    seen_ids    = set()
    total_shown = None
    api_failed  = False   # 区分"真无数据"与"API失败"

    for page in range(MAX_PAGES):
        start = page * RESULTS_PER_PAGE
        logger.info(f"  [{task_name}] p{page+1} (start={start}) ...")

        records, total = xhr_query(session, keywords, country,
                                   ipc_code, da, db,
                                   start=start, num=RESULTS_PER_PAGE)

        if records is None:
            logger.warning(f"  [{task_name}] XHR 失败，跳过剩余页")
            api_failed = True
            break

        if page == 0:
            total_shown = total
            logger.info(f"  [{task_name}] 总结果数: {total}")

        if not records:
            logger.info(f"  [{task_name}] 第{page+1}页为空，停止翻页")
            break

        new_cnt = 0
        for rec in records:
            rid = rec.get('id', '')
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                all_records.append(rec)
                new_cnt += 1

        logger.info(f"  [{task_name}] p{page+1} +{new_cnt} 新，累计 {len(all_records)}")

        if len(records) < RESULTS_PER_PAGE:
            break  # 已是最后一页

        if total_shown and start + RESULTS_PER_PAGE >= total_shown:
            break  # 已取完

        random_sleep()

    # API失败且没有任何数据 → 返回 None，主循环不标为完成
    if api_failed and not all_records:
        return None
    return all_records


# ============================================================
# 主流程
# ============================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cp        = load_checkpoint()
    completed = set(cp['completed'])
    quarters  = get_months(START_YEAR, END_YEAR)

    # 任务列表：国家 × 关键词组 × IPC × 季度
    tasks = [
        (country, group, ipc, da, db)
        for country in COUNTRIES
        for group   in KEYWORD_GROUPS
        for ipc     in AI_IPC_CODES
        for da, db  in quarters
    ]

    total_tasks = len(tasks)
    done_count  = sum(1 for c, g, i, da, db in tasks
                      if task_key(c, g, i, da, db) in completed)

    logger.info('=' * 60)
    logger.info(f"总任务: {total_tasks}  已完成: {done_count}  剩余: {total_tasks - done_count}")
    logger.info(f"预估数据量: 中美各 ~7.5 万，共计 ~15 万")
    logger.info('=' * 60)

    session      = init_session()
    task_counter = 0   # 用于定期刷新 session

    for idx, (country, group, ipc, da, db) in enumerate(tasks, 1):
        key = task_key(country, group, ipc, da, db)
        if key in completed:
            continue

        # 定期刷新 session 防止 cookie 过期
        if task_counter > 0 and task_counter % SESSION_REFRESH == 0:
            logger.info("刷新 Session（重新获取 cookies）...")
            session = init_session()

        logger.info(f"\n[{idx}/{total_tasks}] {country} | {group} | {ipc} | {da}-{db}")

        records = scrape_task(session, KEYWORD_GROUPS[group],
                              country, ipc, da, db, key)

        if records is None:
            # XHR 接口调用失败（503/429 等）——不标为完成，下次继续重试
            logger.warning(f"  [{key}] API失败，下次重试")
        elif len(records) == 0:
            # 查询成功但无结果（该组合确实无专利）——标为完成避免重复查询
            logger.info(f"  [{key}] 无专利（正常），标为完成")
            completed.add(key)
            cp['completed'] = list(completed)
            save_checkpoint(cp)
        else:
            df = pd.DataFrame(records)
            df['_country']     = country
            df['_group']       = group
            df['_ipc_filter']  = ipc
            df['_date_after']  = da
            df['_date_before'] = db

            out = os.path.join(OUTPUT_DIR, f'{key}.csv')
            df.to_csv(out, index=False, encoding='utf-8-sig')

            cp['stats'][country] = cp['stats'].get(country, 0) + len(df)
            logger.info(f"  已保存 {len(df)} 条 → {key}.csv")
            logger.info(f"  累计 CN:{cp['stats'].get('CN',0)} | US:{cp['stats'].get('US',0)}")

            completed.add(key)
            cp['completed'] = list(completed)
            save_checkpoint(cp)

        task_counter += 1
        random_sleep(TASK_DELAY_MIN, TASK_DELAY_MAX)

    # --------------------------------------------------------
    # 合并所有批次 CSV
    # --------------------------------------------------------
    logger.info('\n' + '=' * 60)
    logger.info("合并所有批次文件...")

    all_files = [
        os.path.join(OUTPUT_DIR, f)
        for f in os.listdir(OUTPUT_DIR)
        if f.endswith('.csv')
    ]

    if not all_files:
        logger.error("没有找到任何数据文件！")
        return

    dfs = []
    for f in all_files:
        try:
            dfs.append(pd.read_csv(f, encoding='utf-8-sig', low_memory=False))
        except Exception as e:
            logger.warning(f"读取 {f} 失败: {e}")

    if not dfs:
        logger.error("所有文件读取失败！")
        return

    combined = pd.concat(dfs, ignore_index=True)

    # 去重（同一专利号可能被不同查询命中）
    id_col = 'id' if 'id' in combined.columns else combined.columns[0]
    before = len(combined)
    combined.drop_duplicates(subset=[id_col], inplace=True)
    after  = len(combined)
    logger.info(f"去重：{before} → {after}（删除 {before - after} 重复）")

    for country in COUNTRIES:
        sub = combined[combined['_country'] == country].copy()
        out = os.path.join(BASE_DIR, f'patents_{country}_final.csv')
        sub.to_csv(out, index=False, encoding='utf-8-sig')
        logger.info(f"{country}: {len(sub)} 条 → patents_{country}_final.csv")

    combined.to_csv(
        os.path.join(BASE_DIR, 'patents_all_final.csv'),
        index=False, encoding='utf-8-sig'
    )

    logger.info('=' * 60)
    logger.info(f"Stage 1 完成！合计 {len(combined)} 条唯一专利（含去重）")
    logger.info(f"  中国 (CN): {len(combined[combined['_country']=='CN'])} 条")
    logger.info(f"  美国 (US): {len(combined[combined['_country']=='US'])} 条")
    logger.info('=' * 60)
    logger.info("提示：运行 stage2_enrich() 以补全完整申请人列表和 IPC 代码")


# ============================================================
# Stage 2：补全详情页（多申请人 + IPC），用于合作网络分析
# ============================================================

DETAIL_CP = os.path.join(BASE_DIR, 'detail_checkpoint.json')

def load_detail_checkpoint():
    if os.path.exists(DETAIL_CP):
        with open(DETAIL_CP, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

def save_detail_checkpoint(done_ids):
    with open(DETAIL_CP, 'w', encoding='utf-8') as f:
        json.dump(list(done_ids), f)


def stage2_enrich(input_csv=None, output_csv=None, batch_save=500):
    """
    Stage 2：对 Stage 1 收集的专利逐条抓取详情页，
    补全字段：
      - assignee_full   完整申请人（含所有联合申请人，分号分隔）
      - assignee_count  申请人数量
      - ipc_full        完整 IPC 分类代码
    用于：申请人合作网络（多申请人边权重）分析。

    默认读取 patents_all_final.csv，写出 patents_all_enriched.csv。
    """
    input_csv  = input_csv  or os.path.join(BASE_DIR, 'patents_all_final.csv')
    output_csv = output_csv or os.path.join(BASE_DIR, 'patents_all_enriched.csv')

    if not os.path.exists(input_csv):
        logger.error(f"Stage 2 输入文件不存在: {input_csv}，请先运行 Stage 1")
        return

    df = pd.read_csv(input_csv, encoding='utf-8-sig', low_memory=False)
    logger.info(f"Stage 2：共 {len(df)} 条专利待补全")

    # 断点续传
    done_ids = load_detail_checkpoint()
    logger.info(f"Stage 2：已完成 {len(done_ids)} 条，剩余 {len(df) - len(done_ids)} 条")

    # 确保目标列存在
    for col in ['assignee_full', 'assignee_count', 'ipc_full']:
        if col not in df.columns:
            df[col] = ''

    pending = df[~df['id'].astype(str).isin(done_ids)].copy()

    for i, (idx, row) in enumerate(pending.iterrows(), 1):
        pid = str(row['id'])
        detail = fetch_patent_detail(pid)

        if detail:
            df.at[idx, 'assignee_full']  = detail.get('assignee', '')
            df.at[idx, 'assignee_count'] = len([
                a for a in detail.get('assignee', '').split(';') if a.strip()
            ])
            df.at[idx, 'ipc_full']       = detail.get('ipc', '')
            done_ids.add(pid)
        else:
            # 详情页获取失败：保留 Stage 1 的 assignee，ipc 留空
            df.at[idx, 'assignee_full']  = row.get('assignee', '')
            df.at[idx, 'assignee_count'] = 1
            done_ids.add(pid)

        if i % 100 == 0:
            logger.info(f"  Stage 2: {i}/{len(pending)} 完成，"
                        f"多申请人比例: "
                        f"{(df['assignee_count'].astype(float) > 1).sum()}/{len(done_ids)}")

        # 批量保存
        if i % batch_save == 0:
            df.to_csv(output_csv, index=False, encoding='utf-8-sig')
            save_detail_checkpoint(done_ids)
            logger.info(f"  Stage 2 已保存进度至 {output_csv}")

        random_sleep(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX)

    # 最终保存
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    save_detail_checkpoint(done_ids)

    multi = (df['assignee_count'].astype(float) > 1).sum()
    logger.info('=' * 60)
    logger.info(f"Stage 2 完成！输出: {output_csv}")
    logger.info(f"  多申请人专利（合作边）: {multi} 条 "
                f"({multi/len(df)*100:.1f}%)")
    logger.info('=' * 60)


# ============================================================
# 入口
# ============================================================

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'stage2':
        # 单独运行 Stage 2：python patent_scraper.py stage2
        stage2_enrich()
    else:
        # 默认运行 Stage 1（XHR 批量爬取）
        main()
