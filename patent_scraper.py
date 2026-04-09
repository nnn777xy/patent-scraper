"""
中美人工智能领域专利爬取脚本 v7 — Lens.org API 版
=========================================================
数据源：Lens.org（索引数据与 Google Patents 完全相同）
优势：
  - 免费 API Key，无严格封禁
  - 每次最多 1000 条，支持分页（scroll）
  - 返回完整申请人列表（合作网络分析关键字段）
  - 不需要代理/VPN

查询策略：
  国家 × IPC代码 × 年份 = 2 × 5 × 11 = 110 个任务
  每任务分页获取全部结果，预计总量 20-30 万条，去重后 ~15 万

用法：
  1. 去 https://www.lens.org/lens/user/subscriptions#scholar 申请免费 API key
  2. 设置环境变量  LENS_API_KEY=你的key  （或写入下方 LENS_API_KEY 变量）
  3. python patent_scraper.py
"""

import requests
import json
import time
import random
import os
import re
import logging
import pandas as pd
from pathlib import Path

# ============================================================
# 路径配置
# ============================================================
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'patent_data'
CHECKPOINT = BASE_DIR / 'checkpoint.json'
LOG_FILE   = BASE_DIR / 'scraper.log'

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
# Lens.org API 配置
# ============================================================
LENS_API_URL = 'https://api.lens.org/patent/search'
LENS_API_KEY = os.environ.get('LENS_API_KEY', '')   # 优先从环境变量读取

# ============================================================
# 爬取参数
# ============================================================
COUNTRIES       = ['CN', 'US']
START_YEAR      = 2015
END_YEAR        = 2025

# AI 核心 IPC 代码（只取前缀，Lens.org 用 prefix 匹配）
AI_IPC_PREFIXES = ['G06N 3', 'G06N 20', 'G06N 5', 'G06V', 'G10L']

PAGE_SIZE    = 1000    # 每页最多 1000 条（Lens.org 上限）
MAX_RECORDS  = 10000   # 每个任务最多取 10000 条（10 页）
REQUEST_DELAY = 1.2    # 请求间隔（秒），Lens.org 免费版宽松
MAX_RETRIES   = 4


# ============================================================
# 工具函数
# ============================================================

def task_key(country, ipc, year):
    ipc_safe = ipc.replace(' ', '_')
    return f'{country}__{ipc_safe}__{year}'


def load_checkpoint():
    if CHECKPOINT.exists():
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'completed': [], 'stats': {'CN': 0, 'US': 0}}


def save_checkpoint(cp):
    with open(CHECKPOINT, 'w', encoding='utf-8') as f:
        json.dump(cp, f, indent=2, ensure_ascii=False)


# ============================================================
# Lens.org 查询构建
# ============================================================

def build_query(country, ipc_prefix, year, from_=0, size=PAGE_SIZE, scroll_id=None):
    """构建 Lens.org Patent Search API 请求体"""
    if scroll_id:
        # 续页模式
        return {"scroll_id": scroll_id, "include": _fields()}

    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"jurisdiction": country}},
                    {"term": {"year_published": year}},
                    {
                        "bool": {
                            "should": [
                                {"prefix": {"class_ipcr.symbol": ipc_prefix}},
                                {"prefix": {"classifications_ipcr.symbol": ipc_prefix}},
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        },
        "size": size,
        "from": from_,
        "include": _fields(),
        "sort": [{"date_published": "asc"}],
        "scroll": "1m"   # 启用 scroll（有效期 1 分钟）
    }


def _fields():
    return [
        "lens_id",
        "jurisdiction",
        "date_published",
        "year_published",
        "filing_date",
        "title",
        "abstract",
        "applicant",
        "inventor",
        "class_ipcr",
        "classifications_ipcr",
        "publication_number",
        "pub_key",
        "claims_count",
    ]


# ============================================================
# 解析响应
# ============================================================

def parse_response(data, country, ipc_prefix, year):
    """解析 Lens.org API 响应，提取专利记录"""
    records = []

    for pat in data.get('data', []):
        # 专利号：优先 pub_key，其次 publication_number
        pub_num = pat.get('pub_key') or pat.get('publication_number') or pat.get('lens_id', '')

        # 标题（取英文，无英文则取第一个）
        titles = pat.get('title', [])
        title  = ''
        for t in titles:
            if isinstance(t, dict):
                if t.get('lang', '') == 'en':
                    title = t.get('text', '')
                    break
        if not title and titles:
            first = titles[0]
            title = first.get('text', '') if isinstance(first, dict) else str(first)
        # 去 HTML 标签
        title = re.sub(r'<[^>]+>', '', title).strip()

        # 申请人（全部）
        applicants = pat.get('applicant', [])
        assignee_list = []
        for a in applicants:
            if isinstance(a, dict):
                name = a.get('name', '')
            else:
                name = str(a)
            if name:
                assignee_list.append(name.strip())
        assignee = '; '.join(assignee_list)
        assignee_count = len(assignee_list)

        # 发明人
        inventors = pat.get('inventor', [])
        inventor_list = []
        for i in inventors:
            if isinstance(i, dict):
                name = i.get('name', '')
            else:
                name = str(i)
            if name:
                inventor_list.append(name.strip())
        inventor = '; '.join(inventor_list)

        # IPC 代码
        ipc_items = pat.get('class_ipcr', []) or pat.get('classifications_ipcr', [])
        ipc_codes = []
        for item in ipc_items:
            if isinstance(item, dict):
                sym = item.get('symbol', '')
            else:
                sym = str(item)
            if sym:
                ipc_codes.append(sym.strip())
        ipc = '; '.join(ipc_codes)

        # 摘要（取英文）
        abstracts = pat.get('abstract', [])
        abstract  = ''
        for ab in abstracts:
            if isinstance(ab, dict):
                if ab.get('lang', '') == 'en':
                    abstract = ab.get('text', '')[:400]
                    break
        if not abstract and abstracts:
            first = abstracts[0]
            abstract = (first.get('text', '') if isinstance(first, dict) else str(first))[:400]

        records.append({
            'id':              pub_num,
            'lens_id':         pat.get('lens_id', ''),
            'title':           title,
            'assignee':        assignee,
            'assignee_count':  assignee_count,
            'inventor':        inventor,
            'filing_date':     pat.get('filing_date', ''),
            'pub_date':        pat.get('date_published', ''),
            'year':            pat.get('year_published', year),
            'ipc':             ipc,
            'abstract':        abstract,
            '_country':        country,
            '_ipc_filter':     ipc_prefix,
        })

    total     = data.get('total', 0)
    scroll_id = data.get('scroll_id', '')
    return records, total, scroll_id


# ============================================================
# 单次 API 请求
# ============================================================

def api_request(body, retries=MAX_RETRIES):
    """向 Lens.org 发送 POST 请求，返回 JSON 或 None"""
    headers = {
        'Authorization': f'Bearer {LENS_API_KEY}',
        'Content-Type':  'application/json',
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                LENS_API_URL, headers=headers,
                json=body, timeout=60
            )

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 60 * attempt
                logger.warning(f"  429 限速，等待 {wait}s")
                time.sleep(wait)
            elif resp.status_code == 401:
                logger.error("  401 未授权：请检查 LENS_API_KEY")
                return None
            else:
                logger.warning(f"  HTTP {resp.status_code}，等待 30s（第{attempt}次）")
                logger.debug(f"  响应: {resp.text[:200]}")
                time.sleep(30)

        except requests.exceptions.Timeout:
            logger.warning(f"  请求超时，等待 30s（第{attempt}次）")
            time.sleep(30)
        except Exception as e:
            logger.warning(f"  请求错误: {e}，等待 30s（第{attempt}次）")
            time.sleep(30)

    logger.error(f"  {retries} 次均失败")
    return None


# ============================================================
# 单任务爬取（分页）
# ============================================================

def scrape_task(country, ipc_prefix, year, task_name):
    """
    爬取一个任务的全部专利（支持 scroll 分页）。
    返回：list[dict] 或 None（API失败）
    """
    all_records = []
    seen_ids    = set()
    scroll_id   = None
    page        = 0
    total_shown = None

    while len(all_records) < MAX_RECORDS:
        if scroll_id and page > 0:
            body = build_query(country, ipc_prefix, year, scroll_id=scroll_id)
        else:
            body = build_query(country, ipc_prefix, year, from_=0, size=PAGE_SIZE)

        logger.info(f"  [{task_name}] 第{page+1}页...")
        data = api_request(body)

        if data is None:
            if page == 0:
                return None   # 第1页就失败 → API问题，需重试
            else:
                break         # 已有部分数据，保存已有的

        records, total, scroll_id = parse_response(data, country, ipc_prefix, year)

        if page == 0:
            total_shown = total
            logger.info(f"  [{task_name}] 总结果数: {total}")

        if not records:
            break

        new_cnt = 0
        for rec in records:
            rid = rec.get('id') or rec.get('lens_id')
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                all_records.append(rec)
                new_cnt += 1

        logger.info(f"  [{task_name}] p{page+1} +{new_cnt} 新，累计 {len(all_records)}")

        page += 1

        # 已取完或无 scroll_id 则停止
        if len(records) < PAGE_SIZE or not scroll_id:
            break
        if total_shown and len(all_records) >= total_shown:
            break

        time.sleep(REQUEST_DELAY)

    return all_records


# ============================================================
# 主流程
# ============================================================

def main():
    if not LENS_API_KEY:
        logger.error("未设置 LENS_API_KEY！")
        logger.error("请去 https://www.lens.org/lens/user/subscriptions#scholar 申请免费 Key")
        logger.error("然后运行: set LENS_API_KEY=你的key  (Windows)")
        logger.error("或:       export LENS_API_KEY=你的key (Linux/Mac)")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    cp        = load_checkpoint()
    completed = set(cp['completed'])

    tasks = [
        (country, ipc, year)
        for country in COUNTRIES
        for ipc     in AI_IPC_PREFIXES
        for year    in range(START_YEAR, END_YEAR + 1)
    ]

    total_tasks = len(tasks)
    done_count  = sum(1 for c, i, y in tasks if task_key(c, i, y) in completed)

    logger.info('=' * 60)
    logger.info(f"数据源: Lens.org Patent API")
    logger.info(f"总任务: {total_tasks}  已完成: {done_count}  剩余: {total_tasks - done_count}")
    logger.info('=' * 60)

    for idx, (country, ipc, year) in enumerate(tasks, 1):
        key = task_key(country, ipc, year)
        if key in completed:
            continue

        logger.info(f"\n[{idx}/{total_tasks}] {country} | {ipc} | {year}")

        records = scrape_task(country, ipc, year, key)

        if records is None:
            logger.warning(f"  [{key}] API失败，下次重试（不标为完成）")
        elif len(records) == 0:
            logger.info(f"  [{key}] 无匹配专利，标为完成")
            completed.add(key)
            cp['completed'] = list(completed)
            save_checkpoint(cp)
        else:
            df = pd.DataFrame(records)
            out = OUTPUT_DIR / f'{key}.csv'
            df.to_csv(out, index=False, encoding='utf-8-sig')

            cp['stats'][country] = cp['stats'].get(country, 0) + len(df)
            logger.info(f"  已保存 {len(df)} 条 → {key}.csv")
            logger.info(f"  累计 CN:{cp['stats'].get('CN',0)} | US:{cp['stats'].get('US',0)}")

            completed.add(key)
            cp['completed'] = list(completed)
            save_checkpoint(cp)

        time.sleep(REQUEST_DELAY)

    # -------------------------------------------------------
    # 合并所有 CSV
    # -------------------------------------------------------
    logger.info('\n' + '=' * 60)
    logger.info("合并所有批次文件...")

    all_files = list(OUTPUT_DIR.glob('*.csv'))
    if not all_files:
        logger.error("没有数据文件！")
        return

    dfs = []
    for f in all_files:
        try:
            dfs.append(pd.read_csv(f, encoding='utf-8-sig', low_memory=False))
        except Exception as e:
            logger.warning(f"读取 {f.name} 失败: {e}")

    combined = pd.concat(dfs, ignore_index=True)

    # 去重
    before = len(combined)
    combined.drop_duplicates(subset=['id'], inplace=True)
    after  = len(combined)
    logger.info(f"去重：{before} → {after}（删除 {before - after} 条）")

    for country in COUNTRIES:
        sub = combined[combined['_country'] == country]
        out = BASE_DIR / f'patents_{country}_final.csv'
        sub.to_csv(out, index=False, encoding='utf-8-sig')
        logger.info(f"{country}: {len(sub)} 条 → patents_{country}_final.csv")

    combined.to_csv(BASE_DIR / 'patents_all_final.csv', index=False, encoding='utf-8-sig')

    logger.info('=' * 60)
    logger.info(f"完成！共 {len(combined)} 条唯一专利")
    logger.info(f"  CN: {len(combined[combined['_country']=='CN'])} 条")
    logger.info(f"  US: {len(combined[combined['_country']=='US'])} 条")
    logger.info('=' * 60)


if __name__ == '__main__':
    main()
