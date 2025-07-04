"""
confluence_to_jsonl.py
----------------------
1. 驗證所填 space_key 是否存在 / 有權限
2. 批次抓取該 Space 全部 page
3. 逐筆輸出到 *.jsonl（newline-delimited JSON）
"""
import os, sys, json, time, concurrent.futures, requests
from bs4 import BeautifulSoup
from typing import List, Dict

BASE  = "https://wiki.company.local"
PAT   = os.getenv("CONF_PAT")            # export CONF_PAT=PAT_xxx
HEAD  = {"Authorization": f"Bearer {PAT}"}

# ---------- 工具函式 ----------
def list_accessible_spaces(limit=200) -> List[str]:
    """回傳使用 PAT 可讀的所有 Space Key 陣列"""
    spaces, start = [], 0
    while True:
        url = f"{BASE}/rest/api/space"
        params = dict(limit=limit, start=start)
        data  = requests.get(url, headers=HEAD, params=params, timeout=30).json()
        spaces.extend([s["key"] for s in data["results"]])
        if "_links" not in data or "next" not in data["_links"]:
            return spaces
        start += limit

def list_page_ids(space_key: str, limit=200):
    """列出指定 Space 所有 page 的 ID（不含非 page 類型）"""
    start = 0
    while True:
        url = f"{BASE}/rest/api/content"
        params = dict(spaceKey=space_key, type="page",
                      start=start, limit=limit)
        data  = requests.get(url, headers=HEAD, params=params, timeout=30).json()
        for p in data["results"]:
            yield p["id"]
        if "_links" not in data or "next" not in data["_links"]:
            break
        start += limit

def fetch_page(pid: str) -> Dict:
    """抓單頁詳細內容並回傳乾淨 dict；若 4xx/5xx 會 raise"""
    url = f"{BASE}/rest/api/content/{pid}"
    params = {"expand": "body.storage,metadata.labels,ancestors,version,space"}
    r  = requests.get(url, headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    j  = r.json()
    html = j["body"]["storage"]["value"]
    text = BeautifulSoup(html, "lxml").get_text("\n")
    return dict(
        id       = pid,
        title    = j["title"],
        text     = text,
        space    = j["space"]["key"],
        labels   = [l["name"] for l in j["metadata"]["labels"]["results"]],
        ancestors= [a["title"] for a in j["ancestors"]],
        version  = j["version"]["number"],
        updated  = j["version"]["when"],
        url      = f'{BASE}{j["_links"]["webui"]}',
    )

def crawl_space_to_jsonl(space_key: str, out_path: str):
    """主流程：確認 Space 存在 → 抓取 → 寫 jsonl"""
    # 1) 確認 Space Key 合法
    accessible = list_accessible_spaces()
    if space_key not in accessible:
        print(f"[ERROR] Space '{space_key}' 不存在或無權限。可用 Space: {', '.join(accessible)}")
        sys.exit(1)

    # 2) 取得 page ID 清單
    pids = list(list_page_ids(space_key))
    print(f"[{space_key}] total pages: {len(pids)}")

    # 3) 逐頁抓取並寫 jsonl
    with open(out_path, "w", encoding="utf-8") as fout, \
         concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for doc in ex.map(fetch_page, pids):
            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"✅ 已匯出 {len(pids)} 筆 → {out_path}")

# ---------- 執行 ----------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python confluence_to_jsonl.py <SPACE_KEY>")
        sys.exit(0)

    key = sys.argv[1].strip()
    outfile = f"{key}_confluence_export_{int(time.time())}.jsonl"
    crawl_space_to_jsonl(key, outfile)
