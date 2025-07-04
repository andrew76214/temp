import os, time, concurrent.futures, requests
from bs4 import BeautifulSoup

BASE   = "https://wiki.company.local"
TOKEN  = os.getenv("CONF_PAT")          # 建議放環境變數
HEAD   = {"Authorization": f"Bearer {TOKEN}"}

def list_page_ids(space_key: str, limit=200):
    start = 0
    while True:
        url = f"{BASE}/rest/api/content"
        params = dict(spaceKey=space_key, type="page", start=start,
                      limit=limit, expand="")         # 只拿 meta
        r = requests.get(url, headers=HEAD, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for p in data["results"]:
            yield p["id"]
        if data["_links"].get("next") is None:
            break
        start += limit

def fetch_page(pid: str):
    url = f"{BASE}/rest/api/content/{pid}"
    params = dict(
        expand="body.storage,metadata.labels,ancestors,version,space"
    )
    r = requests.get(url, headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    html = j["body"]["storage"]["value"]
    text = BeautifulSoup(html, "lxml").get_text("\n")
    return {
        "id": pid,
        "title": j["title"],
        "text": text,
        "labels": [l["name"] for l in j["metadata"]["labels"]["results"]],
        "version": j["version"]["number"],
        "updated": j["version"]["when"],
        "ancestors": [a["title"] for a in j["ancestors"]],
        "space": j["space"]["key"],
        "url": f'{BASE}{j["_links"]["webui"]}',
    }

def crawl_space(space_key):
    pids = list(list_page_ids(space_key))
    print(f"[{space_key}] total pages: {len(pids)}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for data in ex.map(fetch_page, pids):
            yield data            # 這裡可直接寫入 DB 或檔案

# ------- example run -------
if __name__ == "__main__":
    for space in ("DEV", "OPS"):
        for doc in crawl_space(space):
            # TODO: clean HTML → Markdown，再寫入向量 DB
            pass
