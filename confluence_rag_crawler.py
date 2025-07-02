import os
import requests
import time
import json
from tqdm import tqdm

BASE_URL = os.getenv("CONFLUENCE_BASE_URL")   # 例如 https://xxx.atlassian.net/wiki
AUTH = ("", os.getenv("CONFLUENCE_PAT"))
HEADERS = {"Accept": "application/json"}

def fetch_paginated(endpoint, params=None):
    url = f"{BASE_URL}/rest/api/{endpoint}"
    start = 0
    while True:
        p = params.copy() if params else {}
        p.update({"start": start, "limit": 50})
        r = requests.get(url, params=p, auth=AUTH, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        for item in data.get("results", []):
            yield item
        if not data.get("links", {}).get("next"):
            break
        start += data["size"]
        time.sleep(0.05)

def get_spaces():
    return list(fetch_paginated("space", {"type": "global"}))

def get_pages(space_key):
    return fetch_paginated("content", {
        "spaceKey": space_key,
        "type": "page",
        "expand": "body.storage,version,ancestors,history,metadata.labels"
    })

def get_page_children(page_id):
    return fetch_paginated(f"content/{page_id}/child/page", {
        "expand": "body.storage,version,ancestors,history"
    })

def get_attachments(page_id):
    return list(fetch_paginated(f"content/{page_id}/child/attachment", {
        "expand": "version,container"
    }))

def get_comments(page_id):
    return list(fetch_paginated(f"content/{page_id}/child/comment", {
        "expand": "body.storage,version,history"
    }))

def doc_meta(content):
    # 基礎內容結構
    meta = {
        "id": content["id"],
        "type": content["type"],
        "title": content["title"],
        "space_key": content["space"]["key"] if "space" in content else None,
        "parent_id": content["ancestors"][-1]["id"] if content.get("ancestors") else None,
        "version": content.get("version", {}).get("number"),
        "created_by": content.get("history", {}).get("createdBy", {}).get("displayName"),
        "created_at": content.get("history", {}).get("createdDate"),
        "updated_at": content.get("version", {}).get("when"),
        "labels": [l.get("name") for l in content.get("metadata", {}).get("labels", {}).get("results", [])] if "metadata" in content else [],
        "body": content.get("body", {}).get("storage", {}).get("value", ""),
        "attachments": [],
        "comments": [],
        "children": [],
    }
    return meta

def parse_attachment(att):
    return {
        "id": att["id"],
        "file_name": att["title"],
        "media_type": att.get("metadata", {}).get("mediaType", ""),
        "download_link": att["_links"].get("download"),
        "created_at": att.get("version", {}).get("when"),
        "created_by": att.get("version", {}).get("by", {}).get("displayName"),
    }

def parse_comment(c):
    return {
        "id": c["id"],
        "author": c.get("history", {}).get("createdBy", {}).get("displayName"),
        "created_at": c.get("history", {}).get("createdDate"),
        "body": c.get("body", {}).get("storage", {}).get("value", ""),
    }

def parse_child(child):
    return {"id": child["id"], "title": child["title"]}

def build_dataset(output_file="confluence_rag_dataset.jsonl"):
    with open(output_file, "w", encoding="utf-8") as fout:
        for space in get_spaces():
            skey = space["key"]
            print(f"=== Crawling SPACE: {skey} ===")
            for page in tqdm(get_pages(skey), desc=f"Pages in {skey}"):
                doc = doc_meta(page)
                # 附件
                attachments = get_attachments(page["id"])
                doc["attachments"] = [parse_attachment(a) for a in attachments]
                # 評論
                comments = get_comments(page["id"])
                doc["comments"] = [parse_comment(c) for c in comments]
                # 子頁面（僅記錄基本資訊，不遞迴抓內容）
                children = get_page_children(page["id"])
                doc["children"] = [parse_child(child) for child in children]
                fout.write(json.dumps(doc, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    build_dataset()
