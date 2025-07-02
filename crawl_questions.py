#!/usr/bin/env python3
"""
Dump all Confluence Questions (Q + A + topics) to questions_full.json
"""

import requests, json, time
from urllib.parse import urljoin

BASE   = "https://conf.example.com"         # ← 你的網址
TOKEN  = "YOUR_PAT_HERE"                    # ← PAT
PAGE   = 50                                 # 每頁抓 50 筆 (上限 100，視版本而定)
FILTER = "recent"                           # popular / unanswered…

# ---------- 建立已驗證的 session ----------
sess = requests.Session()
sess.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
})

def fetch_json(url, **kwargs):
    r = sess.get(url, **kwargs)
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized - PAT 可能無權或已失效")
    r.raise_for_status()
    return r.json()

def list_questions():
    start = 0
    while True:
        url = (f"{BASE}/rest/questions/1.0/question"
               f"?limit={PAGE}&start={start}&filter={FILTER}")
        batch = fetch_json(url, timeout=30)
        if not batch:                       # 抓到底
            break
        for q in batch:
            yield q
        start += PAGE
        time.sleep(0.2)                     # 禮貌等待

def get_answers(qid):
    all_ans, start = [], 0
    while True:
        url = (f"{BASE}/rest/questions/1.0/question/{qid}/answers"
               f"?limit=100&start={start}")
        ans = fetch_json(url, timeout=30)
        if not ans:
            break
        all_ans.extend(ans)
        start += 100
    return all_ans

def get_topics(qid):
    url = f"{BASE}/rest/questions/1.0/question/{qid}/topics"
    return [t["name"].lower() for t in fetch_json(url, timeout=15)]

def crawl(out="questions_full.json"):
    data = []
    for q in list_questions():
        qid = q["id"]
        record = {
            "id":        qid,
            "url":       urljoin(BASE, f"/questions/{qid}"),
            "title":     q["title"],
            "question":  q["body"],          # 以純文字回傳；含簡易 markup
            "created":   q["creationDate"],
            "keywords":  get_topics(qid),
            "answers":   [
                {
                    "id":     a["id"],
                    "body":   a["body"],
                    "author": a["creator"]["username"],
                    "created":a["creationDate"],
                    "accepted": a.get("accepted", False),
                    "upvotes":  a.get("score", 0)
                } for a in get_answers(qid)
            ]
        }
        data.append(record)
    with open(out, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    print(f"✅  Done – {len(data)} questions → {out}")

if __name__ == "__main__":
    crawl()
