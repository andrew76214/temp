#!/usr/bin/env python3
"""
抓取 Confluence Questions > Topics 底下所有 QA
執行範例：
    python crawl_questions.py --base https://confluence.example.com
輸出：confluence_questions.json
"""

import json, re, time, argparse
from urllib.parse import urljoin, urlparse, parse_qs
import requests, browser_cookie3
from bs4 import BeautifulSoup
from tqdm import tqdm

CSS_TOPIC_LINK   = "a.aui-nav-link"                 # Topic 列表頁的 topic 連結
CSS_QUEST_LINK   = "a.question-link"                # Topic 內每題的連結
CSS_PAGINATION   = "a.aui-nav-next"                 # 下一頁按鈕
CSS_QUESTION_TTL = "h1.question-title"
CSS_QUESTION_BDY = "div.question-body"
CSS_ANSWER_BODIES = "div.answer"

def build_session(domain: str) -> requests.Session:
    """沿用 Edge Cookie，建立已登入的 Session"""
    cj = browser_cookie3.edge(domain_name=domain)
    sess = requests.Session()
    sess.cookies.update(cj)
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Edge crawler)"})
    return sess

def get_soup(sess: requests.Session, url: str) -> BeautifulSoup:
    resp = sess.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def list_topics(sess: requests.Session, base_url: str):
    url = urljoin(base_url, "/questions/topics")
    soup = get_soup(sess, url)
    for a in soup.select(CSS_TOPIC_LINK):
        yield a.text.strip(), urljoin(base_url, a["href"])

def paginate(sess: requests.Session, topic_url: str):
    """迭代 topic 下所有分頁"""
    next_url = topic_url
    while next_url:
        soup = get_soup(sess, next_url)
        yield soup
        nxt = soup.select_one(CSS_PAGINATION)
        next_url = urljoin(topic_url, nxt["href"]) if nxt else None

def list_questions(sess: requests.Session, topic_url: str):
    """回傳 (title, url)"""
    for soup in paginate(sess, topic_url):
        for a in soup.select(CSS_QUEST_LINK):
            yield a.text.strip(), urljoin(topic_url, a["href"])

def fetch_qa(sess: requests.Session, q_url: str):
    soup = get_soup(sess, q_url)
    q_title = soup.select_one(CSS_QUESTION_TTL).text.strip()
    q_body  = soup.select_one(CSS_QUESTION_BDY).get_text("\n", strip=True)
    answers = [a.get_text("\n", strip=True) for a in soup.select(CSS_ANSWER_BODIES)]
    return {"url": q_url, "title": q_title, "question": q_body, "answers": answers}

def crawl(base_url: str, out_file: str = "confluence_questions.json"):
    domain = urlparse(base_url).hostname
    sess = build_session(domain)
    data = []

    print("⏳ 取得 Topics 列表…")
    for topic_name, topic_url in list_topics(sess, base_url):
        print(f"🔍 Topic: {topic_name}")
        for q_title, q_url in tqdm(list_questions(sess, topic_url), desc="  抓 Question"):
            try:
                qa = fetch_qa(sess, q_url)
                qa["topic"] = topic_name
                data.append(qa)
            except Exception as e:
                print(f"⚠️  無法抓取 {q_url}: {e}")

            time.sleep(0.5)  # 溫和一點，避免伺服器過載

    with open(out_file, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    print(f"✅ 完成！共 {len(data)} 筆，存檔 {out_file}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Confluence 基底 URL，例如 https://conf.example.com")
    ap.add_argument("--out", default="confluence_questions.json", help="輸出檔名")
    args = ap.parse_args()
    crawl(args.base.rstrip("/"), args.out)
