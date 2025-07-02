import json, time, argparse
from urllib.parse import urljoin, urlparse
import requests, browser_cookie3
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------- 這裡把所有 selector 集中 ----------
CSS_TOPIC_LINK      = 'a[href^="/questions/topic"]'
CSS_TOPIC_NEXT_PAGE = 'a[rel="next"], a.aui-nav-next[href*="page="]'
CSS_QUESTION_LINK   = 'a[href^="/questions/"]:not([href*="/topic"])'

CSS_QUESTION_TITLE  = 'h1.question-title, h1#title-text'
CSS_QUESTION_BODY   = 'div.question-body, div.content-body'
CSS_ANSWER_BLOCK    = 'div.answer, div.comment-answer'

CSS_TOPICS          = (
    'ul.question-topics a.tag, '
    'ul.question-topics a.aui-label, '
    'ul.question-topics a[href^="/questions/topic/"]'
)
# ------------------------------------------------

def build_session(base_url):
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Edge crawler)"})
    sess.cookies.update(browser_cookie3.edge(domain_name=urlparse(base_url).hostname))
    return sess

def get_soup(sess, url):
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def list_topics(sess, base):
    soup = get_soup(sess, urljoin(base, "/questions/topics"))
    for a in soup.select(CSS_TOPIC_LINK):
        yield a.text.strip(), urljoin(base, a["href"])

def paginate(sess, first_url):
    next_url = first_url
    while next_url:
        soup = get_soup(sess, next_url)
        yield soup
        nxt = soup.select_one(CSS_TOPIC_NEXT_PAGE)
        next_url = urljoin(first_url, nxt["href"]) if nxt else None

def list_questions(sess, topic_url):
    for soup in paginate(sess, topic_url):
        for a in soup.select(CSS_QUESTION_LINK):
            yield a.text.strip(), urljoin(topic_url, a["href"])

def fetch_qa(sess, q_url):
    s = get_soup(sess, q_url)
    return {
        "url":       q_url,
        "title":     s.select_one(CSS_QUESTION_TITLE).get_text(strip=True),
        "question":  s.select_one(CSS_QUESTION_BODY).get_text("\n", strip=True),
        "answers":   [a.get_text("\n", strip=True) for a in s.select(CSS_ANSWER_BLOCK)],
        "keywords":  sorted({t.get_text(strip=True).lower() for t in s.select(CSS_TOPICS)})
    }

def crawl(base_url, outfile):
    sess = build_session(base_url)
    data = []
    for topic_name, topic_url in list_topics(sess, base_url):
        for _, q_url in tqdm(list_questions(sess, topic_url), desc=f"Topic {topic_name}"):
            try:
                qa = fetch_qa(sess, q_url)
                qa["topic_page"] = topic_name      # 哪個 topic 列表抓到的
                data.append(qa)
            except Exception as e:
                print("⚠️", e)
            time.sleep(0.3)   # 禮貌等待
    with open(outfile, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    print(f"✅ 共 {len(data)} 筆 → {outfile}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Confluence Base URL")
    ap.add_argument("--out", default="confluence_QA.json")
    args = ap.parse_args()
    crawl(args.base.rstrip("/"), args.out)
