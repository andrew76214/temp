from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from tqdm import tqdm
import json, time

BASE_URL = "https://conf.example.com"

SEL_TOPIC_LINK   = 'a[href^="/questions/topic"]'
SEL_NEXT_PAGE    = 'a[rel="next"], a.aui-nav-next[href*="page="]'
SEL_QUEST_LINK   = 'a[href^="/questions/"]:not([href*="/topic"])'
SEL_TOPIC_TAGS   = 'ul.question-topics a, a[data-tag]'
SEL_SHOW_MORE    = 'button.show-more, a.show-more'   # 視版本調整

def scroll_to_bottom(page, step=1500, wait=1000):
    """滑到最底並等待新資料載入；回傳 True=有新高度"""
    prev = -1
    while True:
        curr = page.evaluate("() => document.body.scrollHeight")
        if curr == prev:
            break
        prev = curr
        page.evaluate(f"() => window.scrollBy(0, {step})")
        page.wait_for_timeout(wait)

def click_show_more_all(page, selector=SEL_SHOW_MORE):
    """反覆點擊 show-more 類按鈕直到消失"""
    while True:
        try:
            btn = page.locator(selector).first
            btn.wait_for(state="visible", timeout=1000)
            btn.click()
            page.wait_for_timeout(800)
        except PWTimeout:
            break

def parse_question_html(html, q_url):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one("h1.question-title, h1#title-text").get_text(strip=True)
    question = soup.select_one("div.question-body, div.content-body").get_text("\n", strip=True)
    answers = [a.get_text("\n", strip=True) for a in soup.select("div.answer, div.comment-answer")]
    tags = sorted({t.get_text(strip=True).lower() for t in soup.select(SEL_TOPIC_TAGS)})
    return {"url": q_url, "title": title, "question": question,
            "answers": answers, "keywords": tags}

def crawl(output="confluence_QA.json"):
    items = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(storage_state="state.json")  
        # 建議：先手動用 Edge 登入一次 → 把 cookie 匯出成 state.json
        page = ctx.new_page()
        page.goto(f"{BASE_URL}/questions/topics", timeout=60000)

        # ── A. 取得所有 Topic 連結 ────────────────────
        scroll_to_bottom(page)                       # 滾到底把列表拉完
        topic_links = [a.get_attribute("href") for a in page.query_selector_all(SEL_TOPIC_LINK)]

        for t_link in topic_links:
            full_topic = BASE_URL + t_link
            page.goto(full_topic, timeout=60000)
            scroll_to_bottom(page)                   # topic 列表若是 infinite scroll

            q_links = {a.get_attribute("href") for a in page.query_selector_all(SEL_QUEST_LINK)}
            for q_link in tqdm(q_links, desc=f"Topic {t_link.split('/')[-1]}"):
                q_url = BASE_URL + q_link
                page.goto(q_url, timeout=60000)
                click_show_more_all(page)            # B. 點 show more
                qa = parse_question_html(page.content(), q_url)
                items.append(qa)
                time.sleep(0.3)                      # 禮貌等待

        json.dump(items, open(output, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"✅ {len(items)} QA saved → {output}")
        browser.close()

if __name__ == "__main__":
    crawl()
