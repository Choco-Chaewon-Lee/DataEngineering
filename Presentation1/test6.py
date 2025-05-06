import os
import asyncio
import re
import csv
import json
import logging
import time
import requests
from io import BytesIO
from PIL import Image
from transformers import pipeline as hf_pipeline
from bs4 import BeautifulSoup  # HTML 파싱용

# Windows 환경에서 비동기 서브프로세스 문제 해결
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from huggingface_hub import InferenceClient

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_prompt(markdown_content):
    return f"""
TASK: Extract a list of books from the webpage content below. For each book, output a bullet point in the following markdown format:

- Title: <책 제목>
  Price: <가격>
  Rating: <평점>
  Availability: <재고 여부>
  Cover: <책표지 이미지 URL>

Make sure to include only the books found in the main content of the page.

CONTENT:
{markdown_content}
    """

def extract_books_info(cleaned_content):
    pattern = (
        r"[*-]\s*Title:\s*(?P<title>.+?)\n\s*Price:\s*(?P<price>.+?)\n\s*Rating:\s*(?P<rating>.+?)\n"
        r"\s*Availability:\s*(?P<availability>.+?)\n\s*Cover:\s*(?P<cover_url>.+?)(?:\n|$)"
    )
    matches = re.finditer(pattern, cleaned_content, re.DOTALL)
    books = [match.groupdict() for match in matches]
    return books

def save_books_to_csv(books, csv_file_path):
    directory = os.path.dirname(csv_file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    fieldnames = ["title", "price", "rating", "availability", "cover_url", "cover_interpretation"]
    with open(csv_file_path, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(books)
    logging.info(f"CSV 파일로 저장 완료: {csv_file_path}")

def save_books_to_json(books, json_file_path):
    directory = os.path.dirname(json_file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=4)
    logging.info(f"JSON 파일로 저장 완료: {json_file_path}")

def call_llama3(prompt):
    client = InferenceClient(
        provider="nebius",
        api_key="hf_"  # API 토큰
    )
    
    messages = [
        {
            "role": "user",
            "content": prompt
        }
    ]
    
    completion = client.chat.completions.create(
        model="meta-llama/Llama-3.3-70B-Instruct",
        messages=messages,
        max_tokens=500,
    )
    return completion.choices[0].message["content"]

def get_image_caption(image_url):
    try:
        response = requests.get(image_url)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content)).convert("RGB")
    except Exception as e:
        logging.error(f"이미지 다운로드 실패 ({image_url}): {e}")
        return "이미지 다운로드 실패"
    
    # 이미지 캡셔닝 파이프라인
    captioner = hf_pipeline("image-to-text", model="nlpconnect/vit-gpt2-image-captioning")
    result = captioner(image)
    if result and isinstance(result, list):
        return result[0]["generated_text"]
    return "캡션 생성 실패"

def parse_rating_from_html(html_content, book_title):
    # 오류로 인해 gpt에게 받은 코드
    soup = BeautifulSoup(html_content, "html.parser")
    # 각 책은 보통 <article class="product_pod"> 안에 있음
    articles = soup.find_all("article", class_="product_pod")
    for article in articles:
        h3 = article.find("h3")
        if h3:
            a_tag = h3.find("a")
            if a_tag and a_tag.get("title", "").strip() == book_title.strip():
                rating_tag = article.find("p", class_="star-rating")
                if rating_tag:
                    classes = rating_tag.get("class", [])
                    for cls in classes:
                        if cls.lower() != "star-rating":
                            return cls  # 예: "Three"
    return "Not available"

async def main(num_pages=5):
    start_time = time.time()

    # CSV와 JSON 파일 저장 경로
    csv_file_path = r"C:\UnderGraduate\Data Engineering\PR1\pr1_BooksInfo2.csv"
    json_file_path = r"C:\UnderGraduate\Data Engineering\PR1\pr1_BooksInfo2.json"

    # 크롤러 관련 설정 (모든 페이지에 동일하게 적용)
    browser_conf = BrowserConfig(
        browser_type="chromium",
        headless=True,
        viewport_width=1280,
        viewport_height=720,
        text_mode=False,
    )
    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
    )

    all_books = []
    all_html = ""
    for page in range(1, num_pages + 1):
        if page == 1:
            url = "https://books.toscrape.com/"
        else:
            url = f"https://books.toscrape.com/catalogue/page-{page}.html"
        logging.info(f"크롤링 시작 - 페이지 {page}: {url}")
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            result = await crawler.arun(url, config=run_conf)
            if not result.success:
                logging.error("크롤링 오류: " + result.error_message)
                continue
            logging.info(f"페이지 {page} 크롤링 완료")
            markdown_content = result.markdown
            # 만약 raw HTML이 필요하다면 result.html 사용 (크롤러가 제공한다면)
            raw_html = getattr(result, "html", "")
            all_html += raw_html  # 전체 HTML을 누적(평점 파싱에 활용)

            # 프롬프트 생성
            prompt = create_prompt(markdown_content)
            logging.info("프롬프트 생성 완료")

            try:
                cleaned_content = await asyncio.to_thread(call_llama3, prompt)
            except Exception as e:
                logging.error("LLM 호출 중 오류 발생: " + str(e))
                continue

            logging.info("LLM 정제 완료")
            logging.info("LLM 응답:\n" + cleaned_content)

            books = extract_books_info(cleaned_content)
            if not books:
                logging.warning(f"페이지 {page}에서 추출된 책 정보가 없습니다.")
            else:
                # 평점이 "Not available"인 경우 HTML 파싱으로 업데이트
                for book in books:
                    if book.get("rating", "").strip() == "Not available" and raw_html:
                        new_rating = parse_rating_from_html(raw_html, book["title"])
                        book["rating"] = new_rating
                    cover_url = book.get("cover_url", "").strip()
                    if cover_url:
                        caption = get_image_caption(cover_url)
                        book["cover_interpretation"] = caption
                        logging.info(f"'{book['title']}' 책표지 해석: {caption}")
                    else:
                        book["cover_interpretation"] = ""
                    logging.info(f"Title: {book['title']}, Price: {book['price']}, Rating: {book['rating']}, Availability: {book['availability']}, Cover: {book.get('cover_url', '')}")
                all_books.extend(books)

    if all_books:
        save_books_to_csv(all_books, csv_file_path)
        save_books_to_json(all_books, json_file_path)
    else:
        logging.warning("전체 페이지에서 추출된 책 정보가 없습니다.")

    end_time = time.time()
    logging.info(f"총 실행 시간: {end_time - start_time:.2f}초")

if __name__ == "__main__":
    asyncio.run(main(num_pages=2))