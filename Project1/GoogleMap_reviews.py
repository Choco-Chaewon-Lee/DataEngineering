import sys 
import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# 저장 경로
from pymongo import MongoClient
client = MongoClient("")
db = client["dataEngineering"]
collection = db["GoogleMap_jeju"]


# Chrome WebDriver 설정
options = Options()
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Google Maps 제주도 여행지 검색
url = "https://www.google.co.kr/maps/search/제주도+여행지/data=!3m1!4b1!4m2!2m1!6e1?hl=ko"
driver.get(url)
time.sleep(5)

# 검색 결과 로딩 대기 및 스크롤
scrollable_div = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, '//div[contains(@aria-label, "검색결과") and @role="feed"]'))
)
for _ in range(25):
    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
    time.sleep(2)

# 장소 링크 수집
place_cards = driver.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')
place_links = []
for card in place_cards:
    try:
        link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
        if "/place/" in link:
            place_links.append(link)
    except:
        continue
print(f"\n 총 수집된 장소 링크 개수: {len(place_links)}개")

if len(place_links) < 120:
    print("수집된 장소 링크 120개 미만 --> 프로그램 종료")
    sys.exit()

# 결과 저장 리스트
results = []
idx, valid_place_count = 0, 0
review_total = 0

while valid_place_count < 120 and idx < len(place_links):
    place_url = place_links[idx]
    idx += 1
    try:
        driver.get(place_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.DUwDvf.lfPIob'))
        )
        time.sleep(2)

        # 장소 이름
        try:
            name = driver.find_element(By.CSS_SELECTOR, 'h1.DUwDvf.lfPIob').text.strip()
        except:
            name = ""


        # 리뷰 탭 클릭
        try:
            review_tab = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//button[contains(@aria-label, "리뷰")]'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", review_tab)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", review_tab)
            time.sleep(3)
        except:
            continue

        # 리뷰 스크롤
        try:
            review_container = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf'))
            )
            last_height = 0
            for _ in range(100): # 리뷰 수 조정 key point
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", review_container)
                time.sleep(1.5)
                new_height = driver.execute_script("return arguments[0].scrollHeight", review_container)
                if new_height == last_height:
                    break
                last_height = new_height
        except:
            continue

        # 리뷰 수집
        reviews_data = []
        review_elements = driver.find_elements(By.CLASS_NAME, 'jJc9Ad')
        for element in review_elements:
            try:
                # 전체 리뷰 보기 클릭
                try:
                    more_btn = element.find_element(By.CLASS_NAME, 'w8nwRe')
                    driver.execute_script("arguments[0].click();", more_btn)
                except:
                    pass

                review_text = element.find_element(By.XPATH, './/span[@class="wiI7pd"]').text.strip()
                star_elements = element.find_elements(By.XPATH, './/span[@class="hCCjke google-symbols NhBTye elGi1d"]')
                review_rating = len(star_elements) if star_elements else "No rating"
                reviewer_name = element.find_element(By.XPATH, './/div[contains(@class, "d4r55 ")]').text.strip()
                reviews_data.append({
                    "reviewer": reviewer_name,
                    "rating": review_rating,
                    "content": review_text
                })
                review_total += 1
            except:
                continue

        results.append({
            "tour_name": name,
            "reviews": reviews_data
        })

        valid_place_count += 1
        print(f"{idx+1}. {name} - 리뷰 {len(reviews_data)}개 수집 완료")

    except:
        continue


# 리뷰 저장
for place in results:
    tour_name = place["tour_name"]
    reviews = place["reviews"]

    if not tour_name or not reviews:
        continue

    existing_doc = collection.find_one({"tour_name": tour_name})

    if existing_doc:
        # 기존 문서에 리뷰 추가
        collection.update_one(
            {"tour_name": tour_name},
            {"$push": {"reviews": {"$each": reviews}}}
        )
        print(f"'{tour_name}'에 리뷰 {len(reviews)}개 추가 완료.")
    else:
        # 문서가 없으면 새로 생성
        new_doc = {
            "tour_name": tour_name, 
            "avg_rating": None,
            "address": None,
            "latitude": None,
            "longitude": None,
            "review_count": None,
            "reviews": reviews
        }
        collection.insert_one(new_doc)
        print(f"'{tour_name}' 새 문서 생성 및 리뷰 {len(reviews)}개 추가 완료.")
