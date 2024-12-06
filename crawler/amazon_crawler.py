import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random

data = {
    'title': [],
    'rating': [],
}

# 파일에서 제목 로드
with open('missing_rating_titles.txt', 'r') as file:
    titles = file.readlines()

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.1722.64 Safari/537.36 Edg/112.0.1722.64',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.92 Safari/537.36',
]

# 요청 속도 제한 변수
max_requests_per_hour = 100
request_interval = 3600 / max_requests_per_hour  # 요청 간의 초
request_count = 0
start_time = time.time()

for title in titles:
    title = title.strip()
    data['title'].append(title)
    data['rating'].append(None)
    print(f'Processing title: {title}')
    
    url = f'https://www.google.com/search?q={requests.utils.quote(title + " site:primevideo.com")}'
    
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.select('h3')
            rat = None
            
            for article in articles:
                link = article.find_parent('a')['href']
                if 'primevideo.com' in link:
                    print(f'Found Amazon Prime Video link: {link}')
                    url = link
                    
                    # Prime Video 페이지 요청
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        rating_span = soup.select_one('span.fbl-maturity-rating')
                        if rating_span:
                            rat = rating_span.get_text()
                            print(f'Extracted Rating: {rat}')
                        data['rating'][-1] = rat
                    break
                else:
                    print("No Amazon Prime Video links found.")
        
        elif response.status_code == 429:
            print(f"429 오류를 받았습니다.")
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                wait_time = int(retry_after)
                time.sleep(wait_time)
            continue
        
        else:
            print(f'Request failed with status code: {response.status_code}')
        
    except Exception as e:
        print(f'An error occurred: {e}')    

    request_count += 1

    # 요청 속도 제한 로직
    if request_count >= max_requests_per_hour:
        elapsed_time = time.time() - start_time
        if elapsed_time < 3600:  # 한 시간이 지나지 않은 경우
            wait_time = 3600 - elapsed_time  # 남은 시간만큼 대기
            print(f"요청 속도 제한에 도달했습니다, {wait_time:.2f}초 동안 대기합니다...")
            time.sleep(wait_time)
        # 다음 시간 동안 카운트와 시작 시간을 초기화합니다.
        request_count = 0
        start_time = time.time()
    else:
        # 제한 내에서 유지하기 위해 요청 간 대기
        time.sleep(request_interval)

# 데이터를 CSV로 저장
df = pd.DataFrame(data)
df.to_csv('amazon_prime_rating.csv', index=False, encoding='utf-8')
print("데이터가 amazon_prime_rating.csv에 저장되었습니다.")