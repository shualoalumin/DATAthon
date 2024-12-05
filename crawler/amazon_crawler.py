import requests
import pandas as pd
from bs4 import BeautifulSoup
import time

data = {
    'title': [],
    'rating': [],
}

with open('missing_rating_titles.txt', 'r') as file:
    titles = file.readlines()

for title in titles:
    title = title.strip()
    print(f'Processing title: {title}')
    
    url = f'https://www.google.com/search?q={requests.utils.quote(title + " site:primevideo.com")}'
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0'
    }
    
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

                response = requests.get(url)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # fbl-maturity-rating 클래스를 가진 span 태그 찾기
                    rating_span = soup.select_one('span.fbl-maturity-rating')
                    if rating_span:
                        rat = rating_span.get_text()
                        print(f'Extracted Rating: {rat}')
                data['title'].append(title) 
                data['rating'].append(rat)
                break
            else:
                print("No Amazon Prime Video links found.")
            time.sleep(0.5)
    else:
        print(f'요청 실패: {response.status_code}')
    time.sleep(0.5)

df = pd.DataFrame(data)
df.to_csv('amazon_prime_rating.csv', index=False, encoding='utf-8')
print("Data has been saved to output.csv")