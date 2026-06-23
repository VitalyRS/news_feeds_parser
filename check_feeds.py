import json
import feedparser
import requests
import concurrent.futures

def check_feed(url):
    try:
        response = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code != 200:
            return url, False, f"HTTP {response.status_code}"
        
        feed = feedparser.parse(response.content)
        
        # Если bozo = 1 и нет entries, значит это скорее всего HTML страница или ошибка
        if feed.bozo and not feed.entries:
            return url, False, f"Invalid XML/RSS format (bozo=1)"
        
        return url, True, f"OK ({len(feed.entries)} entries)"
    except Exception as e:
        return url, False, type(e).__name__

def main():
    with open('feeds.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    urls = data.get('rss', []) + data.get('atom', [])
    print(f"🔍 Начинаю проверку {len(urls)} фидов (в 10 потоков)...\n")
    
    alive = []
    dead = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(check_feed, urls)
        
        for url, is_alive, msg in results:
            if is_alive:
                alive.append(url)
                print(f"✅ ЖИВОЙ: {url} | {msg}")
            else:
                dead.append((url, msg))
                print(f"❌ МЕРТВЫЙ: {url} | Ошибка: {msg}")
                
    print(f"\n📊 Итог: {len(alive)} живых, {len(dead)} мертвых.")
    
    # Сохраним списки для удобства, если захотим удалить
    with open('dead_feeds.txt', 'w') as f:
        for url, msg in dead:
            f.write(f"{url}\n")
            
    print("\nМертвые ссылки сохранены в 'dead_feeds.txt'.")

if __name__ == '__main__':
    main()
