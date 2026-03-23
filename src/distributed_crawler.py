import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool

class DistributedCrawler:
    def __init__(self, seed_urls, num_workers):
        self.seed_urls = seed_urls
        self.num_workers = num_workers
        self.crawled_urls = set()

    def crawl(self):
        with Pool(processes=self.num_workers) as pool:
            results = pool.map(self.crawl_page, self.seed_urls)
            for url, links in results:
                self.crawled_urls.add(url)
                for link in links:
                    if link not in self.crawled_urls:
                        self.seed_urls.append(link)

    def crawl_page(self, url):
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            links = [link.get('href') for link in soup.find_all('a')]
            return url, links
        except:
            return url, []
