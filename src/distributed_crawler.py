import multiprocessing as mp
import requests
from bs4 import BeautifulSoup
import time
import queue

class DistributedCrawler:
    def __init__(self, num_workers, queue_size):
        self.num_workers = num_workers
        self.queue = queue.Queue(maxsize=queue_size)
        self.results = mp.Queue()
        self.worker_processes = []

    def crawl(self, start_urls):
        for url in start_urls:
            self.queue.put(url)

        for _ in range(self.num_workers):
            p = mp.Process(target=self.worker, args=(self.queue, self.results))
            p.start()
            self.worker_processes.append(p)

        while True:
            try:
                result = self.results.get(timeout=1)
                yield result
            except queue.Empty:
                if all(p.exitcode is not None for p in self.worker_processes):
                    break

    def worker(self, task_queue, results_queue):
        while True:
            try:
                url = task_queue.get(timeout=1)
            except queue.Empty:
                return

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                results_queue.put((url, soup.get_text()))
            except:
                pass

if __name__ == '__main__':
    crawler = DistributedCrawler(num_workers=4, queue_size=100)
    for result in crawler.crawl(['https://www.example.com', 'https://www.google.com']):
        print(result)