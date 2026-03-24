import time
import random
from urllib.parse import urlparse
from collections import defaultdict

class DistributedCrawler:
    def __init__(self, delay=1.0, max_requests_per_domain=10):
        self.delay = delay  # Minimum delay between requests to same domain
        self.max_requests_per_domain = max_requests_per_domain
        self.last_request_time = defaultdict(float)
        self.domain_request_count = defaultdict(int)
        self.active_crawlers = set()

    def register_crawler(self, crawler_id):
        """Register a new crawler in the swarm"""
        self.active_crawlers.add(crawler_id)

    def unregister_crawler(self, crawler_id):
        """Remove a crawler from the swarm"""
        self.active_crawlers.remove(crawler_id)

    def can_crawl_url(self, url):
        """Check if URL can be crawled based on rate limits"""
        domain = urlparse(url).netloc
        current_time = time.time()

        # Check domain request count
        if self.domain_request_count[domain] >= self.max_requests_per_domain:
            return False

        # Check if enough time has passed since last request
        time_since_last = current_time - self.last_request_time[domain]
        return time_since_last >= self.delay

    async def crawl_url(self, url, crawler_id):
        """Crawl a URL with rate limiting and polite behavior"""
        domain = urlparse(url).netloc

        # Wait if needed to respect rate limits
        current_time = time.time()
        time_since_last = current_time - self.last_request_time[domain]
        if time_since_last < self.delay:
            await asyncio.sleep(self.delay - time_since_last)

        # Update tracking information
        self.last_request_time[domain] = time.time()
        self.domain_request_count[domain] += 1

        try:
            # Add small random delay for politeness
            jitter = random.uniform(0.1, 0.5)
            await asyncio.sleep(jitter)

            # Actual crawling logic would go here
            # ...

            return {'url': url, 'success': True, 'crawler_id': crawler_id}

        except Exception as e:
            return {'url': url, 'success': False, 'error': str(e)}
        finally:
            self.domain_request_count[domain] -= 1

    def get_crawler_stats(self):
        """Get statistics about the crawler swarm"""
        return {
            'active_crawlers': len(self.active_crawlers),
            'domains_being_crawled': len(self.domain_request_count),
            'total_requests_in_progress': sum(self.domain_request_count.values())
        }

    def reset_stats(self):
        """Reset all crawling statistics"""
        self.last_request_time.clear()
        self.domain_request_count.clear()
