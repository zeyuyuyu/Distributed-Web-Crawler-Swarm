import redis
import hashlib
import json
from typing import Set, Dict, List
from datetime import datetime, timedelta

class DistributedCrawler:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.crawl_queue_key = 'crawler:queue'
        self.visited_key = 'crawler:visited'
        self.active_crawlers_key = 'crawler:active'
        self.crawler_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
    
    def register_crawler(self) -> None:
        """Register this crawler instance in the distributed system"""
        self.redis_client.hset(
            self.active_crawlers_key,
            self.crawler_id,
            json.dumps({
                'last_heartbeat': datetime.now().isoformat(),
                'urls_processed': 0
            })
        )
    
    def heartbeat(self) -> None:
        """Update crawler's last active timestamp"""
        self.redis_client.hset(
            self.active_crawlers_key,
            self.crawler_id,
            json.dumps({
                'last_heartbeat': datetime.now().isoformat(),
                'urls_processed': self.urls_processed
            })
        )
    
    def add_urls_to_queue(self, urls: List[str]) -> None:
        """Add new URLs to the distributed crawl queue"""
        self.redis_client.rpush(self.crawl_queue_key, *urls)
    
    def get_next_url(self) -> str:
        """Get next URL to crawl from the distributed queue"""
        return self.redis_client.lpop(self.crawl_queue_key)
    
    def mark_url_visited(self, url: str) -> None:
        """Mark URL as visited in distributed set"""
        self.redis_client.sadd(self.visited_key, url)
    
    def is_url_visited(self, url: str) -> bool:
        """Check if URL was already visited"""
        return self.redis_client.sismember(self.visited_key, url)
    
    def get_active_crawlers(self) -> Dict[str, dict]:
        """Get status of all active crawlers"""
        active_crawlers = {}
        all_crawlers = self.redis_client.hgetall(self.active_crawlers_key)
        
        for crawler_id, data in all_crawlers.items():
            crawler_data = json.loads(data)
            last_heartbeat = datetime.fromisoformat(crawler_data['last_heartbeat'])
            
            # Remove crawlers inactive for more than 5 minutes
            if datetime.now() - last_heartbeat > timedelta(minutes=5):
                self.redis_client.hdel(self.active_crawlers_key, crawler_id)
            else:
                active_crawlers[crawler_id] = crawler_data
        
        return active_crawlers
    
    def get_queue_size(self) -> int:
        """Get number of URLs waiting to be crawled"""
        return self.redis_client.llen(self.crawl_queue_key)
    
    def get_visited_count(self) -> int:
        """Get total number of visited URLs"""
        return self.redis_client.scard(self.visited_key)
    
    def cleanup(self) -> None:
        """Remove this crawler from active crawlers"""
        self.redis_client.hdel(self.active_crawlers_key, self.crawler_id)
