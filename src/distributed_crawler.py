import time
import random
from urllib.parse import urlparse
from collections import defaultdict

class DistributedCrawler:
    def __init__(self):
        self.rate_limits = defaultdict(lambda: {
            'last_request': 0,
            'min_interval': 1.0,
            'backoff_factor': 1.0,
            'max_retries': 3
        })
        self.results = []
    
    def adaptive_sleep(self, domain):
        """Implements intelligent rate limiting with exponential backoff"""
        domain_info = self.rate_limits[domain]
        current_time = time.time()
        elapsed = current_time - domain_info['last_request']
        
        # Calculate required wait time
        wait_time = max(0, domain_info['min_interval'] * domain_info['backoff_factor'] - elapsed)
        
        if wait_time > 0:
            time.sleep(wait_time + random.uniform(0.1, 0.5))  # Add jitter
            
        domain_info['last_request'] = time.time()
    
    def handle_response(self, domain, success):
        """Adjusts rate limiting based on server response"""
        if success:
            # Gradually reduce backoff on success
            self.rate_limits[domain]['backoff_factor'] = max(
                1.0,
                self.rate_limits[domain]['backoff_factor'] * 0.8
            )
        else:
            # Increase backoff on failure
            self.rate_limits[domain]['backoff_factor'] *= 2.0
    
    async def crawl(self, url, depth=2):
        """Main crawling method with intelligent rate limiting"""
        domain = urlparse(url).netloc
        
        try:
            # Apply rate limiting
            self.adaptive_sleep(domain)
            
            # Simulate request (replace with actual HTTP request)
            success = random.random() > 0.2  # 80% success rate simulation
            
            # Update rate limiting based on response
            self.handle_response(domain, success)
            
            if success:
                # Process successful response
                self.results.append({
                    'url': url,
                    'depth': depth,
                    'timestamp': time.time()
                })
                
                if depth > 0:
                    # Simulate finding new URLs (replace with actual parsing)
                    new_urls = [f"{url}/page{i}" for i in range(3)]
                    for new_url in new_urls:
                        await self.crawl(new_url, depth - 1)
            
            return success
            
        except Exception as e:
            self.handle_response(domain, False)
            print(f"Error crawling {url}: {str(e)}")
            return False
    
    def get_results(self):
        """Return crawling results"""
        return self.results
    
    def reset(self):
        """Reset crawler state"""
        self.rate_limits.clear()
        self.results.clear()