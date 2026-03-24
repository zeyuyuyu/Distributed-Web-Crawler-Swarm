import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional
import aiohttp
import time

@dataclass
class SwarmNode:
    id: str
    endpoint: str
    capacity: int
    last_heartbeat: float
    active_jobs: int

class SwarmManager:
    def __init__(self):
        self.nodes: Dict[str, SwarmNode] = {}
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.results: Dict[str, any] = {}

    async def register_node(self, node_id: str, endpoint: str, capacity: int) -> bool:
        if node_id in self.nodes:
            return False
        
        self.nodes[node_id] = SwarmNode(
            id=node_id,
            endpoint=endpoint,
            capacity=capacity,
            last_heartbeat=time.time(),
            active_jobs=0
        )
        return True

    async def heartbeat(self, node_id: str) -> bool:
        if node_id not in self.nodes:
            return False
        self.nodes[node_id].last_heartbeat = time.time()
        return True

    async def submit_job(self, job_id: str, target_url: str) -> bool:
        await self.job_queue.put({
            'job_id': job_id,
            'target_url': target_url,
            'timestamp': time.time()
        })
        return True

    async def _assign_job(self, job: Dict) -> Optional[str]:
        available_nodes = [
            node for node in self.nodes.values()
            if node.active_jobs < node.capacity
            and (time.time() - node.last_heartbeat) < 30
        ]
        
        if not available_nodes:
            return None

        # Select node with least load
        selected_node = min(available_nodes, key=lambda x: x.active_jobs)
        selected_node.active_jobs += 1
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{selected_node.endpoint}/execute",
                    json=job
                ) as response:
                    if response.status == 200:
                        return selected_node.id
        except:
            selected_node.active_jobs -= 1
            return None

    async def job_scheduler(self):
        while True:
            job = await self.job_queue.get()
            assigned = False
            
            while not assigned:
                node_id = await self._assign_job(job)
                if node_id:
                    assigned = True
                else:
                    await asyncio.sleep(5)  # Retry after delay

    async def collect_result(self, node_id: str, job_id: str, result: any) -> bool:
        if node_id not in self.nodes:
            return False
            
        self.nodes[node_id].active_jobs -= 1
        self.results[job_id] = result
        return True

    async def cleanup_stale_nodes(self):
        while True:
            current_time = time.time()
            stale_nodes = [
                node_id for node_id, node in self.nodes.items()
                if (current_time - node.last_heartbeat) > 60
            ]
            
            for node_id in stale_nodes:
                del self.nodes[node_id]
                
            await asyncio.sleep(30)

    async def start(self):
        asyncio.create_task(self.job_scheduler())
        asyncio.create_task(self.cleanup_stale_nodes())
