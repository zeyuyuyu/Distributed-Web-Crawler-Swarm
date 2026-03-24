import asyncio
import random
import json
from typing import List, Dict

class SwarmNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.data: Dict[str, any] = {}
        self.consensus: Dict[str, any] = {}

    async def aggregate_data(self):
        while True:
            # Fetch data from peers
            peer_data = await self.fetch_peer_data()

            # Aggregate data and build consensus
            self.build_consensus(peer_data)

            # Store aggregated data
            self.data = self.consensus
            await asyncio.sleep(60)  # Wait for 1 minute before the next aggregation

    async def fetch_peer_data(self) -> Dict[str, any]:
        peer_data = {}
        for peer in self.peers:
            try:
                peer_data[peer] = await self.fetch_from_peer(peer)
            except Exception as e:
                print(f'Error fetching data from peer {peer}: {e}')
        return peer_data

    async def fetch_from_peer(self, peer: str) -> Dict[str, any]:
        await asyncio.sleep(random.uniform(0.1, 1.0))  # Simulate network latency
        return {'key1': 'value1', 'key2': 'value2'}

    def build_consensus(self, peer_data: Dict[str, Dict[str, any]]):
        self.consensus = {}
        for key in peer_data[self.peers[0]]:
            values = [peer_data[peer][key] for peer in peer_data]
            self.consensus[key] = self.aggregate_value(values)

    def aggregate_value(self, values: List[any]) -> any:
        return sum(values) / len(values)

async def main():
    node1 = SwarmNode('node1', ['node2', 'node3', 'node4'])
    node2 = SwarmNode('node2', ['node1', 'node3', 'node4'])
    node3 = SwarmNode('node3', ['node1', 'node2', 'node4'])
    node4 = SwarmNode('node4', ['node1', 'node2', 'node3'])

    await asyncio.gather(
        node1.aggregate_data(),
        node2.aggregate_data(),
        node3.aggregate_data(),
        node4.aggregate_data()
    )

if __name__ == '__main__':
    asyncio.run(main())