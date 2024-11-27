import asyncio
import json
import hashlib
import argparse
from datetime import datetime

class PeerNode:
    def __init__(self, peer_id, ip, port, total_peers):
        self.peer_id = peer_id
        self.ip = ip
        self.port = port
        self.total_peers = total_peers
        self.neighbors = self.compute_neighbors()
        self.log = []
        self.subscribers = {}
        self.topics = {}  # {topic_name: [messages]}
        self.log_event(f"Peer {self.peer_id} initialized at {self.ip}:{self.port}")

    def log_event(self, event):
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] {event}"
        self.log.append(log_entry)
        print(log_entry)  

    def hash_function(self, topic):
        """
        Compute the peer node responsible for a given topic using the DHT.
        This will determine the peer ID where the topic should be stored or accessed.
        """
        hash_val = int(hashlib.sha256(topic.encode()).hexdigest(), 16)
        responsible_peer = hash_val % self.total_peers
        self.log_event(f"Computed hash for topic '{topic}' -> responsible peer {responsible_peer}")
        return bin(responsible_peer)[2:].zfill(len(self.peer_id))   # Returns the binary id
    
    def compute_neighbors(self):
        # Generate neighbors by flipping each bit of the peer ID to create the hypercube topology.
        neighbors = []
        for i in range(len(self.peer_id)):
            neighbor = list(self.peer_id)
            neighbor[i] = '1' if neighbor[i] == '0' else '0'  # Flip the i-th bit
            neighbors.append(''.join(neighbor))
        return neighbors

    async def create_topic(self, topic_name):
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name not in self.topics:
                self.topics[topic_name] = []  # Initialize an empty message list
                self.subscribers[topic_name] = []  # Initialize empty subscriber list
                self.log_event(f"Created topic: {topic_name} on peer {self.peer_id}")
            else:
                self.log_event(f"Attempted to create existing topic: {topic_name}")
        else:
            await self.route_request(responsible_peer, {
                'command': 'create_topic', 
                'topic_name': topic_name
            })

    async def delete_topic(self, topic_name):
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                del self.topics[topic_name]
                del self.subscribers[topic_name]
                self.log_event(f"Deleted topic: {topic_name} on peer {self.peer_id}")
            else:
                self.log_event(f"Attempted to delete non-existent topic: {topic_name}")
        else: 
            await self.route_request(responsible_peer, {
                'command': 'delete_topic',
                'topic_name': topic_name
            })

    async def publish_message(self, topic_name, message):
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                self.topics[topic_name].append(message)
                self.log_event(f"Published message to topic: {topic_name} on peer {self.peer_id}")
                await self.propagate_message(topic_name, message)
            else:
                self.log_event(f"Attempted to publish to non-existent topic: {topic_name}")
        else: 
            self.log_event(f"Routing publish request for topic '{topic_name}' to peer {responsible_peer}")
            await self.route_request(responsible_peer, {
                'command': 'publish_message', 
                'topic_name': topic_name, 
                'message': message
            })
        
    async def subscribe_to_topic(self, topic_name):
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                if self.peer_id not in self.subscribers[topic_name]:
                    self.subscribers[topic_name].append(self.peer_id)
                self.log_event(f"Subscribed to topic: {topic_name} on peer {self.peer_id}")
            else:
                self.log_event(f"Attempted to subscribe to non-existent topic: {topic_name}")
        else:
            self.log_event(f"Routing subscribe request for topic '{topic_name}' to peer {responsible_peer}")
            await self.route_request(responsible_peer, {
                'command': 'subscribe_to_topic',
                'topic_name': topic_name
            })

    async def propagate_message(self, topic_name, message):
        """
        Forward the message to all known subscribers of a topic.
        """
        for subscriber_id in self.subscribers.get(topic_name, []):
            if subscriber_id != self.peer_id:
                await self.route_request(subscriber_id, {
                    'command': 'forward_message',
                    'topic_name': topic_name,
                    'message': message
                })
                self.log_event(f"Forwarded message for topic '{topic_name}' to neighbor {subscriber_id}")
                
    async def route_request(self, target_peer_id, payload):
        self.log_event(f"Routing request to peer {target_peer_id} with payload: {payload}")
        if self.peer_id == target_peer_id:
            await self.handle_command(payload)  # Handle locally
        else:
            next_hop = self.find_next_hop(target_peer_id)
            target_address = self.get_peer_address(next_hop)
            await self.send_message(target_address, payload)
            
    def find_next_hop(self, target_peer_id):
        """
        Find the next neighbor in the hypercube that is one bit closer to the target.
        """
        for i in range(len(self.peer_id)):
            if self.peer_id[i] != target_peer_id[i]:  # Find the first differing bit
                next_hop = list(self.peer_id)
                next_hop[i] = target_peer_id[i]  # Flip the bit to move closer
                self.log_event(f"Determining next hop: peer {self.peer_id} -> {''.join(next_hop)}")
                return ''.join(next_hop)

    async def forward_message(self, neighbor_id, topic_name, message):
        target_peer = self.get_peer_address(neighbor_id)
        payload = {
            'command': 'forward_message',
            'topic_name': topic_name,
            'message': message
        }
        await self.send_message(target_peer, payload)
        
    # async def forward_request(self, responsible_peer, payload):
    #     """
    #     Forwards the request to the peer responsible for the topic using DHT.
    #     """
    #     target_peer = self.get_peer_address(responsible_peer)
    #     await self.send_message(target_peer, payload)

    def get_peer_address(self, peer_id):
        # Placeholder: Define how to resolve a peer_id to IP:Port
        return f"127.0.0.1:{6000 + int(peer_id, 2)}"

    async def send_message(self, target_peer, message):
        ip, port = target_peer.split(":")
        port = int(port)
        try:
            reader, writer = await asyncio.open_connection(ip, port)
            writer.write(json.dumps(message).encode())
            await writer.drain()
            response = await reader.read(100)
            writer.close()
            self.log_event(f"Sent message to {target_peer}: {message}")
            self.log_event(f"Received response from {target_peer}: {response.decode()}")
            return json.loads(response.decode())
        except Exception as e:
            self.log_event(f"Failed to send message {message} to {target_peer}: {e}")
            return None
        
    async def handle_command(self, command):
        """
        Process the command locally if received at the correct peer.
        """
        self.log_event(f"Processing command: {command}")
        if command['command'] == 'create_topic':
            await self.create_topic(command['topic_name'])
        elif command['command'] == 'delete_topic':
            await self.delete_topic(command['topic_name'])
        elif command['command'] == 'publish_message':
            await self.publish_message(command['topic_name'], command['message'])
        elif command['command'] == 'subscribe_to_topic':
            await self.subscribe_to_topic(command['topic_name'])

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        command = json.loads(data.decode())
        self.log_event(f"Received client request: {command}")
        await self.handle_command(command)

    async def run_peer(self):
        server = await asyncio.start_server(self.handle_client, self.ip, self.port)
        self.log_event(f"Peer {self.peer_id} started at {self.ip}:{self.port}")
        async with server:
            await server.serve_forever()
            

# async def main():
#     total_peers = 8
#     peers = []
#     for peer_id in range(total_peers):
#         binary_id = bin(peer_id)[2:].zfill(3)
#         peer = PeerNode(binary_id, '127.0.0.1', 6000 + peer_id, total_peers)
#         peers.append(peer)
    
#     await asyncio.gather(*(peer.run_peer() for peer in peers))
