import asyncio
import json
import hashlib
import argparse
from datetime import datetime


class PeerNode:
    def __init__(self, peer_id, ip, port, total_peers, replication_factor=2):
        self.peer_id = peer_id
        self.ip = ip
        self.port = port
        self.total_peers = total_peers
        self.replication_factor = replication_factor
        self.neighbors = self.compute_neighbors()
        self.log = []
        self.subscribers = {}
        self.topics = {}  # {topic_name: [messages]}
        self.replicas = {}  # {topic_name: [replica_peer_ids]}
        self.version_vectors = {}
        self.failed_peers = set()
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
        self.log_event(
            f"Computed hash for topic '{topic}' -> responsible peer {responsible_peer}"
        )
        return bin(responsible_peer)[2:].zfill(
            len(self.peer_id)
        )  # Returns the binary id

    def compute_neighbors(self):
        # Generate neighbors by flipping each bit of the peer ID to create the hypercube topology.
        neighbors = []
        for i in range(len(self.peer_id)):
            neighbor = list(self.peer_id)
            neighbor[i] = "1" if neighbor[i] == "0" else "0"  # Flip the i-th bit
            neighbors.append("".join(neighbor))
        return neighbors

    async def create_topic(self, topic_name):
        """Create a new topic and place replicas."""
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name not in self.topics:
                self.topics[topic_name] = []  # Initialize an empty message list
                self.subscribers[topic_name] = []  # Initialize empty subscriber list
                self.replicas[topic_name] = []  # Initialize empty replica list
                self.log_event(f"Created topic: {topic_name} on peer {self.peer_id}")

                await self.create_replicas(topic_name)
            else:
                self.log_event(f"Attempted to create existing topic: {topic_name}")
        else:
            await self.route_request(
                responsible_peer, {"command": "create_topic", "topic_name": topic_name}
            )

    async def create_replicas(self, topic_name):
        for i in range(self.replication_factor):
            replica_peer_id = self.find_next_hop(self.peer_id)
            if replica_peer_id:
                self.replicas[topic_name].append(replica_peer_id)
                await self.route_request(
                    replica_peer_id,
                    {"command": "replicate_topic", "topic_name": topic_name},
                )
                self.log_event(
                    f"Replicated topic '{topic_name}' to peer {replica_peer_id}"
                )

    async def replicate_topic(self, topic_name):
        """Place replicas of the topic on neighboring nodes."""
        if topic_name not in self.topics:
            self.topics[topic_name] = []  # Initialize an empty message list
            self.subscribers[topic_name] = []  # Initialize empty subscriber list
            self.log_event(f"Replicated topic: {topic_name} on peer {self.peer_id}")

    async def replicate_message(self, topic_name, message, version_vector):
        """Handle incoming replication request and resolve conflicts."""
        local_version_vector = self.version_vectors.setdefault(topic_name, {})
        # Check if the incoming version vector is more recent
        conflict = False
        for peer_id, incoming_ts in version_vector.items():
            local_ts = local_version_vector.get(peer_id, 0)
            if incoming_ts > local_ts:
                local_version_vector[peer_id] = incoming_ts
                conflict = True

        if conflict or topic_name not in self.topics:
            self.topics.setdefault(topic_name, []).append((message, version_vector))
            self.log_event(
                f"Replicated message for topic: {topic_name} on peer {self.peer_id}"
            )

    async def synchronize_replicas(self, topic_name):
        """Periodically synchronize replicas to ensure consistency."""
        version_vector = self.version_vectors.get(topic_name, {})
        for replica_peer_id in self.replicas.get(topic_name, []):
            await self.route_request(
                replica_peer_id,
                {
                    "command": "sync_topic",
                    "topic_name": topic_name,
                    "version_vector": version_vector,
                },
            )

    async def sync_topic(self, topic_name, version_vector):
        """Handle synchronization request and resolve conflicts."""
        local_version_vector = self.version_vectors.get(topic_name, {})
        for peer_id, incoming_ts in version_vector.items():
            local_ts = local_version_vector.get(peer_id, 0)
            if incoming_ts > local_ts:
                self.log_event(
                    f"Syncing topic '{topic_name}' from peer {peer_id} with newer data."
                )
                await self.request_data(peer_id, topic_name)

    async def request_data(self, peer_id, topic_name):
        """Request missing data from a replica peer."""
        await self.route_request(
            peer_id, {"command": "send_missing_data", "topic_name": topic_name}
        )

    async def send_missing_data(self, topic_name):
        """Send the latest data for the topic to the requesting peer."""
        if topic_name in self.topics:
            for message, verion_vector in self.topics[topic_name]:
                await self.replicate_message(topic_name, message, verion_vector)

    async def run_sync_loop(self):
        """Run a periodic synchronization loop."""
        while True:
            for topic in self.topics:
                await self.synchronize_replicas(topic)
            await asyncio.sleep(10)  # Run sync every 10 seconds

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
            await self.route_request(
                responsible_peer, {"command": "delete_topic", "topic_name": topic_name}
            )

    async def publish_message(self, topic_name, message):
        """Publish a message to the topic and update version vector."""
        timestamp = datetime.now().timestamp()
        responsible_peer = self.hash_function(topic_name)

        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                version_vector = self.version_vectors.setdefault(topic_name, {})
                version_vector[self.peer_id] = timestamp
                self.topics[topic_name].append((message, version_vector.copy()))

                self.log_event(
                    f"Published message {message} to topic: {topic_name} on peer {self.peer_id}"
                )
                await self.propagate_message(topic_name, message, version_vector)
                await self.replicate_to_replicas(topic_name, message, version_vector)
            else:
                self.log_event(
                    f"Attempted to publish to non-existent topic: {topic_name}"
                )
        else:
            self.log_event(
                f"Routing publish request for topic '{topic_name}' to peer {responsible_peer}"
            )
            await self.route_request(
                responsible_peer,
                {
                    "command": "publish_message",
                    "topic_name": topic_name,
                    "message": message,
                },
            )

    async def replicate_to_replicas(self, topic_name, message, version_vector):
        """Send updated data to all replicas."""
        for replica_peer_id in self.replicas.get(topic_name, []):
            await self.route_request(
                replica_peer_id,
                {
                    "command": "replicate_message",
                    "topic_name": topic_name,
                    "message": message,
                    "version_vector": version_vector,
                },
            )

    async def subscribe_to_topic(self, topic_name):
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                if self.peer_id not in self.subscribers[topic_name]:
                    self.subscribers[topic_name].append(self.peer_id)
                self.log_event(
                    f"Subscribed to topic: {topic_name} on peer {self.peer_id}"
                )
            else:
                self.log_event(
                    f"Attempted to subscribe to non-existent topic: {topic_name}"
                )
        else:
            self.log_event(
                f"Routing subscribe request for topic '{topic_name}' to peer {responsible_peer}"
            )
            await self.route_request(
                responsible_peer,
                {"command": "subscribe_to_topic", "topic_name": topic_name},
            )

    async def propagate_message(self, topic_name, message):
        """
        Forward the message to all known subscribers of a topic.
        """
        for subscriber_id in self.subscribers.get(topic_name, []):
            if subscriber_id != self.peer_id:
                await self.route_request(
                    subscriber_id,
                    {
                        "command": "forward_message",
                        "topic_name": topic_name,
                        "message": message,
                    },
                )
                self.log_event(
                    f"Forwarded message for topic '{topic_name}' to neighbor {subscriber_id}"
                )

    async def route_request(self, target_peer_id, payload):
        if target_peer_id in self.failed_peers:
            self.log_event(
                f"Peer {target_peer_id} is marked as failed. Rerouting to a replica."
            )
            for replica_peer_id in self.replicas.get(payload.get("topic_name"), []):
                if replica_peer_id not in self.failed_peers:
                    target_peer_id = replica_peer_id
                    break

        next_hop = self.find_next_hop(target_peer_id)
        if next_hop:
            target_address = self.get_peer_address(next_hop)
            await self.send_message(target_address, payload)
        self.log_event(
            f"Routing request to peer {target_peer_id} with payload: {payload}"
        )
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
                self.log_event(
                    f"Determining next hop: peer {self.peer_id} -> {''.join(next_hop)}"
                )
                return "".join(next_hop)

    async def forward_message(self, neighbor_id, topic_name, message):
        target_peer = self.get_peer_address(neighbor_id)
        payload = {
            "command": "forward_message",
            "topic_name": topic_name,
            "message": message,
        }
        await self.send_message(target_peer, payload)

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
        if command["command"] == "create_topic":
            await self.create_topic(command["topic_name"])
        elif command["command"] == "delete_topic":
            await self.delete_topic(command["topic_name"])
        elif command["command"] == "publish_message":
            await self.publish_message(command["topic_name"], command["message"])
        elif command["command"] == "subscribe_to_topic":
            await self.subscribe_to_topic(command["topic_name"])

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        command = json.loads(data.decode())
        self.log_event(f"Received client request: {command}")
        await self.handle_command(command)

    async def run_peer(self):
        """Start the peer and synchronization loop."""
        server = await asyncio.start_server(self.handle_client, self.ip, self.port)
        self.log_event(f"Peer {self.peer_id} started at {self.ip}:{self.port}")
        asyncio.create_task(self.run_sync_loop())
        async with server:
            await server.serve_forever()

