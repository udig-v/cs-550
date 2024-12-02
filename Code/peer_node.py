import asyncio
import json
import hashlib
from datetime import datetime


class PeerNode:
    def __init__(self, peer_id, ip, port, total_peers, peers=None, replication_factor=2):
        self.peer_id = peer_id
        self.ip = ip
        self.port = port
        self.total_peers = total_peers
        self.replication_factor = replication_factor
        self.peers = peers if peers is not None else []  # Use the passed peers or empty list
        self.neighbors = self.compute_neighbors(self.peers)
        self.log = []
        self.subscribers = {}
        self.topics = {}  # {topic_name: [messages]}
        self.replicas = {}  # {topic_name: [replica_peer_ids]}
        self.version_vectors = {}
        self.failed_peers = set()
        self.active_peers = {self.peer_id: (self.ip, self.port)}

        # Locks for concurrency control
        self.peer_lock = asyncio.Lock()
        self.topic_lock = asyncio.Lock()
        self.version_vector_lock = asyncio.Lock()

        self.log_event(f"Peer {self.peer_id} initialized at {self.ip}:{self.port}")

    # -------------------------------
    # Logging
    # -------------------------------
    def log_event(self, event):
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] {event}"
        self.log.append(log_entry)
        print(log_entry)

    # -------------------------------
    # Network Topology & Communication
    # -------------------------------
    def compute_neighbors(self, peers):
        # # Generate neighbors by flipping each bit of the peer ID to create the hypercube topology.
        # neighbors = []
        # for i in range(len(self.peer_id)):
        #     neighbor = list(self.peer_id)
        #     neighbor[i] = "1" if neighbor[i] == "0" else "0"  # Flip the i-th bit
        #     neighbors.append("".join(neighbor))
        # return neighbors
        """Set up the neighbors for this peer based on hypercube logic."""
        neighbors = []
        # Hypercube logic: Each peer connects to others by flipping one bit at a time in the binary representation of the peer id
        for peer in peers:
            if peer.peer_id != self.peer_id and self.is_neighbor(peer.peer_id):
                neighbors.append(peer)
        self.neighbors = neighbors
        return neighbors
        
    def is_neighbor(self, other_peer_id):
        """Logic to determine if another peer is a neighbor in a hypercube."""
        # Check if the two peer ids differ by exactly one bit
        # In a binary string representation, two nodes are neighbors if they differ by exactly one bit
        differing_bits = sum(1 for a, b in zip(self.peer_id, other_peer_id) if a != b)
        return differing_bits == 1

    def get_peer_address(self, peer_id):
        # Placeholder: Define how to resolve a peer_id to IP:Port
        return f"127.0.0.1:{6000 + int(peer_id, 2)}"

    @classmethod
    def initialize_hypercube_topology(cls, num_peers):
        """Initialize a full hypercube topology with the specified number of peers."""
        peers = []
        for i in range(num_peers):
            peer_id = f"{i:04x}"
            peer = cls(peer_id, "127.0.0.1", 6000 + i, num_peers)
            peers.append(peer)

        # Set up neighbors for each peer based on hypercube topology
        for peer in peers:
            peer.compute_neighbors(peers)

        return peers

    async def heartbeat(self):
        """Send heartbeat messages to detect failures."""
        while True:
            for peer_id, (peer_ip, peer_port) in self.active_peers.items():
                if peer_id == self.peer_id or peer_id in self.failed_peers:
                    continue
                try:
                    reader, writer = await asyncio.open_connection(peer_ip, peer_port)
                    writer.write(b"HEARTBEAT\n")
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    self.log_event(f"Detected failure of peer {peer_id}")
                    self.failed_peers.add(peer_id)
                    await self.handle_peer_failure(peer_id)
            await asyncio.sleep(5)  # Send heartbeat every 5 seconds

    async def handle_peer_failure(self, failed_peer_id):
        """Reassign topics from the failed peer to active replicas."""
        for topic, replicas in list(self.replicas.items()):
            if failed_peer_id in replicas:
                replicas.remove(failed_peer_id)
                if len(replicas) < self.replication_factor:
                    await self.reassign_topic(topic)

        self.log_event(f"Reassigned topics from failed peer {failed_peer_id}")

    async def announce_new_node(self, new_peer_id, ip, port):
        """Handle a new node joining the network."""
        self.active_peers[new_peer_id] = (ip, port)
        self.log_event(f"New peer {new_peer_id} joined the network at {ip}:{port}")
        await self.rebalance_topics(new_peer_id)

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

    async def run_peer(self):
        """Start the peer, heartbeat, and handle node joins/failures. and synchronization loop."""
        server = await asyncio.start_server(self.handle_client, self.ip, self.port)
        self.log_event(f"Peer {self.peer_id} started at {self.ip}:{self.port}")
        asyncio.create_task(self.run_sync_loop())
        asyncio.create_task(self.heartbeat())
        async with server:
            await server.serve_forever()

    # -------------------------------
    # Topic Management
    # -------------------------------
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

    async def send_topic_data(self, peer_id, topic_name):
        """Send topic data to the newly assigned peer."""
        if topic_name in self.topics:
            for message, version_vector in self.topics[topic_name]:
                await self.route_request(
                    peer_id,
                    {
                        "command": "replicate_message",
                        "topic_name": topic_name,
                        "message": message,
                        "version_vector": version_vector,
                    },
                )

    async def reassign_topic(self, topic_name):
        """Reassign topic responsibility to another peer."""
        for peer_id in self.active_peers:
            if peer_id not in self.failed_peers:
                self.replicas.setdefault(topic_name, []).append(peer_id)
                self.log_event(
                    f"Reassigned topic '{topic_name}' to peer {peer_id} for replication."
                )
                break

    async def rebalance_topics(self, new_peer_id):
        """Rebalance topics when a new node joins."""
        for topic, replicas in list(self.replicas.items()):
            if len(replicas) < self.replication_factor:
                replicas.append(new_peer_id)
                self.log_event(
                    f"Added new peer {new_peer_id} as a replica for topic '{topic}'."
                )
                await self.send_topic_data(new_peer_id, topic)

    # -------------------------------
    # Replication & Synchronization
    # -------------------------------
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
        async with self.version_vector_lock:
            version_vector = self.version_vectors.get(topic_name, {})
        tasks = [
            self.route_request(
                replica_peer_id,
                {
                    "command": "sync_topic",
                    "topic_name": topic_name,
                    "version_vector": version_vector,
                },
            )
            for replica_peer_id in self.replicas.get(topic_name, [])
        ]
        if tasks:
            await asyncio.gather(*tasks)

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

    async def run_sync_loop(self):
        """Run a periodic synchronization loop."""
        while True:
            for topic in self.topics:
                await self.synchronize_replicas(topic)
            await asyncio.sleep(10)  # Run sync every 10 seconds

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

    async def send_missing_data(self, topic_name):
        """Send the latest data for the topic to the requesting peer."""
        if topic_name in self.topics:
            for message, version_vector in self.topics[topic_name]:
                await self.replicate_message(topic_name, message, version_vector)

    async def send_message(self, target_peer, message):
        """Generic function to send messages to other peers."""
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

    async def request_data(self, peer_id, topic_name):
        """Request missing data from a replica peer."""
        await self.route_request(
            peer_id, {"command": "send_missing_data", "topic_name": topic_name}
        )

    # -------------------------------
    # Messaging & Command Handling
    # -------------------------------
    async def propagate_message(self, topic_name, message, version_vector):
        """
        Forward the message to all known subscribers of a topic.
        """
        async with self.topic_lock:
            tasks = [
                self.route_request(
                    subscriber_id,
                    {
                        "command": "forward_message",
                        "topic_name": topic_name,
                        "message": message,
                    },
                )
                for subscriber_id in self.subscribers.get(topic_name, [])
                if subscriber_id != self.peer_id
            ]
        if tasks:
            await asyncio.gather(*tasks)

        for subscriber_id in self.subscribers.get(topic_name, []):
            if subscriber_id != self.peer_id:
                self.log_event(
                    f"Forwarded message for topic '{topic_name}' to neighbor {subscriber_id}"
                )

    async def handle_command(self, command_payload):
        """
        Process the command locally if received at the correct peer.
        """
        self.log_event(f"Processing command: {command}")
        command = command_payload.get("command")
        if command == "publish_message":
            await self.publish_message(
                command_payload["topic_name"], command_payload["message"]
            )
        elif command == "subscribe_to_topic":
            await self.subscribe_to_topic(command_payload["topic_name"])
        elif command == "create_topic":
            await self.create_topic(command_payload["topic_name"])
        elif command == "delete_topic":
            await self.delete_topic(command_payload["topic_name"])
        elif command == "replicate_message":
            await self.replicate_message(
                command_payload["topic_name"],
                command_payload["message"],
                command_payload["version_vector"],
            )
        elif command == "sync_topic":
            await self.sync_topic(
                command_payload["topic_name"], command_payload["version_vector"]
            )
        elif command == "forward_message":
            await self.propagate_message(
                command_payload["topic_name"],
                command_payload["message"],
                command_payload.get("version_vector", {}),
            )
        else:
            self.log_event(f"Unknown command: {command}")

    async def handle_client(self, reader, writer):
        """Handle incoming connections and messages."""
        data = await reader.read(100)
        message = data.decode()
        if message.startswith("HEARTBEAT"):
            self.log_event(f"Heartbeat received from peer.")
        else:
            command = json.loads(message)
            self.log_event(f"Received client request: {command}")
        await self.handle_command(command)

    # -------------------------------
    # Version Control & Cleanup
    # -------------------------------
    async def version_vector_cleanup(self):
        """Clean up outdated entries in the version vector for the given topic."""
        while True:
            async with self.version_vector_lock:
                now = datetime.now().timestamp()
                for topic, vector in list(self.version_vectors.items()):
                    self.version_vectors[topic] = {
                        peer_id: ts
                        for peer_id, ts in vector.items()
                        if now - ts < 3600  # Keep entries less than an hour old
                    }
            await asyncio.sleep(3600)  # Cleanup every hour
