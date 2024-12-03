import asyncio
import json
import hashlib
from datetime import datetime

"""
实现了去中心化 P2P 系统中每个节点的核心功能，
包括主题管理、消息路由与转发、网络通信以及事件日志记录。
还负责节点之间的协调和分布式操作。
"""
"""
【for final project】
修改peer_node.py以支持以下功能：
1. 动态节点添加和移除。
2. 在运行时，当节点离线时重新路由请求。
3. 节点重新上线时恢复其原本负责的主题。 

关键改动：
1. 使用一个共享的 活跃节点列表（active nodes） 来追踪当前在线的节点
2. 动态更新邻居节点列表
3. 在节点重新上线时，将它原本负责的主题复制回去
"""


class PeerNode:
    # 初始化函数
    def __init__(
        self, peer_id, ip, port, total_peers, active_nodes, replication_factor=2
    ):
        self.peer_id = peer_id  # 节点的二进制标识符
        self.ip = ip  # 节点的 IP 地址
        self.port = port  # 节点的端口号
        self.total_peers = total_peers  # 总节点数
        self.replication_factor = replication_factor  # 主题的副本数量
        self.active_nodes = active_nodes  # 活跃节点列表
        self.neighbors = self.compute_neighbors()  # 动态计算邻居节点
        self.topics = {}  # 存储当前节点负责的主题
        self.replicas = {}
        self.version_vectors = {}
        self.subscribers = {}  # 存储主题的订阅者
        self.replicas = {}  # Replicas for fault tolerance
        self.failed_nodes = []  # Track failed nodes
        self.log = []  # 节点的事件日志
        
        asyncio.create_task(self.detect_node_status())  # 定期检测邻居节点的在线状态

        self.log_event(f"Peer {self.peer_id} initialized at {self.ip}:{self.port}")

    # -------------------------------
    # Logging & Utilities
    # -------------------------------
    def log_event(self, event):
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] {event}"
        self.log.append(log_entry)
        print(log_entry)

    def hash_function(self, topic):
        """
        Computes a target peer ID by flipping one bit in the current peer_id.
        This ensures the replica is created on a different peer.
        """
        hash_val = int(
            hashlib.sha256(topic.encode()).hexdigest(), 16
        )  # 主题名——>对应的目标节点
        responsible_peer = (
            hash_val % self.total_peers
        )  # 使用 SHA-256 对主题名称进行哈希，并根据节点总数取模
        self.log_event(
            f"Computed hash for topic '{topic}' -> responsible peer {responsible_peer}"
        )
        return bin(responsible_peer)[2:].zfill(len(self.peer_id))  # 返回目标节点id

    # 生成当前节点的邻居列表（基于超立方体拓扑）
    # 通过逐个位翻转节点的二进制标识符计算邻居节点
    def compute_neighbors(self):
        """Set up the neighbors for this peer based on hypercube logic."""
        neighbors = []
        for i in range(len(self.peer_id)):
            neighbor = list(self.peer_id)
            neighbor[i] = "1" if neighbor[i] == "0" else "0"  # 翻转第 i 位
            neighbor_id = "".join(neighbor)
            if neighbor_id in self.active_nodes:  # 如果邻居在线，加入列表
                neighbors.append(neighbor_id)
        return neighbors

    def is_neighbor(self, other_peer_id):
        """Logic to determine if another peer is a neighbor in a hypercube."""
        # Check if the two peer ids differ by exactly one bit
        # In a binary string representation, two nodes are neighbors if they differ by exactly one bit
        differing_bits = sum(1 for a, b in zip(self.peer_id, other_peer_id) if a != b)
        return differing_bits == 1

    def get_peer_address(self, peer_id):
        print(f"Resolving address for peer_id: {peer_id}")
        return f"127.0.0.1:{6000 + int(peer_id, 2)}"

    # -------------------------------
    # Network Topology & Communication
    # -------------------------------
    async def announce_new_node(self, new_peer_id, ip, port):
        """Handle a new node joining the network."""
        self.active_peers[new_peer_id] = (ip, port)
        self.log_event(f"New peer {new_peer_id} joined the network at {ip}:{port}")
        await self.rebalance_topics(new_peer_id)

    async def route_request(self, target_peer_id, payload):
        self.log_event(
            f"Routing request to peer {target_peer_id} with payload: {payload}"
        )
        if self.peer_id == target_peer_id:
            await self.handle_command(payload)  # 处理请求本地命令
        else:
            next_hop = self.find_next_hop(target_peer_id)
            if next_hop not in self.active_nodes:
                self.log_event(
                    f"Next hop {next_hop} is not active. Request could not be forwarded."
                )
                return {"status": "failed", "reason": "Next hop not active"}

            target_address = self.get_peer_address(next_hop)
            response = await self.send_message(target_address, payload)
            return response  # 返回最终的处理结果

    # 找到距离目标节点更近的下一个邻居节点
    def find_next_hop(self, target_peer_id):
        """
        Find the next neighbor in the hypercube that is one bit closer to the target.
        """
        if len(self.peer_id) != len(target_peer_id):
            self.log_event(
                f"Error: Mismatched peer ID lengths between {self.peer_id} and {target_peer_id}"
            )
            return None  # Ensure valid inputs
        
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
        # asyncio.create_task(self.run_sync_loop())
        async with server:
            await server.serve_forever()

    """
    【新加】
    """

    # 定期检测邻居节点的在线状态，并更新活跃节点列表。
    async def detect_node_status(self):
        """Periodically check if neighbors are online and update active nodes."""
        while True:
            for neighbor in list(self.neighbors):
                if not await self.ping_node(neighbor):
                    if neighbor not in self.failed_nodes:
                        self.failed_nodes.append(neighbor)
                    if neighbor in self.active_nodes:
                        self.active_nodes.remove(neighbor)
                    self.log_event(f"Neighbor {neighbor} is offline.")
                else:
                    if neighbor not in self.active_nodes:
                        self.active_nodes.append(neighbor)
                        self.log_event(f"Neighbor {neighbor} is back online.")
            self.neighbors = self.compute_neighbors()  # 重新计算基于活跃节点的邻居
            await asyncio.sleep(5)  # 每5秒检查一次

    """
    【新加】
    """

    #  检查指定节点是否在线
    async def ping_node(self, peer_id):
        """Check if a specific node is reachable."""
        target_peer = self.get_peer_address(peer_id)
        try:
            reader, writer = await asyncio.open_connection(
                target_peer.split(":")[0], int(target_peer.split(":")[1])
            )
            writer.write(json.dumps({"command": "ping", "peer_id": peer_id}).encode())
            await writer.drain()
            writer.close()
            return True
        except:
            return False

    """
    【新加】
    """

    #  当节点重新加入时的处理逻辑
    async def handle_node_rejoin(self, rejoined_node):
        """Handle node rejoining by restoring topics that it was responsible for."""
        if rejoined_node not in self.active_nodes:
            self.active_nodes.append(rejoined_node)  # 将节点重新加入活跃列表
        self.neighbors = self.compute_neighbors()  # 更新邻居列表
        await self.replicate_topics(rejoined_node)  # 将该节点负责的主题复制回去

    # -------------------------------
    # Topic Management
    # -------------------------------
    def hash_function(self, topic):
        """
        Compute the peer node responsible for a given topic using the DHT.
        计算 负责该topic的节点
        This will determine the peer ID where the topic should be stored or accessed.
        """
        hash_val = int(
            hashlib.sha256(topic.encode()).hexdigest(), 16
        )  # 主题名——>对应的目标节点
        responsible_peer = (
            hash_val % self.total_peers
        )  # 使用 SHA-256 对主题名称进行哈希，并根据节点总数取模
        self.log_event(
            f"Computed hash for topic '{topic}' -> responsible peer {responsible_peer}"
        )
        return bin(responsible_peer)[2:].zfill(len(self.peer_id))  # 返回目标节点id

    """
    【新加】
    """

    # 将目标节点负责的主题复制给它（适用于节点重新上线）
    async def replicate_topics(self, target_node):
        """Replicate topics back to the rejoined node"""
        for topic_name in list(self.topics.keys()):
            responsible_peer = self.hash_function(topic_name)
            if responsible_peer == target_node:  # 如果主题属于该节点
                await self.route_request(
                    target_node,
                    {
                        "command": "replicate_topic",
                        "topic_name": topic_name,
                        "messages": self.topics[topic_name],
                    },
                )
                self.log_event(
                    f"Replicated topic '{topic_name}' to rejoined node {target_node}"
                )

    # --------------------【API实现】--------------------
    # 创建主题
    # 如果当前节点是目标节点，则直接创建主题；否则，转发请求到目标节点
    async def create_topic(self, topic_name):
        """Create a new topic and replicate it across nearby nodes"""
        responsible_peer = self.hash_function(topic_name)
        print(f"Computed hash for topic '{topic_name}' -> responsible peer {responsible_peer}")
        
        if responsible_peer == self.peer_id:
            if topic_name not in self.topics:
                self.topics[topic_name] = []  # 创建主题消息列表
                self.subscribers[topic_name] = []  # 初始化订阅者列表
                await self.create_replicas(
                    topic_name
                )  # Create replicas for fault tolerance
                self.log_event(f"Created topic: {topic_name} on peer {self.peer_id}")

                await self.create_replicas(topic_name)
            else:
                self.log_event(f"Attempted to create existing topic: {topic_name}")
        else:
            await self.route_request(
                responsible_peer, {"command": "create_topic", "topic_name": topic_name}
            )

    async def create_replicas(self, topic_name):
        """Create replicas for the topic on neighboring nodes."""
        for neighbor in self.neighbors:
            if neighbor not in self.failed_nodes:
                self.replicate_topic(topic_name, self.peer_id)
                self.log_event(
                    f"Node {self.peer_id} created replica of topic {topic_name} on Node {neighbor}"
                )

    def replicate_topic(self, topic_name, source_peer_id):
        """Add a replica of a topic from another node."""
        if topic_name not in self.replicas:
            self.replicas[topic_name] = []
        self.replicas[topic_name].append(source_peer_id)
        self.log_event(
            f"Node {self.peer_id} added replica of topic {topic_name} from Node {source_peer_id}"
        )

    # 删除本地存储的主题
    # 如果当前节点不是目标节点，则将请求转发到目标节点。
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

    # 将消息发布到主题
    # 如果主题存在，存储消息并将其传播给订阅者；
    # 如果当前节点不是目标节点，则转发请求。
    async def publish_message(self, topic_name, message):
        """Publish a message to the topic and update version vector."""
        responsible_peer = self.hash_function(topic_name)
        timestamp = datetime.now().timestamp()

        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                self.topics[topic_name].append(message)
                self.log_event(
                    f"Published message to topic: {topic_name} on peer {self.peer_id}"
                )
                await self.propagate_message(topic_name, message)
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
                    'command': 'publish_message',
                    'topic_name': topic_name,
                    'message': message,
                },
            )

    # 订阅主题；
    # 如果当前节点不是目标节点，则将请求转发到目标节点
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

    # ----------------------【消息的传递与转发】----------------------
    # 将消息转发给所有订阅了该主题的节点。
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
        self.log_event(
            f"Routing request to peer {target_peer_id} with payload: {payload}"
        )
        if self.peer_id == target_peer_id:
            await self.handle_command(payload)  # 处理请求本地命令
        else:
            next_hop = self.find_next_hop(target_peer_id)
            if next_hop not in self.active_nodes:
                self.log_event(
                    f"Next hop {next_hop} is not active. Request could not be forwarded."
                )
                return {"status": "failed", "reason": "Next hop not active"}

            target_address = self.get_peer_address(next_hop)
            response = await self.send_message(target_address, payload)
            return response  # 返回最终的处理结果

    # 找到距离目标节点更近的下一个邻居节点
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

    # ----------------------【网络通信】----------------------
    # 使用异步I/O发送消息到指定的目标节点
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
            await writer.wait_closed()  # 确保连接被关闭
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
        # self.log_event(f"Processing command: {command}")
        if command["command"] == "create_topic":
            await self.create_topic(command["topic_name"])
        elif command["command"] == "delete_topic":
            await self.delete_topic(command["topic_name"])
        elif command["command"] == "publish_message":
            await self.publish_message(command["topic_name"], command["message"])
        elif command["command"] == "subscribe_to_topic":
            await self.subscribe_to_topic(command["topic_name"])
        elif command["command"] == "ping":
            await self.ping_node(command["peer_id"])
        # 【新加】支持新的 replicate_topic 命令
        elif command["command"] == "replicate_topic":
            topic_name = command["topic_name"]
            self.topics[topic_name] = command["messages"]
            # self.log_event(
            #     f"Replicated topic '{topic_name}' on rejoined node {self.peer_id}"
            # )
        else:
            self.log_event(f"Unknown command: {command}")

        return {"status": "success"}

    async def handle_client(self, reader, writer):
        try:
            data = await reader.read(1000)
            
            command = json.loads(data.decode())
            # self.log_event(f"Received client request: {command}")
            await self.handle_command(command)

            # 向客户端发送响应，确认命令已被处理
            response = {"status": "success"}
            writer.write(json.dumps(response).encode())
            await writer.drain()
        except Exception as e:
            print(json.dumps(command))
            self.log_event(f"Error handling client request: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def run_peer(self):
        server = await asyncio.start_server(self.handle_client, self.ip, self.port)
        self.log_event(f"Peer {self.peer_id} started at {self.ip}:{self.port}")
        async with server:
            await server.serve_forever()
