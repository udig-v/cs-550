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
    def __init__(self, peer_id, ip, port, total_peers, active_nodes, replication_factor=2):
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
        self.log = []  # 节点的事件日志
        # self.active_peers = {self.peer_id: (self.ip, self.port)}
        # self.failed_peers = set()  # 存储失败的节点

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
        计算 负责该topic的节点
        This will determine the peer ID where the topic should be stored or accessed.
        """
        hash_val = int(hashlib.sha256(topic.encode()).hexdigest(), 16)  # 主题名——>对应的目标节点
        responsible_peer = hash_val % self.total_peers  # 使用 SHA-256 对主题名称进行哈希，并根据节点总数取模
        self.log_event(f"Computed hash for topic '{topic}' -> responsible peer {responsible_peer}")
        return bin(responsible_peer)[2:].zfill(len(self.peer_id))   # 返回目标节点id


    # 生成当前节点的邻居列表（基于超立方体拓扑）
    # 通过逐个位翻转节点的二进制标识符计算邻居节点
    def compute_neighbors(self):
        # 动态计算邻居节点，基于当前活跃节点列表和超立方体拓扑结构。
        neighbors = []
        for i in range(len(self.peer_id)):
            neighbor = list(self.peer_id)
            neighbor[i] = '1' if neighbor[i] == '0' else '0'  # 翻转第 i 位
            neighbor_id = ''.join(neighbor)
            if neighbor_id in self.active_nodes:  # 如果邻居在线，加入列表
                neighbors.append(neighbor_id)
        return neighbors

    """
    【新加】
    """
    # 定期检测邻居节点的在线状态，并更新活跃节点列表。
    async def detect_node_status(self):
        while True:
            for neighbor in list(self.neighbors):
                if not await self.ping_node(neighbor):
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
        target_peer = self.get_peer_address(peer_id)
        try:
            reader, writer = await asyncio.open_connection(target_peer.split(':')[0], int(target_peer.split(':')[1]))
            writer.write(json.dumps({"command": "ping"}).encode())
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
        self.active_nodes.append(rejoined_node)  # 将节点重新加入活跃列表
        self.neighbors = self.compute_neighbors()  # 更新邻居列表
        await self.replicate_topics(rejoined_node)  # 将该节点负责的主题复制回去

    """
    【新加】
    """
    # 将目标节点负责的主题复制给它（适用于节点重新上线）
    async def replicate_topics(self, target_node):
        for topic_name in list(self.topics.keys()):
            responsible_peer = self.hash_function(topic_name)
            if responsible_peer == target_node:  # 如果主题属于该节点
                await self.route_request(target_node, {
                    'command': 'replicate_topic',
                    'topic_name': topic_name,
                    'messages': self.topics[topic_name]
                })
                self.log_event(f"Replicated topic '{topic_name}' to rejoined node {target_node}")



    # --------------------【API实现】--------------------
    # 创建主题
    # 如果当前节点是目标节点，则直接创建主题；否则，转发请求到目标节点
    async def create_topic(self, topic_name):
        """Create a new topic and place replicas."""
        responsible_peer = self.hash_function(topic_name)
        if responsible_peer == self.peer_id:
            if topic_name not in self.topics:
                self.topics[topic_name] = []  # 创建主题消息列表
                self.subscribers[topic_name] = []  # 初始化订阅者列表
                self.replicas[topic_name] = []  # Initialize empty replica list
                self.log_event(f"Created topic: {topic_name} on peer {self.peer_id}")
                
                await self.create_replicas(topic_name)
            else:
                self.log_event(f"Attempted to create existing topic: {topic_name}")
        else:
            await self.route_request(responsible_peer, {
                'command': 'create_topic', 
                'topic_name': topic_name
            })


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
            await self.route_request(responsible_peer, {
                'command': 'delete_topic',
                'topic_name': topic_name
            })


    # 将消息发布到主题
    # 如果主题存在，存储消息并将其传播给订阅者；
    # 如果当前节点不是目标节点，则转发请求。
    async def publish_message(self, topic_name, message):
        """Publish a message to the topic and update version vector."""
        responsible_peer = self.hash_function(topic_name)
        timestamp = datetime.now().timestamp()
        
        if responsible_peer == self.peer_id:
            if topic_name in self.topics:
                version_vector = self.version_vectors.setdefault(topic_name, {})
                version_vector[self.peer_id] = timestamp
                self.topics[topic_name].append(message, version_vector.copy())
                
                self.log_event(f"Published message {message} to topic: {topic_name} on peer {self.peer_id}")
                await self.propagate_message(topic_name, message)
                await self.replicate_to_replicas(topic_name, message, version_vector)
            else:
                self.log_event(f"Attempted to publish to non-existent topic: {topic_name}")
        else: 
            self.log_event(f"Routing publish request for topic '{topic_name}' to peer {responsible_peer}")
            await self.route_request(responsible_peer, {
                'command': 'publish_message', 
                'topic_name': topic_name, 
                'message': message
            })


    # 订阅主题；
    # 如果当前节点不是目标节点，则将请求转发到目标节点
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
            
    


    # ----------------------【消息的传递与转发】----------------------
    # 将消息转发给所有订阅了该主题的节点。
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


    # 根据目标节点id转发请求。如果目标节点是自己，则直接处理；否则，选择下一个跳点转发。
    # async def route_request(self, target_peer_id, payload):
    #     self.log_event(f"Routing request to peer {target_peer_id} with payload: {payload}")
    #     if self.peer_id == target_peer_id:
    #         await self.handle_command(payload)  # Handle locally
    #     else:
    #         next_hop = self.find_next_hop(target_peer_id)
    #         target_address = self.get_peer_address(next_hop)
    #         await self.send_message(target_address, payload)

    # async def route_request(self, target_peer_id, payload):
    #     self.log_event(f"Routing request to peer {target_peer_id} with payload: {payload}")
    #     if self.peer_id == target_peer_id:
    #         await self.handle_command(payload)  # Handle locally
    #     else:
    #         next_hop = self.find_next_hop(target_peer_id)
    #         if next_hop not in self.active_nodes:
    #             self.log_event(f"Next hop {next_hop} is not active. Request could not be forwarded.")
    #             return
    #         target_address = self.get_peer_address(next_hop)
    #         await self.send_message(target_address, payload)

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

    # 找到距离目标节点更近的下一个邻居节点
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
        self.log_event(f"Processing command: {command}")
        if command['command'] == 'create_topic':
            await self.create_topic(command['topic_name'])
        elif command['command'] == 'delete_topic':
            await self.delete_topic(command['topic_name'])
        elif command['command'] == 'publish_message':
            await self.publish_message(command['topic_name'], command['message'])
        elif command['command'] == 'subscribe_to_topic':
            await self.subscribe_to_topic(command['topic_name'])
        # 【新加】支持新的 replicate_topic 命令
        elif command['command'] == 'replicate_topic':
            topic_name = command['topic_name']
            self.topics[topic_name] = command['messages']
            self.log_event(f"Replicated topic '{topic_name}' on rejoined node {self.peer_id}")
        else:
            self.log_event(f"Unknown command: {command}")

        return {"status" : "success"}

    # async def handle_client(self, reader, writer):
    #     data = await reader.read(100)
    #     command = json.loads(data.decode())
    #     self.log_event(f"Received client request: {command}")
    #     await self.handle_command(command)
    async def handle_client(self, reader, writer):
        try:
            data = await reader.read(100)
            command = json.loads(data.decode())
            self.log_event(f"Received client request: {command}")
            await self.handle_command(command)

            # 向客户端发送响应，确认命令已被处理
            response = {"status": "success"}
            writer.write(json.dumps(response).encode())
            await writer.drain()
        except Exception as e:
            self.log_event(f"Error handling client request: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def run_peer(self):
        server = await asyncio.start_server(self.handle_client, self.ip, self.port)
        self.log_event(f"Peer {self.peer_id} started at {self.ip}:{self.port}")
        async with server:
            await server.serve_forever()
            

