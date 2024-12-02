# benchmark.py
"""
【该文件进行了基准性测试——延迟和吞吐量】
latency：测量  系统在处理单次 API 请求（如创建主题、发布消息、订阅主题、删除主题）时的延迟
Throughput：测量系统在处理大量并发请求（如多次发布消息）时的性能表现
并通过生成延迟、吞吐量的可视化图表，分析系统性能
"""
import asyncio  # 异步
import time
import random  # 随机节点，模拟真实环境中的负载分布
import matplotlib.pyplot as plt
from peer_node import PeerNode  # P2P核心节点的实现

"""
在 benchmark_apis.py 和 test_apis.py 中，添加【模拟节点离线】 和【重新上线】的测试逻辑
"""

# 测试系统4个api的延迟
async def benchmark_create_topic(peer, topic_name):
    start_time = time.time()
    await peer.create_topic(topic_name)  # 调用节点的 create_topic 方法
    end_time = time.time()
    latency = end_time - start_time  # 计算延迟
    return latency

async def benchmark_publish_message(peer, topic_name, message):
    start_time = time.time()
    await peer.publish_message(topic_name, message)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_subscribe_to_topic(peer, topic_name):
    start_time = time.time()
    await peer.subscribe_to_topic(topic_name)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_delete_topic(peer, topic_name):
    start_time = time.time()
    await peer.delete_topic(topic_name)
    end_time = time.time()
    latency = end_time - start_time
    return latency


# 吞吐量
# peers：节点列表，表示所有参与的 Peer Nodes；
# topic_name：测试中使用的主题名称；
# num_requests：要并发执行的请求数量
async def benchmark_throughput(peers, topic_name, num_requests):
    start_time = time.time()
    tasks = []
    for _ in range(num_requests):
        peer = random.choice(peers)  # 随机选择一个节点
        tasks.append(peer.publish_message(topic_name, f"Random message {random.randint(1, 1000)}"))  # 创建任务
    await asyncio.gather(*tasks)  # 并发执行所有任务
    end_time = time.time()
    throughput = num_requests / (end_time - start_time)
    return throughput

# 画图
def create_latency_graph(latencies):
    plt.figure(figsize=(10, 6))
    plt.plot(latencies, marker='o', linestyle='-', color='b')
    plt.title('API Latency Benchmark')
    plt.xlabel('API Call Index')
    plt.ylabel('Latency (seconds)')
    plt.grid(True)
    plt.savefig('graphs/api_latency.svg')
    plt.close()

def create_throughput_graph(throughputs):
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(throughputs)), throughputs, color='g')
    plt.title('API Throughput Benchmark')
    plt.xlabel('Throughput Test Index')
    plt.ylabel('Throughput (requests per second)')
    plt.xticks(range(len(throughputs)))
    plt.grid(axis='y')
    plt.savefig('graphs/api_throughput.svg')
    plt.close()


# 【定义基准测试主流程】
async def run_benchmark():
    total_peers = 8  # 假设有8个节点
    peers = []
    for peer_id in range(total_peers):
        # 每个节点分配一个唯一的二进制标识符和端口号
        peer = PeerNode(bin(peer_id)[2:].zfill(3), '127.0.0.1', 6000 + peer_id, total_peers)
        peers.append(peer)
    
    topic_name = "BenchmarkTopic"

    # Store latencies for each API call
    latencies = []  # 存储每个 API 的延迟值

    print("Benchmarking topic creation latency...")
    latencies.append(await benchmark_create_topic(peers[0], topic_name))
    
    print("Benchmarking message publishing latency...")
    latencies.append(await benchmark_publish_message(peers[0], topic_name, "Sample message"))
    
    print("Benchmarking subscription latency...")
    latencies.append(await benchmark_subscribe_to_topic(peers[0], topic_name))
    
    print("Benchmarking topic deletion latency...")
    latencies.append(await benchmark_delete_topic(peers[0], topic_name))

    # Benchmark throughput (publishing messages)
    print(f"Benchmarking throughput with 100 requests...")
    throughput = await benchmark_throughput(peers, topic_name, 100)
    
    # Create graphs
    create_latency_graph(latencies)
    create_throughput_graph([throughput])  # single throughput value for bar graph

# Run the benchmark
if __name__ == "__main__":
    asyncio.run(run_benchmark())
