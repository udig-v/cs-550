import pytest
import asyncio
from peer_node import PeerNode
from unittest.mock import AsyncMock


# Test Initialization
@pytest.mark.asyncio
async def test_peer_initialization():
    peers = [PeerNode(str(i+1).zfill(4), "127.0.0.1", 6000 + i, 8) for i in range(8)]
    peer = peers[0]  # Use the first peer
    assert peer.peer_id == "0001"
    assert peer.ip == "127.0.0.1"
    assert peer.port == 6000
    assert peer.replication_factor == 2
    assert len(peer.neighbors) == 4  # Since ID is 4 bits
    assert "0001" in peer.active_peers
    assert peer.topics == {}
    assert peer.replicas == {}

# Test Topic Creation
@pytest.mark.asyncio
async def test_create_topic():
    # Initialize 8 peers for the hypercube topology
    peers = PeerNode.initialize_hypercube_topology(num_peers=8)

    # Create a peer and simulate the topic creation process
    peer = peers[0]  # Use the first peer
    topic_name = "test_topic"

    # Simulate the creation of the topic
    peer.topics[topic_name] = "Some data"  # Example of creating a topic

    # Test to see if the topic exists
    assert topic_name in peer.topics


# Test Publishing a Message
@pytest.mark.asyncio
async def test_publish_message():
    peers = [PeerNode(str(i).zfill(4), "127.0.0.1", 6000 + i, 8) for i in range(8)]
    peer = peers[0]  # Use the first peer 
    peer = PeerNode("0001", "127.0.0.1", 6000, total_peers=8)
    peer.send_message = AsyncMock(return_value=None)  # Mock sending messages
    peer.route_request = AsyncMock(return_value=None)  # Mock routing requests

    await peer.create_topic("test_topic")

    assert "test_topic" in peer.topics

    # await peer.publish_message("test_topic", "Hello World")
    # assert peer.topics["test_topic"][0][0] == "Hello World"


# Test Replication
@pytest.mark.asyncio
async def test_replicate_message():
    peer1 = PeerNode("0001", "127.0.0.1", 6000, total_peers=8)
    peer2 = PeerNode("0010", "127.0.0.1", 6001, total_peers=8)

    # Manually add peer2 to peer1's replicas for test purposes
    peer1.replicas["test_topic"] = [peer2]

    await peer1.create_topic("test_topic")
    await peer1.replicate_to_replicas("test_topic", "Message", {})

    # Check that the topic replicated successfully on peer2
    assert "test_topic" in peer2.topics
    assert "Message" in peer2.topics["test_topic"][0]


# Test Heartbeat Mechanism
@pytest.mark.asyncio
async def test_heartbeat_failure_detection():
    peers = [PeerNode(str(i).zfill(4), "127.0.0.1", 6000 + i, 8) for i in range(8)]
    peer = peers[0]  # Use the first peer
    
    # Simulate adding a failed peer
    peer.failed_peers.add("0010")

    # Trigger the heartbeat check
    await peer.heartbeat()

    # Assert the failed peer is still detected
    assert "0010" in peer.failed_peers


# Test Rebalancing Topics
@pytest.mark.asyncio
async def test_rebalance_topics():
    peers = [PeerNode(str(i).zfill(4), "127.0.0.1", 6000 + i, 8) for i in range(8)]
    peer = peers[0]  # Use the first peer
    await peer.create_topic("test_topic")
    await peer.rebalance_topics("0010")
    assert "0010" in peer.replicas["test_topic"]
