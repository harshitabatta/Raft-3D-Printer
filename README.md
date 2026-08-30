# Raft-3D-Printer

A distributed 3D-printer management prototype built to demonstrate **Raft consensus**, leader election, heartbeat-based failure detection, and multi-node communication using Python, Flask, Docker, and Firebase Realtime Database.

> **Project status:** Working prototype. The current implementation has been validated locally as an 8-node Docker cluster with successful leader election and heartbeat communication. Application-level printer/filament/print-job functionality and deeper Raft correctness improvements are planned for subsequent iterations.

## Overview

Distributed systems need to remain available even when individual nodes fail. This project explores that problem through a simulated 3D-printer cluster in which multiple nodes communicate over HTTP and use a Raft-inspired consensus mechanism to elect a leader and maintain cluster state.

The current prototype runs **8 Raft nodes** as Docker containers on a shared Docker network. Each node runs a Flask server and maintains its own Raft state. Firebase Realtime Database is used for persistence in the existing implementation.

## Architecture

```text
                         ┌──────────────────────┐
                         │  Firebase Realtime    │
                         │       Database        │
                         └──────────┬───────────┘
                                    │
                         persistent node state
                                    │
              ┌─────────────────────┴─────────────────────┐
              │              Docker Network               │
              │                                           │
       ┌──────▼──────┐  heartbeat/votes  ┌───────────────▼──────┐
       │   Node 1    │◄─────────────────►│       Node 5         │
       │  Follower   │                   │       Leader          │
       └─────────────┘                   └───────────────────────┘
              │                                      │
              │                                      │
       ┌──────▼──────┐                     ┌──────────▼─────────┐
       │   Node 2    │        ...          │      Node 8        │
       │  Follower   │                     │     Follower       │
       └─────────────┘                     └─────────────────────┘

                     8-node Raft cluster
```

## Current Features

- **8-node distributed cluster** orchestrated with Docker Compose
- **Leader election** using randomized election timeouts
- **Heartbeat communication** between the leader and followers
- **Term and voting state** maintained by each node
- **HTTP-based node communication** using Flask and Requests
- **Cluster status endpoint** for inspecting node state
- **Firebase Realtime Database integration** for persistence
- **Containerized execution** so the cluster can be reproduced locally

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Raft node implementation |
| Flask | HTTP API and inter-node communication |
| Requests | Node-to-node HTTP requests |
| Docker | Containerization |
| Docker Compose | 8-node cluster orchestration |
| Firebase Realtime Database | Persistent state |

## Project Structure

```text
Raft-3D-Printer/
├── raft_node.py            # Main Raft node implementation
├── raft_node_trial.py      # Earlier/trial implementation
├── Dockerfile              # Container image definition
├── docker-compose.yml      # 8-node cluster configuration
├── .gitignore              # Local/secrets exclusions
├── credentials.json        # Local Firebase credentials (NOT committed)
└── logs/                   # Runtime logs (NOT committed)
```

## Prerequisites

- Docker Desktop
- Docker Compose v2+
- A Firebase Realtime Database project configured for the node URLs used by `docker-compose.yml`
- Firebase service-account credentials stored locally as `credentials.json`

**Never commit `credentials.json` or other private keys to Git.**

## Run Locally

From the project root:

### 1. Build the images

```powershell
docker compose build
```

### 2. Start the 8-node cluster

```powershell
docker compose up -d
```

### 3. Verify all nodes are running

```powershell
docker compose ps
```

You should see eight services:

```text
raft_node_1
raft_node_2
raft_node_3
raft_node_4
raft_node_5
raft_node_6
raft_node_7
raft_node_8
```

### 4. Inspect cluster logs

```powershell
docker compose logs --tail=100
```

The nodes should elect a leader and followers should continue receiving heartbeats.

### 5. Stop the cluster

```powershell
docker compose down
```

## Inspect Node State

The current Flask application exposes a `/status` endpoint.

Because the original Compose configuration keeps the node ports internal to the Docker network, the endpoint can be queried from another container:

```powershell
docker exec raft_node_1 python -c "import requests; print(requests.get('http://raft_node_1:5000/status').text)"
```

Example response:

```json
{
  "leader": 5,
  "node_id": 1,
  "state": "follower",
  "term": 1,
  "voted_for": null
}
```

A leader node reports itself as the leader, for example:

```json
{
  "leader": 5,
  "node_id": 5,
  "state": "leader",
  "term": 1,
  "voted_for": 5
}
```

## Failure-Recovery Demonstration

One of the intended demonstrations is leader failure and re-election.

1. Start the cluster.
2. Identify the current leader from `/status` or the logs.
3. Stop that container, for example:

```powershell
docker stop raft_node_5
```

4. Observe the remaining nodes' logs.
5. A follower should eventually begin an election and the cluster should attempt to elect a new leader.

This provides a practical demonstration of why leader election and failure detection are important in distributed systems.

## Engineering Notes

This repository currently represents a **working educational/prototype implementation**, not a production-grade Raft implementation. The next engineering iterations should focus on:

- rigorous Raft term and voting semantics
- persistent Raft state and log replication
- commit/index management
- deterministic integration tests for elections and failures
- clean separation between consensus logic and application logic
- secure injection of Firebase credentials rather than baking credentials into container images
- upgrading the Docker base image from the current Python 3.9 environment
- exposing a clearer cluster-management/demo interface

These improvements will make the project easier to test, explain, maintain, and evaluate as a distributed-systems project.

## Why This Project

The project is designed to demonstrate practical distributed-systems concepts rather than only implementing a standalone algorithm:

- distributed coordination
- leader election
- failure detection
- inter-node communication
- containerized multi-node deployment
- persistent distributed state

## License

License information will be added as the project is prepared for public distribution.
