# PyKV Distributed Store

PyKV Distributed Store is a lightweight, educational distributed key-value store written in Python. It provides a simple API for storing and retrieving string keys and values and demonstrates common distributed-systems concepts like replication, sharding, and eventual consistency. This repository is intended for learning, prototypes, and small demos — not for production use without additional hardening.

## Features

- Simple Python API and CLI to put/get/delete keys
- In-memory and optional on-disk persistence backends
- Configurable replication (master/replica or multi-master demo mode)
- Basic sharding using consistent hashing
- HTTP REST API for remote interaction
- Example clients and test scripts

## Requirements

- Python 3.8+
- pip

Optional:
- Docker (for running multiple nodes locally)

## Installation

1. Clone the repository:

   git clone https://github.com/Hrishikesh544/PyKV-Distributed-Store.git
   cd PyKV-Distributed-Store

2. Create a virtual environment and install dependencies:

   python -m venv .venv
   source .venv/bin/activate  # on Windows: .\.venv\Scripts\activate
   pip install -r requirements.txt

If the project has no requirements.txt, install commonly used libs when needed (e.g., Flask, requests).

## Quick Start (single node)

Run a single node that serves the REST API:

   python -m pykv.server --port 5000

Use the provided client or curl to interact:

   curl -X PUT "http://localhost:5000/kv/mykey" -d "myvalue"
   curl "http://localhost:5000/kv/mykey"

Example using Python requests:

```python
import requests
resp = requests.put('http://localhost:5000/kv/foo', data='bar')
print(resp.status_code)
print(requests.get('http://localhost:5000/kv/foo').text)
```

## Running a multi-node cluster (local demo)

You can run multiple nodes locally on different ports and point them at each other to form a small cluster.

1. Start node A:

   python -m pykv.server --port 5000 --peers http://localhost:5001,http://localhost:5002

2. Start node B:

   python -m pykv.server --port 5001 --peers http://localhost:5000,http://localhost:5002

3. Start node C:

   python -m pykv.server --port 5002 --peers http://localhost:5000,http://localhost:5001

Put a value to any node — replication or sharding will direct/storage behavior according to configuration.

## Configuration

Configuration can be supplied via a YAML/JSON file or command-line flags. Typical options:

- node_id: unique identifier for the node
- bind_address: host:port to bind the HTTP server
- peers: list of peer HTTP endpoints
- replication_factor: number of replicas for each key
- persistence: none | file | sqlite
- data_dir: directory to store on-disk data

Example config (config.yaml):

```yaml
node_id: node-1
bind_address: 0.0.0.0:5000
peers:
  - http://localhost:5001
  - http://localhost:5002
replication_factor: 2
persistence: file
data_dir: ./data
```

Load with:

   python -m pykv.server --config config.yaml

## API

REST endpoints (example):

- GET /kv/<key> — retrieve value for key
- PUT /kv/<key> — set value for key (body contains value)
- DELETE /kv/<key> — delete key
- GET /_status — node health and cluster info

Responses use JSON for metadata and plain text for values by default.

## Design & Architecture

PyKV demonstrates a minimal distributed key-value store architecture:

- HTTP server layer: receives client requests and coordinates with the cluster
- Storage layer: pluggable backends (in-memory, file, sqlite)
- Cluster coordination: basic peer discovery and replication logic
- Sharding: consistent hashing to map keys to responsible nodes
- Replication: asynchronous replication with configurable factor

Refer to the docs/ or docs/architecture.md for a visual diagram and deeper explanation (add if not present).

## Tests

Run unit tests with pytest (if tests exist):

   pytest -q

There are also simple integration scripts under examples/ for running small clusters and exercising replication and failover.

## Docker (optional)

A minimal Dockerfile can be used to run nodes in containers. Build and run:

   docker build -t pykv:latest .
   docker run -p 5000:5000 pykv:latest

For multi-node local testing use docker-compose.yml to launch multiple instances.

## Contributing

Contributions are welcome. Please open issues for bugs or feature requests and submit pull requests for changes. Follow these guidelines:

1. Fork the repo and create a feature branch.
2. Write tests for new features/bug fixes.
3. Ensure linting and formatting (black, flake8) pass.
4. Submit a PR with a clear description and reference any related issues.

## Roadmap / Ideas

- Add strong consistency mode (Raft or Paxos)
- Improve failure detection and anti-entropy (merkle trees)
- Pluggable consensus module with election and leader-driven writes
- Metrics and Prometheus integration

## License

Specify your project's license (e.g., MIT). Example:

Licensed under the MIT License. See LICENSE file for details.

## Contact

Maintainer: Hrishikesh (https://github.com/Hrishikesh544)

If you'd like any specific wording, additional sections, or to tailor the README to the actual code in this repo, I can update it to match — tell me which parts to change.