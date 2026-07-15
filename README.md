# Kache

A Redis-like in-memory key-value store built from scratch in Python — raw TCP sockets, no frameworks. Supports TTL expiry, append-only persistence, and primary-replica replication. Deployed on EC2 via Docker Compose.

> **Why "Kache"?** It's a cache. It's also a key-value store. Its also just a fun name. (The client class was briefly named `KevoClient`. We're not talking about that.)

---

## What it does

Kache speaks a simplified version of the Redis wire protocol over a raw TCP connection. You can talk to it with `nc`, the Python client, or anything that can open a socket.

**Supported commands:**

```
SET key value [EX seconds]   # store a value, with optional TTL
GET key                      # retrieve a value
DELETE key                   # remove a key
EXISTS key                   # returns 1 or 0
FLUSH                        # wipe everything
KEYS                         # list all active keys
```

**Response protocol:**

```
+OK          # success
$value       # string value returned
$nil         # key not found or expired
-ERR message # error
```

---

## Quickstart (under 5 minutes)

**Prerequisites:** Python 3.12+, Docker (for the full setup)

### Option 1 — Run the server locally

```bash
git clone https://github.com/HWalker13/kache.git
cd kache
python3 server/server.py
```

In a second terminal, connect with netcat:

```bash
nc localhost 6379
```

Then type commands:

```
SET name holden
+OK
GET name
$holden
EXISTS name
1
DELETE name
+OK
GET name
$nil
```

### Option 2 — Use the Python client

```bash
python3 client/client.py
```

Or import it in your own code:

```python
from client.client import KacheClient

client = KacheClient(host="localhost", port=6379)

client.set("name", "holden")
client.get("name")          # "holden"
client.set("session", "abc123", ttl=30)
client.exists("session")    # True
client.delete("name")
client.keys()               # []
client.flush()
```

### Option 3 — Run with Docker Compose (primary + replica)

```bash
docker compose up --build
```

This spins up two containers on a shared network:

- Primary on port `6379`
- Replica on port `6380`

Test replication:

```bash
# Write to primary
nc localhost 6379
SET foo bar
+OK

# Read from replica
nc localhost 6380
GET foo
$bar
```

---

## Architecture

```
Client  →  Primary (port 6379)
                ↓  replicates every write
           Replica (port 6380)
```

**Primary** accepts client connections and handles all writes. On every successful `SET`, `DELETE`, or `FLUSH`, it forwards the command to the replica over a dedicated TCP connection.

**Replica** only accepts connections from the primary and applies commands directly to its own store — no AOF write of its own.

**Offline replica handling:** If the replica goes down, the primary queues writes in memory (`collections.deque`). When the replica reconnects, the queue flushes before steady-state sync resumes. No writes are lost.

---

## How each piece works

### In-memory store

The entire database is a Python `dict`. A second dict maps keys to their absolute Unix expiry timestamps. A `threading.Lock` wraps every read and write so multiple clients share the store safely.

```python
store  = {}
expiry = {}  # key -> float unix timestamp
```

### TTL expiry

Two-pronged, exactly like Redis:

**Lazy expiry** — checked on every `GET`. If the key exists but its timestamp is in the past, it's deleted and `$nil` is returned.

**Active eviction** — a background thread sweeps all keys every second and removes expired ones. This prevents dead keys from accumulating in memory indefinitely.

### AOF persistence

Every write (`SET`, `DELETE`, `FLUSH`) is appended to `aof.log` in plain text. On startup, the server reads the log line by line and replays it to rebuild state.

The critical detail: TTLs are stored as **absolute Unix timestamps**, not relative seconds. A key logged as `SET session abc EX 1748000000.0` expires at that Unix time — not 30 seconds from whenever the server restarts. Without this, a key set yesterday with `EX 30` would become immortal after a restart.

```
SET name holden
SET session abc EX 1779747138.982763
DELETE name
```

### Replication

The primary runs a dedicated replication thread that maintains a persistent TCP connection to the replica. On every successful write, the raw command string is pushed to a `collections.deque` and the thread is woken via a `threading.Event`. Commands are sent in order; the queue buffers them if the replica is offline.

The server mode is controlled via CLI args:

```bash
# Primary
python3 server/server.py --mode primary --replica-host 127.0.0.1 --replica-port 6380

# Replica
python3 server/server.py --mode replica --port 6380

# Standalone (default, no replication)
python3 server/server.py
```

---

## Project structure

```
kache/
├── server/
│   └── server.py       # TCP server, command parser, TTL, AOF, replication
├── client/
│   └── client.py       # KacheClient — importable Python library
├── tests/
│   └── ...             # pytest test suite
├── aof.log             # append-only write log (auto-generated)
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Deployment

Both primary and replica run as Docker containers on a single EC2 t2.micro instance. Docker's internal DNS resolves the service name `replica` automatically — no hardcoded IPs inside the containers.

```yaml
# docker-compose.yml (simplified)
services:
  primary:
    build: .
    command: python3 server/server.py --mode primary --replica-host replica --replica-port 6380
    ports: ["6379:6379"]

  replica:
    build: .
    command: python3 server/server.py --mode replica --port 6380
    ports: ["6380:6380"]
```


## Built with

Python 3.12 · `socket` · `threading` · `collections.deque` · Docker · EC2
