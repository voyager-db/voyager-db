<img src="voyager.png" alt="Project banner" width="50%">

# Voyager-DB

Voyager-DB is an **etcd v3 API–compatible KV server** powered by our [Raftx](https://github.com/voyager-db/raftx) consensus engine.  
It aims to be a **drop-in replacement** for etcd (clientv3 works unchanged), with support for:

- **Classic Raft**
- **Fast Raft** (`--enable-fast-path`)
- **C-Raft** (global tier; optional)

---

## Why Voyager-DB?

Distributed systems live and die by consensus. etcd set the standard for strongly consistent coordination, but **latency and global scalability** have become bottlenecks for modern workloads:

- In **classic Raft**, every client write requires full quorum agreement, even if all replicas are local, which adds overhead.
- In **multi-region deployments**, Raft’s requirement for majority agreement across continents makes global consensus slow and expensive.
- As clusters scale, **log compaction, snapshotting, and membership changes** become operational pain points.

Voyager-DB addresses these challenges head-on:

### 🚀 Fast Raft
Our engine can safely fast-commit writes in the common case (same-term, full quorum) with **lower latency** than classic Raft.  
This means **faster key-value operations** without sacrificing linearizability.

### 🌍 C-Raft (Global Consensus)
Voyager-DB introduces **C-Raft**, a hierarchical global tier:

- Local clusters reach consensus quickly (Fast Raft).
- A smaller set of global voters exchange decisions across regions.
- This reduces the number of wide-area round-trips while preserving strong guarantees, enabling **efficient global consensus** at scale.

### 🔄 Operationally Simple
- **etcd v3 API compatibility**: your existing clients and controllers work unchanged.
- **Bolt-backed storage + Raft WAL**: safe recovery and durability.
- **Dynamic membership (ConfChange)**: add/remove nodes without downtime.
- **Snapshots + compaction**: bounded logs, predictable storage use.

---

## Why it matters

Voyager-DB isn’t just “yet another KV store.” It’s:

- A **drop-in etcd replacement** with **better performance** under fast path commits.
- A path to **global coordination** without burning latency budgets on every request.
- A foundation for **cloud-native infrastructure** (Kubernetes, service discovery, locks, leader election) that increasingly spans **multi-region** or **multi-cloud** topologies.
- A **research/production hybrid**: new ideas like C-Raft can be tested and deployed without rewriting your control plane.


## Features 

- ✅ **KV API**: Put, Get/Range, Delete, Txn (with compares: VERSION/MOD/CREATE/VALUE)
- ✅ **Watch API**: bi-di streams, event fanout from apply path
- ✅ **Lease API**: Grant, KeepAlive, TTL, Revoke (in-memory + Bolt-backed; key expiry deletes)
- ✅ **Cluster API**: MemberList (static bootstrap)
- ✅ **Maintenance API**:
  - Status (version, dbSize, raftTerm, raftIndex, etc.)
  - HashKV
  - Snapshot (streaming; works with `etcdctl snapshot save`)
  - Defragment (stub), Alarm (stub)
- ✅ **Persistence**:
  - MVCC state in Bolt (`state.db`)
  - Raft WAL + hardstate + snapshot metadata in Bolt (`raft.db`)
  - Restore raft state from WAL on restart
- ✅ **Snapshots & compaction**:
  - Periodic raft snapshots every `--raft-snapshot-every` entries
  - Log compaction keeps `--raft-compact-keep` trailing entries
- ✅ **Linearizable reads** via barrier proposals
- ✅ **Static cluster bootstrap** via `--initial-cluster`
- ✅ **Healthz/Readyz** HTTP endpoints
- ✅ **3-node Docker Compose demo**

---

## Build

```bash
go build ./cmd/voyagerd
go build ./cmd/voyagerctl
```

---

## Run (single node)

```bash
./voyagerd \
  --name n1 \
  --data-dir=./data1 \
  --client-listen=:2379 \
  --peer-listen=:2380 \
  --initial-cluster "n1=http://127.0.0.1:2380"
```

---

## Run (3-node cluster with Fast Raft)

```bash
# node 1
./voyagerd \
  --name n1 --data-dir=./data1 \
  --client-listen=:2379 --peer-listen=:2380 \
  --initial-cluster "n1=http://127.0.0.1:2380,n2=http://127.0.0.1:3380,n3=http://127.0.0.1:4380" \
  --enable-fast-path

# node 2
./voyagerd \
  --name n2 --data-dir=./data2 \
  --client-listen=:3379 --peer-listen=:3380 \
  --initial-cluster "n1=http://127.0.0.1:2380,n2=http://127.0.0.1:3380,n3=http://127.0.0.1:4380" \
  --enable-fast-path

# node 3
./voyagerd \
  --name n3 --data-dir=./data3 \
  --client-listen=:4379 --peer-listen=:4380 \
  --initial-cluster "n1=http://127.0.0.1:2380,n2=http://127.0.0.1:3380,n3=http://127.0.0.1:4380" \
  --enable-fast-path
```

---

## CLI Examples

### KV

```bash
# Put and Get
etcdctl --endpoints=:2379 put foo bar
etcdctl --endpoints=:2379 get foo

# Delete with prev-kv
etcdctl --endpoints=:2379 del foo --prev-kv -w=json
```


### Watch

```bash
# Terminal 1
etcdctl --endpoints=:2379 watch /w/key

# Terminal 2
etcdctl --endpoints=:2379 put /w/key v1
etcdctl --endpoints=:2379 put /w/key v2
etcdctl --endpoints=:2379 del /w/key
```

### Lease

```bash
# Grant a lease
etcdctl --endpoints=:2379 lease grant 10

# Put with lease
etcdctl --endpoints=:2379 put --lease=<leaseID> /lease/k1 v1

# Keep alive
etcdctl --endpoints=:2379 lease keep-alive <leaseID>

# TTL
etcdctl --endpoints=:2379 lease timetolive <leaseID>
```

### Maintenance

```bash
# Status
etcdctl --endpoints=:2379 endpoint status -w=json

# HashKV
etcdctl --endpoints=:2379 endpoint hashkv

# Snapshot
etcdctl --endpoints=:2379 snapshot save voyager.snap
```

---

## Flags

- `--name` : node name (matches initial-cluster)
- `--data-dir` : data directory (Bolt DBs, WAL)
- `--client-listen` : client listen address (default `:2379`)
- `--peer-listen` : raft peer listen address (default `:2380`)
- `--initial-cluster` : static bootstrap peers, e.g. `"n1=http://127.0.0.1:2380,n2=http://127.0.0.1:3380,n3=http://127.0.0.1:4380"`
- `--enable-fast-path` : enable Raftx fast commits
- `--enable-c-raft` : enable global tier
- `--raft-snapshot-every` : create a raft snapshot every N applied entries (default 10000)
- `--raft-compact-keep` : keep last K entries after snapshot compaction (default 5000)

---

### Roadmap

| Area              | Status |
|-------------------|--------|
| KV / Range/Txn    | ✅ Done |
| Watch             | ✅ Done |
| Leases            | ✅ Done |
| Maintenance (Status, HashKV, Snapshot) | ✅ Done |
| Fast Raft         | ✅ Validated |
| WAL + Restore     | ✅ Done |
| Dynamic Membership (Add/Remove) | ✅ Done |
| MemberUpdate, Learners | ⏳ Planned |
| MVCC ↔ Raft snapshot integration | ⏳ Planned |
| Maintenance: MoveLeader, Defragment | ⏳ Planned |
| Compactor, watch progress | ⏳ Planned |
| Metrics & observability | ⏳ Planned |
| Chaos & perf testing | ⏳ Planned |


---

## License

Apache 2.0
