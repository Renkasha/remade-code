# Remade Code — ARCHANGEL-8 Amity Integration

Full Sensory AI with Persistent Episodic Memory

This repository contains the ARCHANGEL-8 "Amity" integration: a lightweight in‑process runtime for high-frequency sensory telemetry, bounded episodic memory, and a simple circulatory orchestrator (Orchestrator8) that routes packets to domain-specific arteries (sensory, emotional, memory).

Key features
- Thread-safe RingBuffer for high-frequency numeric telemetry
- Bounded sensory_samples and episodic_log using collections.deque with configurable max lengths
- Atomic session save/load with JSON serialization
- Orchestrator8: distributes CirculatoryPacket objects to SectorInterface implementations and adjusts homeostasis based on venous returns
- Safe defaults plus options to tune capacities for telemetry and episodic memory

Recent changes (v1.2.1)
- SessionManager now honors the max_sensory and max_episodic constructor arguments and reconstructs saved deques to the configured maxlen
- telemetry_buffer capacity is configurable (telemetry_capacity) and defaults to max(256, max_sensory) if not provided
- Orchestrator8 now uses a reentrant lock to protect internal state (last_venous, throttle_flag, etc.) and catches artery ingestion exceptions so a single failing artery won't crash distribution
- Improved robustness on save/load and clarified overflow counting behavior

Hotfixes applied after initial release
- Orchestrator8.distribute now calls artery.ingest() outside the Orchestrator lock to avoid potential deadlocks and reduce contention; last_venous is updated under a short critical section.
- SensoryArtery buffer_pressure computation now guards against division-by-zero.

Quick start

1. Install (pure Python module — no external deps):

   python3 -m pip install -r requirements.txt  # if you add tests/CI deps

2. Basic usage

```python
from remade_code import SessionManager

# create a manager with custom caps
mgr = SessionManager(max_sensory=2000, max_episodic=50000, telemetry_capacity=2048)

# heartbeat examples
mgr.heart.heartbeat({"friction":0.85,"temp":36.6,"wave":1.2,"profile":"A"}, "sensory", priority=3)
mgr.heart.heartbeat({"sentiment":9.0,"temp":37.1}, "emotional", priority=8)
mgr.heart.heartbeat({"event_type":"milestone","note":"first heartbeat"}, "memory", priority=9)

# persist state
mgr.save_to_file("session_state.json")

# load from disk (respects provided caps)
mgr2 = SessionManager.load_from_file("session_state.json", max_sensory=2000, max_episodic=50000)
```

Configuration notes
- SessionManager(max_sensory, max_episodic, telemetry_capacity)
  - max_sensory: maximum number of SensorReading objects kept in memory (deque maxlen)
  - max_episodic: maximum number of EpisodicMemory entries kept in memory (deque maxlen)
  - telemetry_capacity: capacity for the numeric RingBuffer used for waveform telemetry; defaults to max(256, max_sensory)

Testing recommendations
- Add unit tests for:
  - RingBuffer boundary behavior and concurrency
  - SessionManager save/load cycles with different max sizes
  - Orchestrator8 concurrent heartbeat calls

Contributing
- Open a pull request against the `main` branch. Prefer small, focused changes and include tests for any behavior changes.

License
- (Add your preferred license here)
