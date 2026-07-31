# ARCHANGEL-8 AMITY Integration

Full Sensory AI with Persistent Memory - A refactored implementation featuring Episodic Memory (Amity integration), better error handling, memory management, and thread safety.

## Overview

This project provides a comprehensive sensory data capture and processing system with:

- **High-frequency sampling** at 40kHz (25 microseconds per sample)
- **Thread-safe operations** with proper locking mechanisms
- **Persistent memory** with atomic writes and backup recovery
- **Ring buffer** for efficient telemetry data management
- **Configurable sensory profiles** for flexible sensor integration

## New in this release

### Amity integration (ARCHANGEL-8)

This release adds an Amity integration module (amity_integration.py) that provides:

- EpisodicMemory dataclass for timestamped human moments (notes, conversations, art, milestones)
- SessionState with thread-safe deques for sensory and episodic storage
- Atomic JSON persistence and a periodic background saver
- Helpers: add_sensor_reading, add_episode, daily_recall
- RingBuffer improvements and defensive (de)serialization

File: `amity_integration.py` — see the repository root for the new module.

## Features

### 🔧 Core Components

- **RingBuffer**: Thread-safe circular buffer for high-frequency telemetry
- **SensorReading**: Structured sensor data with metadata
- **SessionState**: Persistent session management with memory serialization and episodic memory
- **PersistentMemoryCore**: Atomic file operations with backup/restore (older releases)
- **SensoryEngine**: High-frequency data capture and sweep operations

### 🚀 Key Capabilities

- Capture individual sensor readings with timestamps and profiles
- Execute continuous sensory sweeps with configurable duration
- Automatic periodic saves to prevent data loss
- Full session state recovery from disk
- Buffer overflow management with logging
- Effective sample rate calculation and monitoring

## Installation

```bash
git clone https://github.com/Renkasha/remade-code.git
cd remade-code
python main.py
```

## Usage

### Basic Example

```python
from main import PersistentMemoryCore, SensoryEngine

# Initialize core systems
core = PersistentMemoryCore()
engine = SensoryEngine(core)

# Show initial state
print(core.get_state_summary())

# Run a sensory sweep
stats = engine.sweep(
    profile_name="high_freq_sensor_stream",
    hz=12.0,
    friction=0.30,
    temp=37.0,
    seconds=0.1
)

print(stats)
```

### Amity integration example

```python
from amity_integration import SessionState, SensorReading, EpisodicMemory
import time

state = SessionState()
state.add_sensor_reading(SensorReading(time.time(), "A", 1.0, 0.5, 22.1))
state.add_episode(EpisodicMemory(time.time(), "note", {"note": "hello from Amity"}))
state.save_to_file("session_state.json")
loaded = SessionState.load_from_file("session_state.json")
print("Loaded samples:", len(loaded.sensory_samples))
```

### Capture Single Reading

```python
reading = engine.capture(
    profile_name="sensor_profile",
    hz=10.0,
    friction=0.25,
    temp=25.0
)

print(reading.to_dict())
```

### Access Ring Buffer Data

```python
# Get last 50 buffered values
buffer_data = engine.get_buffer_data(limit=50)
print(f"Buffer contents: {buffer_data}")
```

## Architecture

### Memory Management

- Sessions are persisted to `vanguard_link_memory.json` (or the path you configure)
- Atomic writes with backup file strategy
- Automatic recovery on load failure
- Deque with fixed size limit (1000 samples) to prevent unbounded growth

### Threading

All critical sections are protected with locks:
- Ring buffer push/pop operations
- Sample capture and timing
- File I/O operations

### Sampling

- Fixed 40kHz sampling interval (25 microseconds)
- Configurable frequency profiles
- Synthetic waveform generation for testing
- Friction and temperature tracking

## Configuration

### Sampling Interval

Modify `SensoryEngine.SAMPLING_INTERVAL` to change frequency:
```python
SAMPLING_INTERVAL = 0.000025  # 40kHz
```

### Ring Buffer Capacity

Adjust `RingBuffer` size during initialization:
```python
ring_buffer = RingBuffer(capacity=8192)  # Default: 4096
```

### Sensory Sample History

Modify `SessionState.sensory_samples` deque maxlen:
```python
deque(maxlen=2000)  # Default: 1000
```

## Logging

Logging is configured at INFO level with timestamps. Key log messages:
- `✅` - Successful operations
- `🔍` - Operation start (sweeps)
- `⚠️` - Warnings (overflows, interrupts)
- `❌` - Errors
- `🔄` - Reset operations

## Error Handling

- JSON decode errors with graceful fallback
- IOError handling for file operations
- KeyboardInterrupt support for sweep cancellation
- Buffer overflow detection and logging
- Automatic file recovery from backup

## File Structure

```
remade-code/
├── main.py           # Main implementation
├── amity_integration.py # Amity integration (episodic memory + session state)
├── LICENSE           # Apache 2.0 License
└── README.md         # This file
```

## Data Persistence

### Saved State

The system automatically saves:
- Pilot signature
- Session timestamps
- All sensory samples
- Episodic memories (notes, conversations, milestones)
- Sample count and metrics
- Buffer overflow counters

### File Format

State is saved as JSON with human-readable formatting:
```json
{
  "pilot_signature": "Ren",
  "session_start": 1234567890.5,
  "last_update": 1234567891.2,
  "sensory_samples": [...],
  "sample_count": 42,
  "buffer_overflow_count": 0
}
```

## Performance

- **Ring Buffer Operations**: O(1) push/pop
- **Memory Footprint**: ~512KB for default 4096-capacity ring buffer
- **Sample Rate**: Effective capture rate of 100-1000Hz depending on system load
- **File I/O**: Atomic writes with ~10-50ms latency

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## Author

Created by **Renkasha**

---

**Status**: Active Development  
**Version**: 1.1.0  
**Last Updated**: 2026-07-31  
