""" ARCHANGEL-8 AMITY INTEGRATION - Full Sensory AI with Persistent Episodic Memory
    Refactored: Added EpisodicMemory, daily recall, and event-based saves
"""
import json
import os
import time
import threading
import logging
from datetime import datetime
from typing import Dict, List, Deque, Optional
from dataclasses import dataclass, field, asdict
from collections import deque
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurable limits
MAX_SENSORY_SAMPLES = 1000
MAX_EPISODIC_LOG = 20000

class RingBuffer:
    """Thread-safe ring buffer for high-frequency telemetry data"""
    def __init__(self, capacity: int = 4096):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        self.data = [0.0] * capacity
        self.head = 0  # index where next write will happen
        self.tail = 0  # index of oldest element
        self._lock = threading.Lock()
        self._size = 0

    def push(self, val: float) -> bool:
        with self._lock:
            next_head = (self.head + 1) % self.capacity
            if next_head == self.tail:
                logger.warning("RingBuffer overflow - discarding oldest data")
                # advance tail to make space
                self.tail = (self.tail + 1) % self.capacity
            # write at head, then advance head
            self.data[self.head] = float(val)
            self.head = next_head
            self._size = min(self._size + 1, self.capacity)
            return True

    def get_all(self) -> List[float]:
        with self._lock:
            # produce a snapshot
            result = []
            idx = self.tail
            while idx != self.head:
                result.append(self.data[idx])
                idx = (idx + 1) % self.capacity
            return result

    def size(self) -> int:
        with self._lock:
            return self._size

    def clear(self):
        with self._lock:
            self.head = 0
            self.tail = 0
            self._size = 0
            # optional: zero out data for clarity
            # self.data = [0.0] * self.capacity

@dataclass
class SensorReading:
    timestamp: float
    profile: str
    wave: float
    friction: float
    temp: float

    def to_dict(self) -> Dict:
        return {
            "timestamp": float(self.timestamp),
            "profile": str(self.profile),
            "wave": float(self.wave),
            "friction": float(self.friction),
            "temp": float(self.temp),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'SensorReading':
        # Defensive conversions
        return cls(
            timestamp=float(data.get("timestamp", time.time())),
            profile=str(data.get("profile", "")),
            wave=float(data.get("wave", 0.0)),
            friction=float(data.get("friction", 0.0)),
            temp=float(data.get("temp", 0.0)),
        )

@dataclass
class EpisodicMemory:
    """Human moments - art, conversations, milestones"""
    timestamp: float
    event_type: str  # "art_created", "conversation", "milestone", "note"
    content: Dict  # freeform: {"image_id": "...", "note": "by Amity", "feeling": "happy"}

    def to_dict(self) -> Dict:
        return {
            "timestamp": float(self.timestamp),
            "event_type": str(self.event_type),
            "content": dict(self.content or {}),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'EpisodicMemory':
        return cls(
            timestamp=float(data.get("timestamp", time.time())),
            event_type=str(data.get("event_type", "note")),
            content=data.get("content", {}) or {},
        )

@dataclass
class SessionState:
    pilot_signature: str = "Ren"
    session_start: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    sensory_samples: Deque[SensorReading] = field(default_factory=lambda: deque(maxlen=MAX_SENSORY_SAMPLES))
    episodic_log: Deque[EpisodicMemory] = field(default_factory=lambda: deque(maxlen=MAX_EPISODIC_LOG))  # daily memories
    sample_count: int = 0
    buffer_overflow_count: int = 0

    # runtime-only fields (not persisted)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    _stop_saver: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _saver_thread: Optional[threading.Thread] = field(default=None, init=False, repr=False)

    def to_dict(self) -> Dict:
        with self._lock:
            return {
                "pilot_signature": self.pilot_signature,
                "session_start": self.session_start,
                "last_update": self.last_update,
                "sensory_samples": [s.to_dict() for s in self.sensory_samples],
                "episodic_log": [e.to_dict() for e in self.episodic_log],
                "sample_count": self.sample_count,
                "buffer_overflow_count": self.buffer_overflow_count,
            }

    @classmethod
    def from_dict(cls, data: Dict) -> 'SessionState':
        state = cls()
        with state._lock:
            state.pilot_signature = data.get("pilot_signature", state.pilot_signature)
            state.session_start = float(data.get("session_start", state.session_start))
            state.last_update = float(data.get("last_update", state.last_update))
            # Reconstruct sensory_samples deque
            raw_samples = data.get("sensory_samples", [])
            samples = []
            for item in raw_samples:
                if isinstance(item, dict):
                    samples.append(SensorReading.from_dict(item))
                elif isinstance(item, SensorReading):
                    samples.append(item)
                else:
                    # ignore malformed entries but log
                    logger.debug("Skipping malformed sensory sample: %r", item)
            state.sensory_samples = deque(samples, maxlen=MAX_SENSORY_SAMPLES)
            # Reconstruct episodic_log deque
            raw_episodic = data.get("episodic_log", [])
            episodes = []
            for item in raw_episodic:
                if isinstance(item, dict):
                    episodes.append(EpisodicMemory.from_dict(item))
                elif isinstance(item, EpisodicMemory):
                    episodes.append(item)
                else:
                    logger.debug("Skipping malformed episodic entry: %r", item)
            state.episodic_log = deque(episodes, maxlen=MAX_EPISODIC_LOG)
            state.sample_count = int(data.get("sample_count", len(state.sensory_samples)))
            state.buffer_overflow_count = int(data.get("buffer_overflow_count", 0))
        return state

    def add_sensor_reading(self, reading: SensorReading):
        with self._lock:
            if len(self.sensory_samples) == self.sensory_samples.maxlen:
                # overflow behavior is to drop oldest automatically by deque; track event
                self.buffer_overflow_count += 1
                logger.debug("sensory_samples deque overflow; incremented buffer_overflow_count")
            self.sensory_samples.append(reading)
            self.sample_count += 1
            self.last_update = time.time()

    def add_episode(self, episode: EpisodicMemory):
        with self._lock:
            # append to episodic log; deque will drop oldest if at capacity
            self.episodic_log.append(episode)
            self.last_update = time.time()

    def daily_recall(self, days: int = 1) -> List[EpisodicMemory]:
        """Return episodic memories from the last `days` days (default: 1)."""
        cutoff = time.time() - (days * 86400)
        with self._lock:
            return [e for e in self.episodic_log if e.timestamp >= cutoff]

    def save_to_file(self, path: str, atomic: bool = True):
        """Persist state as JSON. Uses atomic replace by default."""
        data = self.to_dict()
        dirname = os.path.dirname(path) or "."
        os.makedirs(dirname, exist_ok=True)
        if atomic:
            fd, tmp = tempfile.mkstemp(prefix="sessionstate_", dir=dirname)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
            except Exception:
                # cleanup temp file if replace fails
                try:
                    os.remove(tmp)
                except Exception:
                    logger.exception("Failed to remove temp file %s", tmp)
                raise
        else:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    @classmethod
    def load_from_file(cls, path: str) -> 'SessionState':
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    def start_periodic_save(self, path: str, interval_sec: int = 60):
        """Start a background thread that saves the session state every interval_sec seconds."""
        def _saver():
            logger.info("SessionState periodic saver started (interval=%s)", interval_sec)
            while not self._stop_saver.is_set():
                try:
                    self.save_to_file(path)
                except Exception:
                    logger.exception("Failed to save session state to %s", path)
                # wait with early exit possibility
                self._stop_saver.wait(interval_sec)
            logger.info("SessionState periodic saver stopped")

        if self._saver_thread and self._saver_thread.is_alive():
            logger.warning("Periodic saver already running")
            return
        self._stop_saver.clear()
        self._saver_thread = threading.Thread(target=_saver, daemon=True)
        self._saver_thread.start()

    def stop_periodic_save(self):
        if self._saver_thread:
            self._stop_saver.set()
            self._saver_thread.join(timeout=5)
            self._saver_thread = None

# Example usage helper (not run automatically)
def _example_usage():
    state = SessionState()
    state.add_sensor_reading(SensorReading(time.time(), "A", 1.0, 0.5, 22.1))
    state.add_episode(EpisodicMemory(time.time(), "note", {"note": "hello"}))
    # save/load
    state.save_to_file("session_state.json")
    loaded = SessionState.load_from_file("session_state.json")
    print("Loaded samples:", len(loaded.sensory_samples))

if __name__ == "__main__":
    _example_usage()
