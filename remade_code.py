"""ARCHANGEL-8 AMITY INTEGRATION - Full Sensory AI with Persistent Episodic Memory
v1.2.1: Circulatory System - Orchestrator8 + Arteries + Feedback Loop (Fixed)
"""
import json
import os
import time
import threading
import logging
import atexit
import tempfile
from typing import Dict, List, Deque, Optional, Any
from dataclasses import dataclass, field, asdict
from collections import deque

logger = logging.getLogger(__name__)

DEFAULT_MAX_SENSORY = 1000
DEFAULT_MAX_EPISODIC = 20000

__all__ = [
    "RingBuffer", "SensorReading", "EpisodicMemory",
    "SessionState", "SessionManager",
    "CirculatoryPacket", "VenousReturn", "Orchestrator8"
]

# ===== CORE DATA STRUCTURES =====
class RingBuffer:
    """Thread-safe ring buffer for high-frequency telemetry data"""
    def __init__(self, capacity: int = 4096):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        self.data = [0.0] * capacity
        self.head = 0
        self.tail = 0
        self._size = 0
        self._lock = threading.Lock()

    def push(self, val: float):
        with self._lock:
            if self._size == self.capacity:
                self.tail = (self.tail + 1) % self.capacity
            else:
                self._size += 1
            self.data[self.head] = float(val)
            self.head = (self.head + 1) % self.capacity

    def get_all(self) -> List[float]:
        with self._lock:
            result = []
            idx = self.tail
            for _ in range(self._size):
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
            self.data = [0.0] * self.capacity


@dataclass
class SensorReading:
    timestamp: float
    profile: str
    wave: float
    friction: float
    temp: float

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'SensorReading':
        return cls(
            timestamp=float(data.get("timestamp", time.time())),
            profile=str(data.get("profile", "")),
            wave=float(data.get("wave", 0.0)),
            friction=float(data.get("friction", 0.0)),
            temp=float(data.get("temp", 0.0)),
        )


@dataclass
class EpisodicMemory:
    timestamp: float
    event_type: str
    content: Dict

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'EpisodicMemory':
        return cls(
            timestamp=float(data.get("timestamp", time.time())),
            event_type=str(data.get("event_type", "note")),
            content=data.get("content", {}) or {},
        )


@dataclass
class SessionState:
    version: str = "1.2.1"
    pilot_signature: str = "Ren"
    session_start: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    # FIX: Use deque for O(1) append/pop at capacity boundaries
    sensory_samples: Deque[SensorReading] = field(default_factory=lambda: deque(maxlen=DEFAULT_MAX_SENSORY))
    episodic_log: Deque[EpisodicMemory] = field(default_factory=lambda: deque(maxlen=DEFAULT_MAX_EPISODIC))
    sample_count: int = 0
    buffer_overflow_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "version": self.version,
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
        # FIX: Default to current version
        state.version = data.get("version", "1.2.1")
        state.pilot_signature = data.get("pilot_signature", state.pilot_signature)
        state.session_start = float(data.get("session_start", state.session_start))
        state.last_update = float(data.get("last_update", state.last_update))

        raw_samples = data.get("sensory_samples", [])
        samples = []
        for item in raw_samples:
            if isinstance(item, dict):
                samples.append(SensorReading.from_dict(item))
            elif isinstance(item, SensorReading):
                samples.append(item)
            else:
                logger.debug("Skipping malformed sensory sample: %r", item)
        # Reconstruct deque with maxlen
        state.sensory_samples = deque(samples, maxlen=DEFAULT_MAX_SENSORY)

        raw_episodic = data.get("episodic_log", [])
        episodes = []
        for item in raw_episodic:
            if isinstance(item, dict):
                episodes.append(EpisodicMemory.from_dict(item))
            elif isinstance(item, EpisodicMemory):
                episodes.append(item)
            else:
                logger.debug("Skipping malformed episodic entry: %r", item)
        state.episodic_log = deque(episodes, maxlen=DEFAULT_MAX_EPISODIC)

        state.sample_count = int(data.get("sample_count", len(state.sensory_samples)))
        state.buffer_overflow_count = int(data.get("buffer_overflow_count", 0))
        return state


# ===== CIRCULATORY SYSTEM =====
@dataclass
class CirculatoryPacket:
    timestamp: float
    oxygen_level: float  # 0.0-1.0. Decays with priority
    origin_sector: str
    payload: Dict[str, Any]
    priority_tier: int = 0

    def age(self) -> float:
        return time.time() - self.timestamp

    def is_fresh(self) -> bool:
        # FIX: >= 0.1 so priority 9 packets (oxygen=0.1) are not silently dropped
        return self.oxygen_level >= 0.1 and self.age() < 0.5


@dataclass
class VenousReturn:
    sector: str
    metrics: Dict[str, Any] = field(default_factory=dict)


class SectorInterface:
    def __init__(self, name: str):
        self.name = name

    def ingest(self, packet: CirculatoryPacket) -> Optional[VenousReturn]:
        raise NotImplementedError


class SensoryArtery(SectorInterface):
    def __init__(self, session_manager):
        super().__init__("sensory")
        self.sm = session_manager

    def ingest(self, packet: CirculatoryPacket) -> Optional[VenousReturn]:
        if "friction" in packet.payload:
            reading = SensorReading(
                timestamp=packet.timestamp,
                profile=packet.payload.get("profile", "default"),
                wave=packet.payload.get("wave", 0.0),
                friction=packet.payload.get("friction", 0.0),
                temp=packet.payload.get("temp", 0.0),
            )
            self.sm.add_sensor_reading(reading)
            self.sm.telemetry_buffer.push(reading.wave)
        pressure = self.sm.telemetry_buffer.size() / max(1, self.sm.telemetry_buffer.capacity)
        return VenousReturn("sensory", {"buffer_pressure": pressure})


class EmotionalArtery(SectorInterface):
    def __init__(self):
        super().__init__("emotional")
        self.target_temp = 37.0

    def ingest(self, packet: CirculatoryPacket) -> Optional[VenousReturn]:
        if "sentiment" in packet.payload:
            drift = abs(self.target_temp - packet.payload.get("temp", self.target_temp))
            return VenousReturn("emotional", {"thermal_drift": drift})
        return None


class MemoryArtery(SectorInterface):
    def __init__(self, session_manager):
        super().__init__("memory")
        self.sm = session_manager

    def ingest(self, packet: CirculatoryPacket) -> Optional[VenousReturn]:
        if packet.priority_tier >= 7:
            episode = EpisodicMemory(
                timestamp=packet.timestamp,
                event_type=packet.payload.get("event_type", "note"),
                content=packet.payload,
            )
            self.sm.add_episode(episode)
        pulse = 1 if packet.priority_tier >= 5 else 0
        return VenousReturn("memory", {"consolidation_pulse": pulse})


class Orchestrator8:
    """The Heart. Systolic pulse -> distribute -> collect -> adjust"""

    def __init__(self, session_manager):
        self.sm = session_manager
        self.arteries: Dict[str, SectorInterface] = {
            "sensory": SensoryArtery(session_manager),
            "emotional": EmotionalArtery(),
            "memory": MemoryArtery(session_manager),
        }
        self.last_venous: Dict[str, VenousReturn] = {}
        # FIX: Adjustment state carries forward to next beat
        self.throttle_flag: bool = False
        self.thermal_drift: float = 0.0
        self.consolidation_urgency: int = 0
        # Add a lock to protect internal state when heartbeat may be called concurrently
        self._lock = threading.RLock()

    def contract(self, payload: Dict[str, Any], origin: str, priority: int = 0) -> CirculatoryPacket:
        # FIX: Ensure high-priority packets get enough oxygen to pass is_fresh()
        oxygen = max(0.15, 1.0 - (priority / 10.0))
        return CirculatoryPacket(time.time(), oxygen, origin, payload, priority)

    def distribute(self, packet: CirculatoryPacket):
        venous = []
        # Call artery.ingest without holding Orchestrator lock to avoid deadlocks
        for artery in self.arteries.values():
            try:
                ret = artery.ingest(packet)
            except Exception:
                logger.exception("Artery %s failed to ingest packet", getattr(artery, 'name', 'unknown'))
                ret = None
            if ret:
                venous.append(ret)
        # Update last_venous in a short critical section
        with self._lock:
            self.last_venous = {v.sector: v for v in venous}

    def adjust(self):
        """Homeostasis: read venous feedback and modulate next beat."""
        with self._lock:
            # keep default values and then update based on venous feedback
            self.throttle_flag = False
            self.thermal_drift = 0.0
            self.consolidation_urgency = 0

            pressure = self.last_venous.get("sensory")
            if pressure:
                bp = pressure.metrics.get("buffer_pressure", 0)
                if bp > 0.9:
                    self.throttle_flag = True
                    logger.warning("High buffer pressure (%.2f) - throttling next beat", bp)

            drift = self.last_venous.get("emotional")
            if drift:
                self.thermal_drift = drift.metrics.get("thermal_drift", 0)
                if self.thermal_drift > 2.0:
                    logger.warning("Thermal drift detected (%.2f°C)", self.thermal_drift)

            mem = self.last_venous.get("memory")
            if mem:
                self.consolidation_urgency = mem.metrics.get("consolidation_pulse", 0)

    def heartbeat(self, payload: Dict[str, Any], origin: str, priority: int = 0):
        # Check throttle flag under lock but avoid holding the lock during ingestion
        with self._lock:
            throttling = self.throttle_flag
        if throttling and priority < 5:
            logger.debug("Throttling non-critical beat from %s", origin)
            return

        pulse = self.contract(payload, origin, priority)
        if not pulse.is_fresh():
            logger.debug("Packet stale or depleted - dropping beat from %s", origin)
            return
        # distribute (ingestion happens without long-held orchestrator lock)
        self.distribute(pulse)
        # adjust will acquire lock briefly
        self.adjust()


# ===== SESSION MANAGER =====
class SessionManager:
    def __init__(self, max_sensory: int = DEFAULT_MAX_SENSORY, max_episodic: int = DEFAULT_MAX_EPISODIC, telemetry_capacity: Optional[int] = None):
        # record desired caps
        self.max_sensory = int(max_sensory)
        self.max_episodic = int(max_episodic)
        # create state with correct deque maxlen right away
        self.state = SessionState()
        # Rebuild the deques to respect requested maxlen (handles default state constructor)
        self.state.sensory_samples = deque(self.state.sensory_samples, maxlen=self.max_sensory)
        self.state.episodic_log = deque(self.state.episodic_log, maxlen=self.max_episodic)

        # telemetry buffer capacity: default to provided telemetry_capacity or max_sensory
        if telemetry_capacity is None:
            telemetry_capacity = max(256, self.max_sensory)
        self.telemetry_buffer = RingBuffer(capacity=int(telemetry_capacity))

        self.heart = Orchestrator8(self)

        self._lock = threading.RLock()
        self._stop_saver = threading.Event()
        self._saver_thread: Optional[threading.Thread] = None
        atexit.register(self.stop_periodic_save)

    def add_sensor_reading(self, reading: SensorReading):
        with self._lock:
            # FIX: deque handles maxlen automatically - no manual pop(0)
            if len(self.state.sensory_samples) == self.state.sensory_samples.maxlen:
                self.state.buffer_overflow_count += 1
                logger.debug("sensory_samples deque overflow; incremented buffer_overflow_count")
            self.state.sensory_samples.append(reading)
            self.state.sample_count += 1
            self.state.last_update = time.time()

    def add_episode(self, episode: EpisodicMemory):
        with self._lock:
            # FIX: deque handles maxlen automatically
            self.state.episodic_log.append(episode)
            self.state.last_update = time.time()

    def daily_recall(self, days: int = 1) -> List[EpisodicMemory]:
        cutoff = time.time() - (days * 86400)
        with self._lock:
            return [e for e in self.state.episodic_log if e.timestamp >= cutoff]

    def save_to_file(self, path: str, atomic: bool = True):
        with self._lock:
            data = self.state.to_dict()
        dirname = os.path.dirname(path) or "."
        os.makedirs(dirname, exist_ok=True)
        if atomic:
            try:
                with tempfile.NamedTemporaryFile(
                    'w', delete=False, dir=dirname, suffix='.tmp', encoding='utf-8'
                ) as tmp:
                    json.dump(data, tmp, indent=2)
                    tmp.flush()
                    os.fsync(tmp.fileno())
                    tmp_path = tmp.name
                os.replace(tmp_path, path)
            except Exception:
                logger.exception("Failed to save session state to %s", path)
                if 'tmp_path' in locals():
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                raise
        else:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    @classmethod
    def load_from_file(cls, path: str, max_sensory: int = DEFAULT_MAX_SENSORY, max_episodic: int = DEFAULT_MAX_EPISODIC, telemetry_capacity: Optional[int] = None) -> 'SessionManager':
        manager = cls(max_sensory, max_episodic, telemetry_capacity=telemetry_capacity)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # load state from disk
            loaded_state = SessionState.from_dict(data)
            # preserve cumulative sample_count if present, otherwise keep loaded len
            # reconstruct deques with manager's configured maxlens to avoid ignoring user settings
            loaded_state.sensory_samples = deque(loaded_state.sensory_samples, maxlen=manager.max_sensory)
            loaded_state.episodic_log = deque(loaded_state.episodic_log, maxlen=manager.max_episodic)
            manager.state = loaded_state
        except FileNotFoundError:
            logger.info("No session file found at %s, starting fresh", path)
        except json.JSONDecodeError as e:
            logger.error("Corrupted session file %s: %s. Starting fresh.", path, e)
        except Exception as e:
            logger.exception("Failed to load session state: %s", e)
        return manager

    def start_periodic_save(self, path: str, interval_sec: int = 60):
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")

        def _saver():
            logger.info("SessionManager periodic saver started (interval=%s)", interval_sec)
            while not self._stop_saver.is_set():
                try:
                    self.save_to_file(path)
                except Exception:
                    logger.exception("Failed to save session state to %s", path)
                self._stop_saver.wait(interval_sec)
            logger.info("SessionManager periodic saver stopped")

        with self._lock:
            if self._saver_thread and self._saver_thread.is_alive():
                logger.warning("Periodic saver already running")
                return
            self._stop_saver.clear()
            self._saver_thread = threading.Thread(target=_saver, daemon=True)
            self._saver_thread.start()

    def stop_periodic_save(self):
        with self._lock:
            if self._saver_thread:
                self._stop_saver.set()
                self._saver_thread.join(timeout=5)
                self._saver_thread = None


# ===== EXAMPLE =====
def _example_usage():
    manager = SessionManager()
    # One heartbeat cycle
    manager.heart.heartbeat(
        {"friction": 0.85, "temp": 63.0, "wave": 1.2, "profile": "A"},
        "sensory", priority=3
    )
    manager.heart.heartbeat(
        {"sentiment": 9.0, "temp": 37.1},
        "emotional", priority=8
    )
    manager.heart.heartbeat(
        {"event_type": "milestone", "note": "first heartbeat"},
        "memory", priority=9
    )
    print("Venous return:", manager.heart.last_venous)
    print("Throttle flag:", manager.heart.throttle_flag)
    print("Thermal drift:", manager.heart.thermal_drift)
    manager.save_to_file("session_state.json")


if __name__ == "__main__":
    _example_usage()
