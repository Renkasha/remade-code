"""
ARCHANGEL-7 AMITY INTEGRATION - Full Sensory AI with Persistent Memory
Refactored: Better error handling, memory management, and thread safety
"""

import json
import os
import time
import math
import threading
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import deque

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RingBuffer:
    """Thread-safe ring buffer for high-frequency telemetry data with typed entries"""
    
    def __init__(self, capacity: int = 4096):
        self.capacity = capacity
        self.data = [0.0] * capacity
        self.head = 0
        self.tail = 0
        self._lock = threading.Lock()
        self._size = 0
    
    def push(self, val: float) -> bool:
        """Push value to buffer. Returns False if buffer full."""
        with self._lock:
            next_head = (self.head + 1) % self.capacity
            if next_head == self.tail:
                logger.warning("RingBuffer overflow - discarding oldest data")
                self.tail = (self.tail + 1) % self.capacity
            
            self.data[self.head] = val
            self.head = next_head
            self._size = min(self._size + 1, self.capacity)
            return True
    
    def pop(self) -> Optional[float]:
        """Pop value from buffer. Returns None if empty."""
        with self._lock:
            if self.tail == self.head:
                return None
            val = self.data[self.tail]
            self.tail = (self.tail + 1) % self.capacity
            self._size = max(self._size - 1, 0)
            return val
    
    def get_all(self) -> List[float]:
        """Get all buffered values in FIFO order."""
        with self._lock:
            result = []
            idx = self.tail
            while idx != self.head:
                result.append(self.data[idx])
                idx = (idx + 1) % self.capacity
            return result
    
    def size(self) -> int:
        """Get current buffer size."""
        with self._lock:
            return self._size
    
    def clear(self):
        """Clear the buffer."""
        with self._lock:
            self.head = 0
            self.tail = 0
            self._size = 0


@dataclass
class SensorReading:
    """Individual sensor reading with metadata"""
    timestamp: float
    profile: str
    wave: float
    friction: float
    temp: float
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SessionState:
    """Thread-safe session state with memory management"""
    pilot_signature: str = "Ren"
    session_start: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    sensory_samples: deque = field(default_factory=lambda: deque(maxlen=1000))
    sample_count: int = 0
    buffer_overflow_count: int = 0
    
    def to_dict(self) -> Dict:
        """Serialize to dict, converting deque to list"""
        return {
            "pilot_signature": self.pilot_signature,
            "session_start": self.session_start,
            "last_update": self.last_update,
            "sensory_samples": list(self.sensory_samples),
            "sample_count": self.sample_count,
            "buffer_overflow_count": self.buffer_overflow_count,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SessionState':
        """Deserialize from dict"""
        state = cls()
        state.pilot_signature = data.get("pilot_signature", "Ren")
        state.session_start = data.get("session_start", time.time())
        state.last_update = data.get("last_update", time.time())
        state.sample_count = data.get("sample_count", 0)
        state.buffer_overflow_count = data.get("buffer_overflow_count", 0)
        
        # Rebuild deque with samples
        samples = data.get("sensory_samples", [])
        state.sensory_samples = deque(samples, maxlen=1000)
        return state


class PersistentMemoryCore:
    """Manages persistent state with error handling and atomic writes"""
    
    STORAGE_FILE = "vanguard_link_memory.json"
    BACKUP_FILE = "vanguard_link_memory.backup.json"
    
    def __init__(self):
        self.state = SessionState()
        self._lock = threading.Lock()
        self.load()
    
    def load(self) -> bool:
        """Load state from disk. Returns True if successful."""
        if not os.path.exists(self.STORAGE_FILE):
            logger.info("No existing memory file. Starting fresh session.")
            return False
        
        try:
            with open(self.STORAGE_FILE, 'r') as f:
                data = json.load(f)
                self.state = SessionState.from_dict(data)
                logger.info(f"✅ Loaded persisted state: {self.state.sample_count} samples")
                return True
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error loading memory: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading memory: {e}")
            return False
    
    def save(self) -> bool:
        """Atomically save state to disk. Returns True if successful."""
        with self._lock:
            try:
                # Write to backup first
                backup_path = self.BACKUP_FILE
                with open(backup_path, 'w') as f:
                    json.dump(self.state.to_dict(), f, indent=2)
                
                # Atomic rename
                if os.path.exists(self.STORAGE_FILE):
                    os.remove(self.STORAGE_FILE)
                os.rename(backup_path, self.STORAGE_FILE)
                
                logger.debug(f"✅ State saved: {self.state.sample_count} samples persisted")
                return True
            except IOError as e:
                logger.error(f"IO error saving memory: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error saving memory: {e}")
                return False
    
    def get_state_summary(self) -> Dict:
        """Get human-readable state summary"""
        elapsed = time.time() - self.state.session_start
        return {
            "pilot": self.state.pilot_signature,
            "uptime_seconds": elapsed,
            "total_samples": self.state.sample_count,
            "buffered_samples": len(self.state.sensory_samples),
            "overflows": self.state.buffer_overflow_count,
        }


class SensoryEngine:
    """High-frequency sensory data capture with configurable sampling"""
    
    # 40kHz sampling frequency (25 microseconds per sample)
    SAMPLING_INTERVAL = 0.000025
    
    def __init__(self, core: PersistentMemoryCore):
        self.core = core
        self.cycles = 0
        self.ring_buffer = RingBuffer(capacity=4096)
        self._lock = threading.Lock()
        logger.info("🚀 SensoryEngine initialized")
    
    def capture(self, profile_name: str, hz: float, friction: float, temp: float) -> SensorReading:
        """Capture single sensory sample"""
        with self._lock:
            t = self.cycles * self.SAMPLING_INTERVAL
            self.cycles += 1
        
        # Generate synthetic wave (e.g., oscillating sensor)
        wave = math.sin(2 * math.pi * hz * t)
        
        # Push components to ring buffer
        self.ring_buffer.push(friction)
        self.ring_buffer.push(temp)
        self.ring_buffer.push(wave)
        
        # Create structured reading
        reading = SensorReading(
            timestamp=t,
            profile=profile_name,
            wave=wave,
            friction=friction,
            temp=temp,
        )
        
        # Add to session state
        self.core.state.sensory_samples.append(reading.to_dict())
        self.core.state.sample_count += 1
        self.core.state.last_update = time.time()
        
        return reading
    
    def sweep(self, 
              profile_name: str, 
              hz: float, 
              friction: float, 
              temp: float, 
              seconds: float,
              save_interval: float = 0.05) -> Dict[str, Any]:
        """
        Continuous sensory sweep for specified duration.
        
        Args:
            profile_name: Named sensor profile
            hz: Frequency in Hz
            friction: Friction coefficient (0.0-1.0)
            temp: Temperature in Celsius
            seconds: Duration of sweep
            save_interval: Save to disk every N seconds
        
        Returns:
            Sweep statistics
        """
        logger.info(f"🔍 Starting sweep: {profile_name} @ {hz}Hz for {seconds}s")
        
        start_time = time.time()
        last_save = start_time
        sample_count_start = self.core.state.sample_count
        readings: List[SensorReading] = []
        
        try:
            while time.time() - start_time < seconds:
                reading = self.capture(profile_name, hz, friction, temp)
                readings.append(reading)
                
                # Periodic saves
                current_time = time.time()
                if current_time - last_save >= save_interval:
                    self.core.save()
                    last_save = current_time
            
            # Final save
            self.core.state.ring_buffer_data = self.ring_buffer.get_all()
            self.core.save()
            
            elapsed = time.time() - start_time
            samples_captured = len(readings)
            sample_rate = samples_captured / elapsed if elapsed > 0 else 0
            
            stats = {
                "status": "complete",
                "profile": profile_name,
                "duration_seconds": elapsed,
                "samples_captured": samples_captured,
                "effective_rate_hz": sample_rate,
                "total_samples_in_session": self.core.state.sample_count,
                "buffer_size": self.ring_buffer.size(),
            }
            
            logger.info(f"✅ Sweep complete. {samples_captured} samples @ {sample_rate:.1f}Hz")
            return stats
            
        except KeyboardInterrupt:
            logger.warning("⚠️  Sweep interrupted by user")
            self.core.save()
            return {
                "status": "interrupted",
                "samples_captured": len(readings),
                "time_elapsed": time.time() - start_time,
            }
        except Exception as e:
            logger.error(f"❌ Sweep failed: {e}")
            self.core.save()
            return {"status": "error", "error": str(e)}
    
    def get_buffer_data(self, limit: Optional[int] = None) -> List[float]:
        """Get ring buffer contents, optionally limited"""
        data = self.ring_buffer.get_all()
        if limit:
            return data[-limit:]
        return data
    
    def reset(self):
        """Reset engine state"""
        with self._lock:
            self.cycles = 0
        self.ring_buffer.clear()
        logger.info("🔄 Engine reset")


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Initialize core systems
    core = PersistentMemoryCore()
    engine = SensoryEngine(core)
    
    # Show initial state
    print("📊 Initial State:")
    print(json.dumps(core.get_state_summary(), indent=2))
    print()
    
    # Run sweep
    stats = engine.sweep(
        profile_name="high_freq_sensor_stream",
        hz=12.0,
        friction=0.30,
        temp=37.0,
        seconds=0.1
    )
    
    # Show results
    print("📈 Sweep Results:")
    print(json.dumps(stats, indent=2))
    print()
    
    # Show buffer data (last 50 values)
    buffer_data = engine.get_buffer_data(limit=50)
    print(f"📦 Ring Buffer (last 50 of {len(engine.ring_buffer.get_all())}):")
    print(f"   {buffer_data}")
    print()
    
    # Show final state
    print("📊 Final State:")
    print(json.dumps(core.get_state_summary(), indent=2))
