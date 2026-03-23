"""
Stream Simulator - Generador de eventos en tiempo real
=======================================================

Simula el flujo continuo de eventos de la plataforma de streaming.

Arquitectura de streaming real:
┌─────────────────────────────────────────────────────────────┐
│                    ARQUITECTURA REAL                        │
│                                                             │
│  Fuentes reales:  Kafka / Kinesis / RabbitMQ                │
│      │                                                      │
│      ▼                                                      │
│  [Producer Thread] ──► [Queue en memoria] ──► [Consumers]  │
│                                                             │
│  Esta implementación simula eso con:                        │
│  Python Generator  ──► queue.Queue         ──► Threads      │
└─────────────────────────────────────────────────────────────┘

Conceptos clave implementados:
    - Generator pattern: producción lazy de eventos (memoria O(1))
    - Backpressure: si la cola se llena, el productor espera
    - Rate limiting: controlar eventos/segundo simulados
    - Burst mode: simular picos de tráfico (ej: estreno de película)
"""

import time
import random
import threading
import queue
from datetime import datetime
from dataclasses import dataclass, field
from typing import Generator, Optional, Iterator
import numpy as np


# ── Catálogo del sistema ───────────────────────────────────────────────────────
NUM_USERS   = 10_000
NUM_VIDEOS  = 1_000
NUM_IPS     = 50_000

USER_IDS  = [f"user_{i:05d}" for i in range(NUM_USERS)]
VIDEO_IDS = [f"video_{i:04d}" for i in range(NUM_VIDEOS)]
IP_POOL   = [f"{random.randint(1,254)}.{random.randint(0,255)}."
             f"{random.randint(0,255)}.{random.randint(1,254)}"
             for _ in range(NUM_IPS)]

# Distribución realista de eventos (Zipf: pocos videos acaparan la mayoría)
VIDEO_WEIGHTS = np.array([1 / (i + 1) ** 1.2 for i in range(NUM_VIDEOS)])
VIDEO_WEIGHTS /= VIDEO_WEIGHTS.sum()

USER_WEIGHTS = np.array([1 / (i + 1) ** 1.5 for i in range(NUM_USERS)])
USER_WEIGHTS /= USER_WEIGHTS.sum()

EVENT_DIST = {
    "play":           0.40,
    "pause":          0.10,
    "resume":         0.09,
    "search":         0.15,
    "like":           0.08,
    "dislike":        0.02,
    "comment":        0.04,
    "quality_change": 0.03,
    "purchase":       0.02,
    "subscription":   0.01,
    "log":            0.06,
}

SEARCH_QUERIES = [
    "avengers", "avatar", "dark knight", "inception", "matrix",
    "interstellar", "parasite", "spider", "thor", "iron man",
    "black panther", "doctor strange", "guardians", "the", "2024",
]

PREMIUM_USERS = set(random.sample(USER_IDS, k=int(NUM_USERS * 0.15)))
BOT_USERS     = set(random.sample(USER_IDS, k=int(NUM_USERS * 0.02)))


@dataclass
class RawEvent:
    """Evento crudo del stream (antes de procesar)."""
    timestamp:  str
    user_id:    str
    event_type: str
    video_id:   Optional[str]
    ip:         str
    is_premium: bool
    session_id: str
    payload:    dict = field(default_factory=dict)

    def __repr__(self):
        return (f"[{self.timestamp}] {self.event_type:15s} "
                f"user={self.user_id} {'★' if self.is_premium else ' '} "
                f"video={self.video_id or '—':12s} ip={self.ip}")


class EventStreamSimulator:
    """
    Simula un stream infinito de eventos de la plataforma.

    Genera eventos con distribuciones realistas:
    - Popularidad de videos: distribución Zipf
    - Actividad de usuarios: distribución Zipf
    - Tipos de evento: pesos definidos
    - Bots: patrón de múltiples IPs

    Uso básico:
        sim = EventStreamSimulator(rate=1000)
        for event in sim.stream():
            process(event)

    Uso con burst:
        sim = EventStreamSimulator(rate=500)
        sim.trigger_burst(multiplier=10, duration_s=5)  # 5000 eps por 5 segs
    """

    def __init__(self, rate: int = 1000, seed: int = 42):
        """
        Args:
            rate: Eventos por segundo a generar (0 = máxima velocidad)
            seed: Semilla para reproducibilidad
        """
        self.rate = rate          # eventos/segundo (0 = sin límite)
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)

        self._total_generated = 0
        self._start_time: Optional[float] = None

        # Estado de burst
        self._burst_multiplier = 1.0
        self._burst_until: float = 0.0
        self._lock = threading.Lock()

        # Sesiones activas por usuario
        self._user_sessions: dict[str, str] = {}

    def _generate_one(self) -> RawEvent:
        """Genera un evento aleatorio con distribuciones realistas."""
        # Samplear usuario y video con distribución Zipf
        user_idx  = self._np_rng.choice(NUM_USERS,  p=USER_WEIGHTS)
        video_idx = self._np_rng.choice(NUM_VIDEOS, p=VIDEO_WEIGHTS)

        user_id  = USER_IDS[user_idx]
        video_id = VIDEO_IDS[video_idx]

        is_bot     = user_id in BOT_USERS
        is_premium = user_id in PREMIUM_USERS

        # Bots usan IPs aleatorias distintas cada vez (señal de detección)
        if is_bot:
            ip = self._rng.choice(IP_POOL)
        else:
            # Usuarios reales tienen 1-3 IPs fijas
            seed_ip = hash(user_id) % NUM_IPS
            ip_idx  = (seed_ip + self._rng.randint(0, 2)) % NUM_IPS
            ip = IP_POOL[ip_idx]

        # Tipo de evento
        event_type = self._rng.choices(
            list(EVENT_DIST.keys()),
            weights=list(EVENT_DIST.values()),
            k=1
        )[0]

        # Sesión (usuarios reales mantienen sesión, bots cambian seguido)
        if user_id not in self._user_sessions or is_bot:
            self._user_sessions[user_id] = f"sess_{self._rng.randint(0, 9_999_999):07d}"
        session_id = self._user_sessions[user_id]

        # Payload según tipo
        payload = {}
        if event_type == "search":
            payload["query"] = self._rng.choice(SEARCH_QUERIES)
            video_id = None
        elif event_type == "quality_change":
            payload["quality"] = self._rng.choice(["360p", "720p", "1080p", "4K"])
        elif event_type in ("purchase", "subscription"):
            payload["amount"] = round(self._rng.uniform(4.99, 19.99), 2)
            payload["plan"]   = self._rng.choice(["basic", "standard", "premium"])

        return RawEvent(
            timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            user_id    = user_id,
            event_type = event_type,
            video_id   = video_id if event_type not in ("search",) else None,
            ip         = ip,
            is_premium = is_premium,
            session_id = session_id,
            payload    = payload,
        )

    def stream(self, max_events: int = 0) -> Generator[RawEvent, None, None]:
        """
        Generador principal del stream. Produce eventos indefinidamente
        (o hasta max_events si se especifica).

        Implementa rate limiting con sleep adaptativo para mantener
        el throughput objetivo de self.rate eventos/segundo.

        Args:
            max_events: Límite de eventos (0 = infinito)
        """
        self._start_time = time.perf_counter()
        count = 0

        while max_events == 0 or count < max_events:
            t_start = time.perf_counter()

            # Calcular cuántos eventos generar en este tick
            with self._lock:
                in_burst = time.time() < self._burst_until
                multiplier = self._burst_multiplier if in_burst else 1.0

            effective_rate = int(self.rate * multiplier)

            if effective_rate == 0:
                # Sin rate limit: máxima velocidad
                event = self._generate_one()
                self._total_generated += 1
                count += 1
                yield event
            else:
                # Generar 1 evento y dormir el tiempo correspondiente
                event = self._generate_one()
                self._total_generated += 1
                count += 1
                yield event

                # Rate limiting: sleep adaptativo
                elapsed = time.perf_counter() - t_start
                target  = 1.0 / effective_rate
                sleep_t = target - elapsed
                if sleep_t > 0:
                    time.sleep(sleep_t)

    def trigger_burst(self, multiplier: float = 5.0, duration_s: float = 3.0) -> None:
        """
        Simula un pico de tráfico (ej: estreno de película, evento en vivo).
        El rate se multiplica por `multiplier` durante `duration_s` segundos.
        """
        with self._lock:
            self._burst_multiplier = multiplier
            self._burst_until = time.time() + duration_s
        print(f"  [BURST] Pico de tráfico: {multiplier}x por {duration_s}s")

    @property
    def current_rate(self) -> float:
        """Throughput real actual (eventos/segundo)."""
        if self._start_time is None:
            return 0.0
        elapsed = time.perf_counter() - self._start_time
        return self._total_generated / elapsed if elapsed > 0 else 0.0

    @property
    def total_generated(self) -> int:
        return self._total_generated

    def to_queue(self, q: queue.Queue, max_events: int = 0,
                 stop_event: threading.Event = None) -> threading.Thread:
        """
        Lanza un thread productor que pone eventos en una Queue.
        Patrón Producer-Consumer para procesamiento paralelo.

        Args:
            q:          Cola compartida con los consumidores
            max_events: Límite de eventos (0 = hasta stop_event)
            stop_event: threading.Event para detener el productor

        Returns:
            Thread del productor (ya iniciado)
        """
        def _produce():
            for event in self.stream(max_events):
                if stop_event and stop_event.is_set():
                    break
                q.put(event)          # Bloquea si la cola está llena (backpressure)
            q.put(None)               # Señal de fin de stream

        t = threading.Thread(target=_produce, daemon=True, name="StreamProducer")
        t.start()
        return t
