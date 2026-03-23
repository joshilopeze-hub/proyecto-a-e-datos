"""
RealtimeStreamProcessor - Procesamiento en tiempo real con múltiples workers
=============================================================================

Implementa el patrón Producer-Consumer con múltiples threads:

                    ┌─────────────────────────────────────┐
                    │         PIPELINE EN TIEMPO REAL      │
                    │                                      │
    Generador       │  ┌──────────┐    ┌───────────────┐  │
    de eventos  ───►│  │ Producer │───►│  Queue        │  │
    (stream)        │  │ Thread   │    │  (buffer)     │  │
                    │  └──────────┘    └──────┬────────┘  │
                    │                         │            │
                    │               ┌─────────┼────────┐  │
                    │               ▼         ▼        ▼  │
                    │          ┌────────┐ ┌────────┐       │
                    │          │Worker 1│ │Worker 2│  ...  │
                    │          └───┬────┘ └───┬────┘       │
                    │              └──────────┘            │
                    │                   │                  │
                    │                   ▼                  │
                    │         ┌──────────────────┐         │
                    │         │  Estructuras de  │         │
                    │         │  datos (shared)  │         │
                    │         │  (thread-safe)   │         │
                    │         └──────────────────┘         │
                    └─────────────────────────────────────┘

Ventana deslizante (Sliding Window):
    Para detectar anomalías y bots se usa una ventana temporal.
    Solo los eventos de los últimos W segundos se consideran "activos".
"""

import time
import queue
import threading
from collections import deque, defaultdict
from typing import Optional, Callable
from dataclasses import dataclass, field

from .stream_simulator import EventStreamSimulator, RawEvent
from ..system.stream_processor import StreamProcessor


@dataclass
class StreamMetrics:
    """Métricas en tiempo real del pipeline."""
    events_received:  int = 0
    events_processed: int = 0
    events_dropped:   int = 0
    bots_flagged:     int = 0
    cache_hits:       int = 0
    cache_misses:     int = 0
    queue_size:       int = 0
    throughput_eps:   float = 0.0  # eventos/segundo procesados
    latency_ms:       float = 0.0  # latencia promedio de procesamiento
    window_events:    int = 0      # eventos en ventana deslizante actual

    def to_dict(self) -> dict:
        return {
            "recibidos":    self.events_received,
            "procesados":   self.events_processed,
            "descartados":  self.events_dropped,
            "bots":         self.bots_flagged,
            "cache_hits":   self.cache_hits,
            "cache_misses": self.cache_misses,
            "en_cola":      self.queue_size,
            "throughput":   f"{self.throughput_eps:,.0f} eps",
            "latencia_ms":  f"{self.latency_ms:.2f}",
            "ventana":      self.window_events,
        }


class SlidingWindow:
    """
    Ventana deslizante temporal para análisis de eventos recientes.
    Solo mantiene los eventos de los últimos `window_s` segundos.

    Uso: detección de patrones en ventana de tiempo (ej: 60 segundos).
    Complejidad: O(1) amortizado para add y clean.
    """

    def __init__(self, window_s: float = 60.0):
        self.window_s = window_s
        self._events: deque = deque()   # (timestamp, data)
        self._lock = threading.Lock()

    def add(self, data) -> None:
        now = time.time()
        with self._lock:
            self._events.append((now, data))
            self._clean(now)

    def get_current(self) -> list:
        """Retorna todos los eventos dentro de la ventana actual."""
        now = time.time()
        with self._lock:
            self._clean(now)
            return [d for _, d in self._events]

    def count(self) -> int:
        now = time.time()
        with self._lock:
            self._clean(now)
            return len(self._events)

    def _clean(self, now: float) -> None:
        """Elimina eventos fuera de la ventana. Llamar con lock adquirido."""
        cutoff = now - self.window_s
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()


class RealtimeStreamProcessor:
    """
    Procesador de streams en tiempo real con múltiples workers.

    Características:
    - Producer-Consumer con queue bounded (backpressure control)
    - N workers paralelos consumiendo la queue
    - Ventana deslizante de 60 segundos para análisis
    - Métricas en tiempo real actualizadas por los workers
    - Stop graceful con señal threading.Event
    """

    def __init__(self,
                 num_workers: int = 2,
                 queue_size: int = 10_000,
                 window_s: float = 60.0):
        """
        Args:
            num_workers: Número de threads consumidores (workers)
            queue_size:  Tamaño máximo del buffer (backpressure)
            window_s:    Tamaño de la ventana deslizante en segundos
        """
        self.num_workers = num_workers

        # Cola compartida Producer → Consumers (bounded = backpressure)
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)

        # Estructuras de datos del sistema
        self._processor = StreamProcessor(cache_size=1000)

        # Ventana deslizante
        self._window = SlidingWindow(window_s=window_s)

        # Métricas (compartidas entre threads con lock)
        self._metrics = StreamMetrics()
        self._metrics_lock = threading.Lock()
        self._latencies: deque = deque(maxlen=1000)  # últimas 1000 latencias

        # Control de threads
        self._stop_event = threading.Event()
        self._workers: list[threading.Thread] = []
        self._producer: Optional[threading.Thread] = None
        self._start_time: Optional[float] = None

        # Callbacks opcionales
        self._on_bot_detected: Optional[Callable] = None
        self._on_trending_update: Optional[Callable] = None

    def on_bot_detected(self, fn: Callable) -> None:
        """Registra callback para cuando se detecta un bot."""
        self._on_bot_detected = fn

    def on_trending_update(self, fn: Callable) -> None:
        """Registra callback para actualizaciones de trending."""
        self._on_trending_update = fn

    def _worker_loop(self, worker_id: int) -> None:
        """
        Loop principal de cada worker.
        Extrae eventos de la queue y los procesa con las estructuras de datos.
        """
        while not self._stop_event.is_set():
            try:
                # Timeout para poder chequear stop_event
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is None:
                # Señal de fin de stream
                self._queue.task_done()
                break

            raw: RawEvent = item
            t0 = time.perf_counter()

            # ── Procesar el evento con las estructuras de datos ──────────────
            self._processor.ingest_event(
                event_type = raw.event_type,
                user_id    = raw.user_id,
                video_id   = raw.video_id,
                ip         = raw.ip,
                is_premium = raw.is_premium,
                payload    = raw.payload,
            )

            # Procesar inmediatamente (tiempo real)
            result = self._processor.process_next()

            # Agregar a ventana deslizante
            self._window.add({
                "user_id":    raw.user_id,
                "event_type": raw.event_type,
                "video_id":   raw.video_id,
            })

            # ── Actualizar métricas ──────────────────────────────────────────
            latency_ms = (time.perf_counter() - t0) * 1000
            self._latencies.append(latency_ms)

            with self._metrics_lock:
                self._metrics.events_processed += 1
                self._metrics.window_events = self._window.count()

                if result:
                    if "ALERTA" in str(result.get("actions", [])):
                        self._metrics.bots_flagged += 1
                        if self._on_bot_detected:
                            self._on_bot_detected(raw.user_id, raw.ip)

                    cache_status = result.get("cache", "")
                    if cache_status == "HIT":
                        self._metrics.cache_hits += 1
                    elif cache_status == "MISS":
                        self._metrics.cache_misses += 1

            self._queue.task_done()

    def start(self, simulator: EventStreamSimulator,
              max_events: int = 0) -> None:
        """
        Inicia el pipeline en tiempo real.

        1. Lanza thread productor (simulador → queue)
        2. Lanza N threads workers (queue → estructuras de datos)

        Args:
            simulator:  EventStreamSimulator configurado
            max_events: Límite de eventos (0 = infinito)
        """
        self._start_time = time.time()
        self._stop_event.clear()

        print(f"Iniciando pipeline con {self.num_workers} workers...")
        print(f"Rate objetivo: {simulator.rate:,} eventos/seg")
        print(f"Ventana deslizante: {self._window.window_s}s")
        print("-" * 50)

        # Thread productor
        self._producer = simulator.to_queue(
            self._queue,
            max_events=max_events,
            stop_event=self._stop_event,
        )

        # Threads workers
        for i in range(self.num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                daemon=True,
                name=f"Worker-{i}",
            )
            t.start()
            self._workers.append(t)

    def stop(self) -> None:
        """Detiene el pipeline gracefully."""
        self._stop_event.set()
        # Señales de fin para cada worker
        for _ in self._workers:
            self._queue.put(None)
        for t in self._workers:
            t.join(timeout=5)
        self._workers.clear()

    def get_metrics(self) -> StreamMetrics:
        """Retorna snapshot de métricas actuales (thread-safe)."""
        elapsed = time.time() - (self._start_time or time.time())

        with self._metrics_lock:
            m = StreamMetrics(
                events_received  = self._metrics.events_received,
                events_processed = self._metrics.events_processed,
                events_dropped   = self._metrics.events_dropped,
                bots_flagged     = self._metrics.bots_flagged,
                cache_hits       = self._metrics.cache_hits,
                cache_misses     = self._metrics.cache_misses,
                queue_size       = self._queue.qsize(),
                throughput_eps   = self._metrics.events_processed / elapsed if elapsed > 0 else 0,
                latency_ms       = sum(self._latencies) / len(self._latencies) if self._latencies else 0,
                window_events    = self._window.count(),
            )
        return m

    def get_trending(self, n: int = 10) -> list:
        """Top-N videos trending en tiempo real."""
        return self._processor.get_trending(n)

    def get_recommendations(self, user_id: str, n: int = 5) -> list:
        """Recomendaciones para un usuario."""
        return self._processor.get_recommendations(user_id, n)

    def get_window_stats(self) -> dict:
        """Estadísticas de la ventana deslizante actual."""
        events = self._window.get_current()
        if not events:
            return {}

        type_counts: dict = defaultdict(int)
        video_counts: dict = defaultdict(int)

        for e in events:
            type_counts[e["event_type"]] += 1
            if e["video_id"]:
                video_counts[e["video_id"]] += 1

        top_video = max(video_counts, key=video_counts.get) if video_counts else "—"

        return {
            "total_eventos_ventana": len(events),
            "tipos": dict(sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            "video_hot_ahora": top_video,
            "video_hot_count": video_counts.get(top_video, 0),
        }

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set() and bool(self._workers)
