"""
Cola de Procesamiento con Prioridades (Binary Heap)
====================================================
Uso en el proyecto:
    Los eventos del stream no se procesan en orden de llegada,
    sino por prioridad:
        Prioridad 0 (MÁXIMA) — Eventos de pago (compras, suscripciones)
        Prioridad 1 (ALTA)   — Usuarios Premium
        Prioridad 2 (MEDIA)  — Usuarios estándar con engagement alto
        Prioridad 3 (BAJA)   — Eventos de telemetría / logs

Complejidad del Heap:
    - Inserción: O(log n)
    - Extraer mínimo: O(log n)
    - Peek: O(1)
    - Construcción desde lista: O(n)
"""

import heapq
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional


class Priority(IntEnum):
    PAYMENT = 0       # Máxima: compras, suscripciones
    PREMIUM = 1       # Alta: usuarios premium
    STANDARD = 2      # Media: usuarios normales
    BACKGROUND = 3    # Baja: logs, telemetría


EVENT_TYPES = {
    "purchase": Priority.PAYMENT,
    "subscription": Priority.PAYMENT,
    "play": Priority.PREMIUM,          # se ajusta según tipo de usuario
    "pause": Priority.STANDARD,
    "resume": Priority.STANDARD,
    "quality_change": Priority.STANDARD,
    "search": Priority.STANDARD,
    "like": Priority.BACKGROUND,
    "dislike": Priority.BACKGROUND,
    "comment": Priority.BACKGROUND,
    "log": Priority.BACKGROUND,
}


@dataclass(order=True)
class Event:
    """Evento del stream con prioridad para el heap."""
    priority: int
    timestamp: float = field(compare=False)
    event_type: str = field(compare=False)
    user_id: str = field(compare=False)
    video_id: Optional[str] = field(compare=False, default=None)
    payload: dict = field(compare=False, default_factory=dict)
    is_premium: bool = field(compare=False, default=False)

    @classmethod
    def create(cls, event_type: str, user_id: str, video_id: Optional[str] = None,
               is_premium: bool = False, payload: dict = None) -> "Event":
        """Factory method para crear eventos con prioridad automática."""
        base_priority = EVENT_TYPES.get(event_type, Priority.STANDARD)

        # Usuarios premium suben un nivel de prioridad (mínimo PREMIUM)
        if is_premium and base_priority > Priority.PREMIUM:
            base_priority = Priority.PREMIUM

        return cls(
            priority=int(base_priority),
            timestamp=time.time(),
            event_type=event_type,
            user_id=user_id,
            video_id=video_id,
            payload=payload or {},
            is_premium=is_premium,
        )


class PriorityQueue:
    """
    Cola de prioridad basada en Binary Min-Heap.

    Para desempate en misma prioridad: FIFO (por timestamp).
    """

    def __init__(self):
        self._heap: list[Event] = []
        self._processed = 0
        self._total_wait_time = 0.0

    def push(self, event: Event) -> None:
        """Inserta un evento. O(log n)"""
        heapq.heappush(self._heap, event)

    def pop(self) -> Optional[Event]:
        """Extrae el evento de mayor prioridad. O(log n)"""
        if not self._heap:
            return None
        event = heapq.heappop(self._heap)
        wait = time.time() - event.timestamp
        self._total_wait_time += wait
        self._processed += 1
        return event

    def peek(self) -> Optional[Event]:
        """Ve el próximo evento sin extraerlo. O(1)"""
        return self._heap[0] if self._heap else None

    def push_batch(self, events: list[Event]) -> None:
        """Inserta múltiples eventos. O(n log n)"""
        for event in events:
            self.push(event)

    def process_all(self, handler_fn) -> int:
        """Procesa todos los eventos en orden de prioridad."""
        count = 0
        while self._heap:
            event = self.pop()
            handler_fn(event)
            count += 1
        return count

    @property
    def size(self) -> int:
        return len(self._heap)

    @property
    def avg_wait_ms(self) -> float:
        if self._processed == 0:
            return 0.0
        return (self._total_wait_time / self._processed) * 1000

    def stats(self) -> dict:
        priority_counts = {}
        for event in self._heap:
            p = Priority(event.priority).name
            priority_counts[p] = priority_counts.get(p, 0) + 1
        return {
            "en_cola": self.size,
            "procesados": self._processed,
            "espera_promedio_ms": round(self.avg_wait_ms, 3),
            "distribucion_prioridades": priority_counts,
        }

    def __len__(self):
        return self.size

    def __bool__(self):
        return bool(self._heap)
