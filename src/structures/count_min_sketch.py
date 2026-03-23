"""
Count-Min Sketch - Estimación de Frecuencias en Streams (Top-K)
===============================================================
Paper original:
    Cormode, G., & Muthukrishnan, S. (2005). "An improved data stream summary:
    the count-min sketch and its applications". Journal of Algorithms, 55(1), 58-75.

Uso en el proyecto:
    Mantener en tiempo real los Top-K videos más vistos de la plataforma.
    Con 10 millones de eventos, un dict exacto consume demasiada memoria.
    Count-Min Sketch usa una fracción del espacio con error controlado.

Complejidad:
    - Update:  O(d)  donde d = número de filas (depth)
    - Query:   O(d)
    - Espacio: O(w * d) donde w = ancho, d = profundidad
                vs. O(n) de un dict exacto

    Error garantizado: P[error > ε * N] ≤ δ
    Donde: w = ⌈e/ε⌉, d = ⌈ln(1/δ)⌉

Trade-offs vs Counter exacto:
    + Memoria fija independiente del número de elementos distintos
    + Velocidad O(d) constante
    - Sobreestima frecuencias (nunca subestima)
    - No puede listar todos los elementos (solo consultar por clave)
"""

import math
import numpy as np
import mmh3
import heapq
from collections import defaultdict


class CountMinSketch:
    """
    Count-Min Sketch para estimación de frecuencias en streams.

    Garantiza: P[count(x) > real(x) + ε*N] ≤ δ
    """

    def __init__(self, epsilon: float = 0.001, delta: float = 0.01):
        """
        Args:
            epsilon: Error relativo máximo (ej: 0.001 = 0.1% del total)
            delta:   Probabilidad de exceder el error (ej: 0.01 = 1%)
        """
        self.epsilon = epsilon
        self.delta = delta

        # Dimensiones óptimas
        self.width = math.ceil(math.e / epsilon)   # w = e/ε ≈ 2718 para ε=0.001
        self.depth = math.ceil(math.log(1 / delta)) # d = ln(1/δ) ≈ 5 para δ=0.01

        self.table = np.zeros((self.depth, self.width), dtype=np.int64)
        self.total = 0

    def update(self, item: str, count: int = 1) -> None:
        """Agrega 'count' ocurrencias del item. O(d)"""
        self.total += count
        for row in range(self.depth):
            col = mmh3.hash(item, seed=row) % self.width
            self.table[row][col] += count

    def query(self, item: str) -> int:
        """Estima la frecuencia del item. O(d) — siempre >= real"""
        return int(min(
            self.table[row][mmh3.hash(item, seed=row) % self.width]
            for row in range(self.depth)
        ))

    def memory_bytes(self) -> int:
        return self.table.nbytes

    def __repr__(self):
        return (
            f"CountMinSketch(ε={self.epsilon}, δ={self.delta}, "
            f"tabla={self.depth}×{self.width}, "
            f"memoria={self.memory_bytes() / 1024:.1f} KB, "
            f"eventos={self.total:,})"
        )


class TopKTracker:
    """
    Top-K tracker en tiempo real combinando Count-Min Sketch + Min-Heap.

    Estrategia:
        - Count-Min Sketch estima la frecuencia de cada video (eficiente en memoria)
        - Min-Heap mantiene los K videos candidatos con mayor frecuencia estimada
        - Cuando llega un evento, se actualiza el sketch y el heap

    Complejidad por evento: O(d + log K)
    """

    def __init__(self, k: int = 1000, epsilon: float = 0.001, delta: float = 0.01):
        """
        Args:
            k: Número de top items a mantener (ej: top 1000 videos)
        """
        self.k = k
        self.cms = CountMinSketch(epsilon=epsilon, delta=delta)
        # Min-heap: [(frecuencia, video_id), ...]
        self._heap: list[tuple[int, str]] = []
        self._in_heap: set[str] = set()

    def add_event(self, item: str, count: int = 1) -> None:
        """
        Procesa un nuevo evento de visualización. O(d + log K)
        """
        self.cms.update(item, count)
        freq = self.cms.query(item)

        if item in self._in_heap:
            # Actualizar heap (lazy update: agregar nueva entrada, la vieja se ignora)
            heapq.heappush(self._heap, (freq, item))
        elif len(self._heap) < self.k:
            heapq.heappush(self._heap, (freq, item))
            self._in_heap.add(item)
        elif freq > self._heap[0][0]:
            # Desplazar el mínimo actual
            old_freq, old_item = heapq.heapreplace(self._heap, (freq, item))
            self._in_heap.discard(old_item)
            self._in_heap.add(item)

    def get_top_k(self) -> list[tuple[str, int]]:
        """
        Retorna los K items más frecuentes ordenados desc. O(K log K)
        """
        # Consolidar duplicados del lazy heap
        seen = {}
        for freq, item in self._heap:
            real_freq = self.cms.query(item)
            if item not in seen or seen[item] < real_freq:
                seen[item] = real_freq

        return sorted(seen.items(), key=lambda x: x[1], reverse=True)[:self.k]

    def get_top_n(self, n: int) -> list[tuple[str, int]]:
        """Retorna los N más frecuentes (N ≤ K)."""
        return self.get_top_k()[:n]

    def query_frequency(self, item: str) -> int:
        """Consulta la frecuencia estimada de un item específico."""
        return self.cms.query(item)

    def stats(self) -> dict:
        return {
            "total_eventos": self.cms.total,
            "items_en_heap": len(self._in_heap),
            "memoria_sketch_KB": round(self.cms.memory_bytes() / 1024, 2),
            "k": self.k,
        }
