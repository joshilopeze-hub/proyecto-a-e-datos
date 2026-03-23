"""
LRU Cache - Caché Inteligente de Contenido
==========================================
Uso en el proyecto:
    Mantener en caché los 1,000 videos más accedidos recientemente.
    Cuando llega una solicitud de stream, primero se busca en caché
    (hit → respuesta inmediata) antes de ir al almacenamiento principal
    (miss → latencia mayor).

Implementación:
    Combina un HashMap + Doubly Linked List para O(1) en todas las ops:
    - HashMap: acceso directo al nodo por key
    - Linked List: orden temporal (MRU al frente, LRU al final)

Complejidad:
    - Get:    O(1)
    - Put:    O(1)
    - Evict:  O(1) — siempre el nodo de la cola (LRU)
    - Espacio: O(capacity)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Any
import time


@dataclass
class CacheNode:
    key: str
    value: Any
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    prev: Optional[CacheNode] = field(default=None, repr=False)
    next: Optional[CacheNode] = field(default=None, repr=False)


class LRUCache:
    """
    LRU Cache con HashMap + Doubly Linked List.
    Todas las operaciones son O(1).
    """

    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self._cache: dict[str, CacheNode] = {}

        # Sentinel nodes (simplifica lógica de bordes)
        self._head = CacheNode(key="__HEAD__", value=None)  # MRU end
        self._tail = CacheNode(key="__TAIL__", value=None)  # LRU end
        self._head.next = self._tail
        self._tail.prev = self._head

        # Métricas
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Obtiene un valor del caché. O(1)
        Si existe, lo mueve al frente (MRU).
        """
        if key not in self._cache:
            self._misses += 1
            return None

        node = self._cache[key]
        node.access_count += 1
        node.last_access = time.time()
        self._move_to_front(node)
        self._hits += 1
        return node.value

    def put(self, key: str, value: Any) -> Optional[str]:
        """
        Inserta o actualiza un valor. O(1)
        Retorna la key eviccionada (si hubo) o None.
        """
        evicted_key = None

        if key in self._cache:
            node = self._cache[key]
            node.value = value
            node.access_count += 1
            node.last_access = time.time()
            self._move_to_front(node)
        else:
            if len(self._cache) >= self.capacity:
                evicted_key = self._evict_lru()
            node = CacheNode(key=key, value=value)
            self._cache[key] = node
            self._add_to_front(node)

        return evicted_key

    def contains(self, key: str) -> bool:
        """Verifica si una key está en caché sin actualizar el orden."""
        return key in self._cache

    def _add_to_front(self, node: CacheNode) -> None:
        """Agrega nodo justo después del head (posición MRU)."""
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node
        self._head.next = node

    def _remove_node(self, node: CacheNode) -> None:
        """Desconecta un nodo de la lista."""
        node.prev.next = node.next
        node.next.prev = node.prev

    def _move_to_front(self, node: CacheNode) -> None:
        """Mueve un nodo existente al frente (MRU)."""
        self._remove_node(node)
        self._add_to_front(node)

    def _evict_lru(self) -> Optional[str]:
        """Elimina el nodo LRU (justo antes del tail). O(1)"""
        lru_node = self._tail.prev
        if lru_node is self._head:
            return None
        self._remove_node(lru_node)
        del self._cache[lru_node.key]
        self._evictions += 1
        return lru_node.key

    def get_lru_order(self) -> list[str]:
        """Retorna las keys en orden MRU → LRU."""
        result = []
        current = self._head.next
        while current is not self._tail:
            result.append(current.key)
            current = current.next
        return result

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    @property
    def size(self) -> int:
        return len(self._cache)

    def stats(self) -> dict:
        return {
            "capacity": self.capacity,
            "size": self.size,
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "hit_rate": f"{self.hit_rate:.2%}",
        }

    def __len__(self):
        return self.size

    def __repr__(self):
        return f"LRUCache(size={self.size}/{self.capacity}, hit_rate={self.hit_rate:.2%})"
