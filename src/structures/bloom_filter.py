"""
Bloom Filter - Detección probabilística de membresía
=====================================================
Paper original: Bloom, B. H. (1970). "Space/time trade-offs in hash coding
                with allowable errors". Communications of the ACM, 13(7), 422-426.

Uso en el proyecto:
    Detectar usuarios duplicados o bots en la plataforma de streaming.
    Un mismo usuario_id apareciendo desde múltiples IPs en poco tiempo
    es una señal de comportamiento sospechoso.

Complejidad:
    - Inserción:  O(k)  donde k = número de funciones hash
    - Consulta:   O(k)
    - Espacio:    O(m)  donde m = tamaño del bit array

Trade-offs:
    + Mucho más eficiente en memoria que un Hash Set
    + Velocidad constante independiente del tamaño
    - Falsos positivos posibles (nunca falsos negativos)
    - No se puede eliminar elementos (versión básica)
"""

import math
import mmh3
from bitarray import bitarray


class BloomFilter:
    """
    Implementación de Bloom Filter usando MurmurHash3.

    Parámetros:
        n (int): Número esperado de elementos
        fp_rate (float): Tasa de falsos positivos deseada (ej: 0.01 = 1%)
    """

    def __init__(self, n: int = 1_000_000, fp_rate: float = 0.01):
        self.n = n
        self.fp_rate = fp_rate

        # Tamaño óptimo del bit array: m = -(n * ln(p)) / (ln(2)^2)
        self.m = self._optimal_m(n, fp_rate)

        # Número óptimo de funciones hash: k = (m/n) * ln(2)
        self.k = self._optimal_k(self.m, n)

        self.bit_array = bitarray(self.m)
        self.bit_array.setall(0)
        self.count = 0

    @staticmethod
    def _optimal_m(n: int, p: float) -> int:
        return int(-n * math.log(p) / (math.log(2) ** 2))

    @staticmethod
    def _optimal_k(m: int, n: int) -> int:
        return max(1, int((m / n) * math.log(2)))

    def _hash_positions(self, item: str) -> list[int]:
        """Genera k posiciones en el bit array usando k seeds distintos."""
        return [mmh3.hash(item, seed=i) % self.m for i in range(self.k)]

    def add(self, item: str) -> None:
        """Agrega un elemento al filtro. O(k)"""
        for pos in self._hash_positions(item):
            self.bit_array[pos] = 1
        self.count += 1

    def contains(self, item: str) -> bool:
        """
        Verifica si un elemento podría estar en el filtro. O(k)
        Retorna True  -> probablemente presente (puede ser falso positivo)
        Retorna False -> definitivamente ausente
        """
        return all(self.bit_array[pos] for pos in self._hash_positions(item))

    def expected_fp_rate(self) -> float:
        """Calcula la tasa real de falsos positivos dado el número de inserciones."""
        return (1 - math.exp(-self.k * self.count / self.m)) ** self.k

    def fill_ratio(self) -> float:
        """Proporción de bits en 1. Útil para monitorear saturación del filtro."""
        return float(self.bit_array.count()) / len(self.bit_array)

    def memory_bytes(self) -> int:
        """Retorna el uso de memoria en bytes."""
        return self.m // 8

    def __repr__(self):
        return (
            f"BloomFilter(n={self.n}, fp_rate={self.fp_rate}, "
            f"m={self.m} bits, k={self.k} hashes, "
            f"elementos={self.count}, "
            f"memoria={self.memory_bytes() / 1024:.2f} KB)"
        )


class BotDetector:
    """
    Sistema de detección de bots usando Bloom Filter.
    Detecta el patrón: mismo user_id visto desde múltiples IPs.
    """

    def __init__(self, expected_users: int = 1_000_000):
        # Filtro para pares (user_id, ip) ya vistos
        self.seen_pairs = BloomFilter(n=expected_users * 5, fp_rate=0.001)
        # Conteo exacto de IPs por usuario (solo para los marcados como sospechosos)
        self.suspicious_users: dict[str, set] = {}
        self.THRESHOLD = 3  # Más de 3 IPs distintas = sospechoso

    def register_event(self, user_id: str, ip: str) -> bool:
        """
        Registra un evento y retorna True si el usuario es sospechoso.
        """
        pair_key = f"{user_id}:{ip}"

        if not self.seen_pairs.contains(pair_key):
            self.seen_pairs.add(pair_key)
            # Rastrear IPs únicas para este usuario
            if user_id not in self.suspicious_users:
                self.suspicious_users[user_id] = set()
            self.suspicious_users[user_id].add(ip)

        ip_count = len(self.suspicious_users.get(user_id, set()))
        return ip_count >= self.THRESHOLD

    def get_suspicious_users(self) -> list[tuple[str, int]]:
        """Retorna lista de (user_id, num_ips) ordenada por sospecha."""
        return sorted(
            [(uid, len(ips)) for uid, ips in self.suspicious_users.items()
             if len(ips) >= self.THRESHOLD],
            key=lambda x: x[1],
            reverse=True
        )
