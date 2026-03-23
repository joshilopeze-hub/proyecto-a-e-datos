"""
MinHash + LSH (Locality Sensitive Hashing) - Recomendaciones por Similitud
==========================================================================
Papers originales:
    - Broder, A. Z. (1997). "On the resemblance and containment of documents"
    - Indyk, P., & Motwani, R. (1998). "Approximate nearest neighbors:
      towards removing the curse of dimensionality"

Uso en el proyecto:
    Sistema de recomendación de videos. Dos usuarios con patrones de
    co-watching similares recibirán recomendaciones parecidas.
    MinHash estima la similitud de Jaccard entre conjuntos de videos
    vistos, y LSH agrupa usuarios similares en "buckets" para búsqueda
    eficiente de vecinos sin comparar todos contra todos.

    Ejemplo:
        Usuario A vio: {vid_1, vid_3, vid_7, vid_12}
        Usuario B vio: {vid_1, vid_3, vid_9, vid_12}
        Jaccard = |A∩B| / |A∪B| = 3/5 = 0.6  → muy similares

Complejidad:
    - MinHash signature: O(k * |S|) donde k = num_hash_functions, |S| = tamaño del conjunto
    - LSH query:         O(k * L)  donde L = num_bands (sublineal vs. O(n) fuerza bruta)
    - Espacio:           O(n * k)  donde n = número de usuarios

Trade-offs:
    + Escala a millones de usuarios (imposible con similitud exacta)
    + Aproximación controlable (más bandas = más precisión, más tiempo)
    - Es probabilístico (puede perder algunos vecinos cercanos)
    - Falsos positivos posibles en los buckets
"""

import numpy as np
import random
from collections import defaultdict
from typing import Optional


class MinHash:
    """
    MinHash: Estimación de Similitud de Jaccard entre conjuntos.

    Transforma un conjunto en una firma compacta de k valores enteros.
    La probabilidad de que dos firmas coincidan en posición i = Jaccard(A, B).
    """

    def __init__(self, num_perm: int = 128, seed: int = 42):
        """
        Args:
            num_perm: Número de permutaciones (más = más preciso, más lento)
            seed: Semilla para reproducibilidad
        """
        self.num_perm = num_perm
        self._mersenne_prime = (1 << 61) - 1  # Primo de Mersenne M61
        self._max_hash = (1 << 32) - 1

        rng = np.random.RandomState(seed)
        self.a = rng.randint(1, self._mersenne_prime, num_perm, dtype=np.int64)
        self.b = rng.randint(0, self._mersenne_prime, num_perm, dtype=np.int64)

    def compute(self, items: set) -> np.ndarray:
        """
        Calcula la firma MinHash de un conjunto. O(k * |items|)

        Args:
            items: Conjunto de elementos (ej: set de video_ids vistos)
        Returns:
            Array de k valores enteros (la firma)
        """
        signature = np.full(self.num_perm, self._max_hash, dtype=np.int64)

        for item in items:
            # Hash universal: h(x) = (ax + b) mod p mod max_hash
            hv = hash(item) & self._max_hash
            hash_values = (self.a * hv + self.b) % self._mersenne_prime % self._max_hash
            signature = np.minimum(signature, hash_values)

        return signature

    @staticmethod
    def jaccard_from_signatures(sig_a: np.ndarray, sig_b: np.ndarray) -> float:
        """
        Estima la similitud de Jaccard comparando dos firmas. O(k)
        J(A,B) ≈ # posiciones donde sig_a == sig_b / k
        """
        return float(np.sum(sig_a == sig_b) / len(sig_a))

    @staticmethod
    def jaccard_exact(set_a: set, set_b: set) -> float:
        """Calcula la similitud de Jaccard exacta (para comparación)."""
        if not set_a and not set_b:
            return 1.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0.0


class LSH:
    """
    Locality Sensitive Hashing para búsqueda aproximada de vecinos.

    Agrupa firmas MinHash en "bandas". Si dos usuarios comparten
    al menos una banda idéntica, son candidatos a ser similares.

    Parámetro threshold:
        Con b bandas de r filas: P(colisión) ≈ 1 - (1 - s^r)^b
        donde s = similitud de Jaccard real
    """

    def __init__(self, num_perm: int = 128, bands: int = 32):
        """
        Args:
            num_perm: Debe coincidir con el MinHash usado
            bands: Número de bandas (más bandas = menor threshold de similitud)
        """
        assert num_perm % bands == 0, "num_perm debe ser divisible por bands"
        self.num_perm = num_perm
        self.bands = bands
        self.rows = num_perm // bands  # Filas por banda
        self.hash_tables: list[dict] = [defaultdict(list) for _ in range(bands)]
        self.signatures: dict[str, np.ndarray] = {}

    @property
    def threshold(self) -> float:
        """Threshold de similitud teórico: (1/b)^(1/r)"""
        return (1 / self.bands) ** (1 / self.rows)

    def index(self, user_id: str, signature: np.ndarray) -> None:
        """
        Indexa un usuario con su firma MinHash. O(b)
        """
        self.signatures[user_id] = signature
        for band_idx in range(self.bands):
            start = band_idx * self.rows
            end = start + self.rows
            band_key = tuple(signature[start:end])
            self.hash_tables[band_idx][band_key].append(user_id)

    def query(self, signature: np.ndarray, top_k: int = 10,
              exclude_id: Optional[str] = None) -> list[tuple[str, float]]:
        """
        Encuentra los top_k usuarios más similares. O(b * |bucket|)

        Returns:
            Lista de (user_id, similitud_estimada) ordenada desc.
        """
        candidates = set()

        for band_idx in range(self.bands):
            start = band_idx * self.rows
            end = start + self.rows
            band_key = tuple(signature[start:end])
            for candidate in self.hash_tables[band_idx].get(band_key, []):
                if candidate != exclude_id:
                    candidates.add(candidate)

        # Calcular similitud estimada para cada candidato
        results = []
        for candidate_id in candidates:
            sim = MinHash.jaccard_from_signatures(signature, self.signatures[candidate_id])
            results.append((candidate_id, sim))

        return sorted(results, key=lambda x: x[1], reverse=True)[:top_k]

    def remove(self, user_id: str) -> None:
        """Elimina un usuario del índice."""
        if user_id not in self.signatures:
            return
        sig = self.signatures.pop(user_id)
        for band_idx in range(self.bands):
            start = band_idx * self.rows
            end = start + self.rows
            band_key = tuple(sig[start:end])
            bucket = self.hash_tables[band_idx].get(band_key, [])
            if user_id in bucket:
                bucket.remove(user_id)

    def __len__(self):
        return len(self.signatures)

    def __repr__(self):
        return (
            f"LSH(usuarios={len(self)}, bandas={self.bands}, "
            f"filas/banda={self.rows}, threshold~{self.threshold:.2f})"
        )


class VideoRecommender:
    """
    Sistema de recomendación de videos usando MinHash + LSH.

    Flujo:
        1. Registrar videos vistos por cada usuario
        2. Al pedir recomendaciones: encontrar usuarios similares (LSH)
        3. Recomendar videos que los similares vieron y el usuario no
    """

    def __init__(self, num_perm: int = 128, bands: int = 32):
        self.minhash = MinHash(num_perm=num_perm)
        self.lsh = LSH(num_perm=num_perm, bands=bands)
        self.user_videos: dict[str, set] = {}     # user_id -> set de video_ids
        self.video_popularity: dict[str, int] = {}  # video_id -> conteo

    def add_watch_event(self, user_id: str, video_id: str) -> None:
        """Registra que un usuario vio un video."""
        if user_id not in self.user_videos:
            self.user_videos[user_id] = set()
        self.user_videos[user_id].add(video_id)
        self.video_popularity[video_id] = self.video_popularity.get(video_id, 0) + 1

        # Re-indexar el usuario con su firma actualizada
        signature = self.minhash.compute(self.user_videos[user_id])
        if user_id in self.lsh.signatures:
            self.lsh.remove(user_id)
        self.lsh.index(user_id, signature)

    def recommend(self, user_id: str, top_k: int = 10) -> list[tuple[str, float]]:
        """
        Genera recomendaciones para un usuario.

        Returns:
            Lista de (video_id, score) ordenada por relevancia
        """
        if user_id not in self.user_videos:
            # Usuario nuevo: recomendar por popularidad global
            return sorted(self.video_popularity.items(), key=lambda x: x[1], reverse=True)[:top_k]

        user_sig = self.lsh.signatures.get(user_id)
        if user_sig is None:
            return []

        # Encontrar usuarios similares
        similar_users = self.lsh.query(user_sig, top_k=20, exclude_id=user_id)

        # Agregar videos no vistos por el usuario, ponderados por similitud
        seen = self.user_videos[user_id]
        candidate_scores: dict[str, float] = {}

        for similar_user_id, similarity in similar_users:
            for video_id in self.user_videos.get(similar_user_id, set()):
                if video_id not in seen:
                    candidate_scores[video_id] = candidate_scores.get(video_id, 0) + similarity

        return sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    def get_stats(self) -> dict:
        return {
            "usuarios_indexados": len(self.lsh),
            "videos_en_catalogo": len(self.video_popularity),
            "threshold_similitud": round(self.lsh.threshold, 3),
        }
