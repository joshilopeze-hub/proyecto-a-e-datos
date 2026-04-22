"""
Módulo de Recomendación - Integra LSH + Count-Min Sketch + LRU Cache
"""

from structures.lsh_minhash import VideoRecommender
from structures.count_min_sketch import TopKTracker
from system.lru_cache import LRUCache


class RecommendationEngine:
    """
    Motor de recomendaciones que combina:
    - VideoRecommender (MinHash+LSH): usuarios similares → recomendaciones personalizadas
    - TopKTracker (Count-Min Sketch): top videos globales → para usuarios nuevos
    - LRUCache: caché de resultados de recomendación por usuario
    """

    def __init__(self, top_k_cache: int = 1000):
        self.video_rec = VideoRecommender(num_perm=128, bands=32)
        self.top_k = TopKTracker(k=top_k_cache)
        self.rec_cache = LRUCache(capacity=500)  # Caché de recomendaciones calculadas

    def process_watch(self, user_id: str, video_id: str, is_premium: bool = False) -> None:
        """Registra evento de visualización en todas las estructuras."""
        self.video_rec.add_watch_event(user_id, video_id)
        self.top_k.add_event(video_id)
        # Invalidar caché de recomendaciones del usuario (outdated)
        self.rec_cache.put(f"recs:{user_id}", None)

    def get_recommendations(self, user_id: str, n: int = 10) -> list[str]:
        """
        Obtiene recomendaciones para un usuario.
        Usa caché si el resultado es reciente.
        """
        cache_key = f"recs:{user_id}"
        cached = self.rec_cache.get(cache_key)
        if cached:
            return cached[:n]

        recs = self.video_rec.recommend(user_id, top_k=n)
        result = [vid for vid, _ in recs]

        if not result:
            # Fallback: top videos globales
            result = [vid for vid, _ in self.top_k.get_top_n(n)]

        self.rec_cache.put(cache_key, result)
        return result

    def get_trending(self, n: int = 20) -> list[tuple[str, int]]:
        """Retorna los N videos trending globalmente."""
        return self.top_k.get_top_n(n)

    def stats(self) -> dict:
        return {
            "recommender": self.video_rec.get_stats(),
            "top_k": self.top_k.stats(),
            "rec_cache": self.rec_cache.stats(),
        }

# Alias para compatibilidad con notebooks
Recommender = RecommendationEngine
