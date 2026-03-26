"""
Stream Processor - Integrador Central del Sistema
==================================================
Orquesta todas las estructuras de datos:
    1. PriorityQueue   → procesar eventos por prioridad
    2. BloomFilter     → detectar bots/duplicados
    3. Trie            → autocompletado de búsquedas
    4. LRUCache        → caché de contenido (1000 videos)
    5. MinHash+LSH     → recomendaciones personalizadas
    6. CountMinSketch  → Top-K videos en tiempo real
"""

import time
from typing import Optional
from system.priority_queue import PriorityQueue, Event, Priority
from system.lru_cache import LRUCache
from system.recommender import RecommendationEngine
from structures.bloom_filter import BotDetector
from structures.trie import SearchAutocomplete


class StreamProcessor:
    """
    Sistema central de procesamiento de streams de la plataforma de video.

    Simula una plataforma tipo Netflix/YouTube procesando eventos
    en tiempo real con múltiples estructuras de datos avanzadas.
    """

    def __init__(self, cache_size: int = 1000):
        # Cola de prioridad para eventos
        self.event_queue = PriorityQueue()

        # Caché de videos más accedidos
        self.content_cache = LRUCache(capacity=cache_size)

        # Motor de recomendaciones (LSH + Count-Min Sketch)
        self.recommender = RecommendationEngine(top_k_cache=cache_size)

        # Detección de bots
        self.bot_detector = BotDetector(expected_users=1_000_000)

        # Autocompletado
        self.autocomplete = SearchAutocomplete()

        # Métricas generales
        self._events_processed = 0
        self._bots_detected = set()
        self._start_time = time.time()

    def ingest_event(self, event_type: str, user_id: str,
                     video_id: Optional[str] = None,
                     ip: Optional[str] = None,
                     is_premium: bool = False,
                     payload: dict = None) -> None:
        """
        Punto de entrada principal: ingesta un evento al sistema.
        El evento se encola con su prioridad correspondiente.
        """
        event = Event.create(
            event_type=event_type,
            user_id=user_id,
            video_id=video_id,
            is_premium=is_premium,
            payload={**(payload or {}), "ip": ip}
        )
        self.event_queue.push(event)

    def process_next(self) -> Optional[dict]:
        """
        Procesa el siguiente evento de mayor prioridad. O(log n)
        Retorna un dict con los resultados del procesamiento.
        """
        event = self.event_queue.pop()
        if event is None:
            return None

        self._events_processed += 1
        result = {"event": event.event_type, "user": event.user_id, "actions": []}

        ip = event.payload.get("ip")

        # 1. Detección de bots
        if ip:
            is_suspicious = self.bot_detector.register_event(event.user_id, ip)
            if is_suspicious:
                self._bots_detected.add(event.user_id)
                result["actions"].append(f"ALERTA: usuario sospechoso detectado")

        # 2. Procesar según tipo de evento
        if event.event_type == "play" and event.video_id:
            result.update(self._handle_play(event))

        elif event.event_type == "search":
            query = event.payload.get("query", "")
            if query:
                suggestions = self.autocomplete.suggest(query, top_k=5)
                self.autocomplete.record_search(query)
                result["suggestions"] = suggestions

        elif event.event_type in ("purchase", "subscription"):
            result["actions"].append("Evento de pago procesado con MÁXIMA prioridad")

        return result

    def _handle_play(self, event: Event) -> dict:
        """Maneja evento de reproducción."""
        video_id = event.video_id
        result = {}

        # Verificar caché
        cached_content = self.content_cache.get(video_id)
        if cached_content:
            result["cache"] = "HIT"
        else:
            result["cache"] = "MISS"
            # Simular carga del contenido
            self.content_cache.put(video_id, {"video_id": video_id, "loaded_at": time.time()})

        # Actualizar recomendaciones
        self.recommender.process_watch(event.user_id, video_id, event.is_premium)

        return result

    def process_batch(self, n: int = 100) -> list[dict]:
        """Procesa los próximos n eventos en orden de prioridad."""
        results = []
        for _ in range(min(n, len(self.event_queue))):
            result = self.process_next()
            if result:
                results.append(result)
        return results

    def get_recommendations(self, user_id: str, n: int = 10) -> list[str]:
        """Obtiene recomendaciones para un usuario."""
        return self.recommender.get_recommendations(user_id, n)

    def get_trending(self, n: int = 10) -> list[tuple[str, int]]:
        """Obtiene los videos trending."""
        return self.recommender.get_trending(n)

    def autocomplete_search(self, prefix: str, top_k: int = 5) -> list[tuple[str, int]]:
        """Autocompletado con frecuencias."""
        return self.autocomplete.suggest_with_counts(prefix, top_k)

    def load_catalog(self, titles: list[str]) -> None:
        """Carga el catálogo de videos al motor de autocompletado."""
        self.autocomplete.load_catalog(titles)

    def system_stats(self) -> dict:
        elapsed = time.time() - self._start_time
        return {
            "eventos_procesados": self._events_processed,
            "eventos_en_cola": len(self.event_queue),
            "bots_detectados": len(self._bots_detected),
            "tiempo_operacion_s": round(elapsed, 2),
            "throughput_eps": round(self._events_processed / elapsed, 1) if elapsed > 0 else 0,
            "cola": self.event_queue.stats(),
            "cache_contenido": self.content_cache.stats(),
            "recomendaciones": self.recommender.stats(),
        }
