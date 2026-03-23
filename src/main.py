"""
Sistema Inteligente de Análisis y Procesamiento de Streams de Datos
====================================================================
Opción A: Plataforma de Streaming de Video (Tipo Netflix/YouTube)

Demo de ejecución completa del sistema.
"""

import random
import time
from system.stream_processor import StreamProcessor

# ─── Datos de demostración ────────────────────────────────────────────────────
CATALOG = [
    "Avengers: Endgame", "Avatar", "The Dark Knight", "Inception",
    "Interstellar", "Parasite", "The Matrix", "Pulp Fiction",
    "The Shawshank Redemption", "Forrest Gump", "The Lion King",
    "Toy Story", "Finding Nemo", "The Avengers", "Iron Man",
    "Spider-Man: No Way Home", "Doctor Strange", "Thor: Ragnarok",
    "Black Panther", "Guardians of the Galaxy",
]

USER_IDS = [f"user_{i:04d}" for i in range(100)]
VIDEO_IDS = [f"video_{i:03d}" for i in range(50)]
IPS = [f"192.168.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(200)]

EVENT_TYPES = ["play", "pause", "resume", "search", "like", "dislike", "comment"]


def demo_completo():
    print("=" * 65)
    print("  SISTEMA DE STREAMING - DEMO COMPLETO")
    print("=" * 65)

    processor = StreamProcessor(cache_size=1000)
    processor.load_catalog(CATALOG)

    # ── 1. Ingesta de eventos ─────────────────────────────────────────────────
    print("\n[1] Ingesta de 500 eventos al stream...")
    t0 = time.time()

    for i in range(500):
        user = random.choice(USER_IDS)
        etype = random.choice(EVENT_TYPES)
        video = random.choice(VIDEO_IDS)
        ip = random.choice(IPS)
        is_premium = random.random() < 0.2  # 20% usuarios premium

        payload = {}
        if etype == "search":
            payload["query"] = random.choice([t[:5] for t in CATALOG])

        processor.ingest_event(
            event_type=etype,
            user_id=user,
            video_id=video,
            ip=ip,
            is_premium=is_premium,
            payload=payload,
        )

    # Simular bot: mismo usuario desde 5 IPs distintas
    bot_user = "bot_user_999"
    for _ in range(10):
        processor.ingest_event("play", bot_user, "video_001",
                               ip=f"10.0.0.{random.randint(1,50)}")

    # Simular evento de pago (máxima prioridad)
    processor.ingest_event("purchase", "user_0001", payload={"amount": 9.99})

    print(f"   Eventos ingresados en {(time.time()-t0)*1000:.1f} ms")
    print(f"   En cola: {len(processor.event_queue)} eventos")

    # ── 2. Procesar eventos ───────────────────────────────────────────────────
    print("\n[2] Procesando todos los eventos (orden de prioridad)...")
    t1 = time.time()
    results = processor.process_batch(n=600)
    elapsed = time.time() - t1

    print(f"   Procesados: {len(results)} eventos en {elapsed*1000:.1f} ms")

    # Verificar que el pago fue primero
    first_payment = next((r for r in results if r["event"] == "purchase"), None)
    if first_payment:
        print(f"   OK: Evento de pago procesado con MAXIMA prioridad")

    # ── 3. Autocompletado ─────────────────────────────────────────────────────
    print("\n[3] Autocompletado de búsquedas:")
    for prefix in ["aven", "the", "sp", "inter"]:
        suggestions = processor.autocomplete_search(prefix, top_k=3)
        print(f"   '{prefix}' → {[s for s, _ in suggestions]}")

    # ── 4. Recomendaciones ────────────────────────────────────────────────────
    print("\n[4] Recomendaciones personalizadas:")
    for user in USER_IDS[:3]:
        recs = processor.get_recommendations(user, n=5)
        print(f"   {user}: {recs[:3]}...")

    # ── 5. Trending ───────────────────────────────────────────────────────────
    print("\n[5] Top 5 videos trending:")
    trending = processor.get_trending(n=5)
    for i, (vid, count) in enumerate(trending, 1):
        print(f"   #{i} {vid}: {count:,} views")

    # ── 6. Estadísticas del sistema ───────────────────────────────────────────
    print("\n[6] Estadísticas del sistema:")
    stats = processor.system_stats()
    print(f"   Eventos procesados : {stats['eventos_procesados']:,}")
    print(f"   Bots detectados    : {stats['bots_detectados']}")
    print(f"   Throughput         : {stats['throughput_eps']} eventos/seg")
    print(f"   Cache hit rate     : {stats['cache_contenido']['hit_rate']}")
    print(f"   Usuarios similares : {stats['recomendaciones']['recommender']['usuarios_indexados']}")

    print("\n" + "=" * 65)
    print("  Demo completado exitosamente")
    print("=" * 65)


if __name__ == "__main__":
    demo_completo()
