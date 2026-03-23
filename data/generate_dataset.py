"""
Generador de Dataset Sintético - 10M eventos de streaming
==========================================================
Genera un dataset realista de eventos de una plataforma de video
con las distribuciones propias del sector (Zipf para popularidad).

Uso:
    python data/generate_dataset.py --events 1000000 --output data/events.csv
    python data/generate_dataset.py --events 10000000 --output data/events_10M.csv
"""

import argparse
import csv
import random
import time
from datetime import datetime, timedelta

import numpy as np

# ── Configuración ──────────────────────────────────────────────────────────────
NUM_USERS = 100_000
NUM_VIDEOS = 10_000
NUM_IPS_POOL = 500_000
BOT_RATE = 0.02          # 2% de tráfico es bots
PREMIUM_RATE = 0.15      # 15% usuarios premium

EVENT_TYPES = [
    ("play", 0.45),
    ("pause", 0.12),
    ("resume", 0.10),
    ("search", 0.15),
    ("like", 0.08),
    ("dislike", 0.02),
    ("comment", 0.04),
    ("purchase", 0.02),
    ("quality_change", 0.02),
]

# Géneros de contenido
GENRES = ["accion", "drama", "comedia", "terror", "documental", "animacion", "romance", "sci-fi"]

VIDEO_CATALOG = [
    f"video_{i:05d}" for i in range(NUM_VIDEOS)
]


def zipf_sample(n: int, size: int, alpha: float = 1.2) -> np.ndarray:
    """
    Muestra con distribución Zipf (más realista que uniforme).
    Los videos más populares tienen muchísimas más vistas.
    """
    ranks = np.arange(1, n + 1)
    probs = 1.0 / (ranks ** alpha)
    probs /= probs.sum()
    return np.random.choice(n, size=size, p=probs)


def generate_events(num_events: int, output_path: str, chunk_size: int = 100_000):
    """Genera eventos y los escribe en CSV por chunks para eficiencia de memoria."""

    print(f"Generando {num_events:,} eventos...")
    print(f"Usuarios: {NUM_USERS:,} | Videos: {NUM_VIDEOS:,}")

    # Pre-generar usuarios y sus propiedades
    user_is_premium = {
        f"user_{i:06d}": (random.random() < PREMIUM_RATE)
        for i in range(NUM_USERS)
    }

    # Pool de IPs (bots usan IPs aleatorias, usuarios reales tienen pocas IPs)
    user_ips = {
        uid: [f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
              for _ in range(random.randint(1, 3))]
        for uid in user_is_premium
    }

    # Distribución de tipos de evento
    event_names = [e[0] for e in EVENT_TYPES]
    event_probs = [e[1] for e in EVENT_TYPES]

    start = datetime(2025, 1, 1)

    written = 0
    t0 = time.time()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "user_id", "event_type", "video_id",
                         "ip", "is_premium", "session_id", "quality"])

        while written < num_events:
            batch = min(chunk_size, num_events - written)

            # Samplear usuarios con distribución Zipf (los activos tienen más eventos)
            user_indices = zipf_sample(NUM_USERS, batch, alpha=1.5)
            video_indices = zipf_sample(NUM_VIDEOS, batch, alpha=1.2)
            etypes = np.random.choice(len(event_names), size=batch, p=event_probs)

            for j in range(batch):
                uid = f"user_{user_indices[j]:06d}"
                vid = VIDEO_CATALOG[video_indices[j]]

                # Bots usan IPs aleatorias
                if random.random() < BOT_RATE:
                    ip = f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
                else:
                    ip = random.choice(user_ips[uid])

                ts = start + timedelta(seconds=random.randint(0, 365 * 24 * 3600))
                session = f"sess_{random.randint(0, 9999999):07d}"
                quality = random.choice(["360p", "720p", "1080p", "4K"])

                writer.writerow([
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    uid,
                    event_names[etypes[j]],
                    vid,
                    ip,
                    user_is_premium[uid],
                    session,
                    quality,
                ])

            written += batch

            if written % (chunk_size * 5) == 0:
                elapsed = time.time() - t0
                rate = written / elapsed
                eta = (num_events - written) / rate
                print(f"  {written:>10,} / {num_events:,} ({written/num_events*100:.1f}%) "
                      f"| {rate:,.0f} eventos/s | ETA: {eta:.0f}s")

    elapsed = time.time() - t0
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\nDataset generado en {elapsed:.1f}s")
    print(f"Archivo: {output_path} ({size_mb:.1f} MB)")
    print(f"Throughput: {num_events/elapsed:,.0f} eventos/s")


if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser(description="Generador de dataset de streaming")
    parser.add_argument("--events", type=int, default=1_000_000,
                        help="Número de eventos a generar (default: 1M)")
    parser.add_argument("--output", type=str, default="data/events.csv",
                        help="Ruta del archivo de salida")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    generate_events(args.events, args.output)
