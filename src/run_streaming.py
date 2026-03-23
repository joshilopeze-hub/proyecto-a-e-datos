"""
run_streaming.py — Demo de procesamiento en tiempo real
========================================================

Ejecuta el sistema con stream continuo de eventos y dashboard en vivo.

Uso:
    cd src
    python run_streaming.py                        # 1000 eps, 30 segundos
    python run_streaming.py --rate 5000 --time 60  # 5000 eps, 1 minuto
    python run_streaming.py --rate 0               # máxima velocidad
"""

import argparse
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from streaming.stream_simulator import EventStreamSimulator
from streaming.realtime_processor import RealtimeStreamProcessor
from streaming.dashboard import LiveDashboard


def run(rate: int = 1000, duration_s: int = 30, workers: int = 2,
        burst: bool = True, show_dashboard: bool = True):

    print("\n" + "=" * 60)
    print("  SISTEMA DE STREAMING EN TIEMPO REAL")
    print(f"  Rate: {rate:,} eps | Duración: {duration_s}s | Workers: {workers}")
    print("=" * 60 + "\n")

    # Crear componentes
    simulator = EventStreamSimulator(rate=rate)
    processor = RealtimeStreamProcessor(
        num_workers=workers,
        queue_size=50_000,
        window_s=60.0,
    )

    # Callback de bots detectados
    bots_log = []
    def on_bot(user_id, ip):
        bots_log.append((user_id, ip))

    processor.on_bot_detected(on_bot)

    # Iniciar dashboard
    dashboard = None
    if show_dashboard:
        dashboard = LiveDashboard(processor, refresh_s=1.0)
        dashboard.start()

    # Iniciar pipeline
    processor.start(simulator, max_events=0)

    try:
        # Simular burst a mitad del tiempo
        if burst and duration_s >= 10:
            burst_at = duration_s // 2
            print(f"  Burst de trafico programado en t={burst_at}s")
        else:
            burst_at = None

        t_start = time.time()
        while time.time() - t_start < duration_s:
            elapsed = time.time() - t_start

            if burst_at and abs(elapsed - burst_at) < 0.5:
                simulator.trigger_burst(multiplier=5.0, duration_s=3.0)
                burst_at = None  # Solo una vez

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n  Deteniendo...")
    finally:
        processor.stop()
        if dashboard:
            dashboard.stop()

    # ── Resumen final ──────────────────────────────────────────────────────────
    print("\n\n" + "=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)

    m = processor.get_metrics()
    print(f"  Eventos procesados: {m.events_processed:,}")
    print(f"  Throughput:         {m.throughput_eps:,.0f} eventos/seg")
    print(f"  Latencia promedio:  {m.latency_ms:.3f} ms")
    print(f"  Cache hit rate:     {m.cache_hits / max(m.cache_hits + m.cache_misses, 1) * 100:.1f}%")
    print(f"  Bots detectados:    {m.bots_flagged}")

    print("\n  Top 10 videos trending:")
    for i, (vid, cnt) in enumerate(processor.get_trending(10), 1):
        print(f"    #{i:2d} {vid}  —  {cnt:,} views")

    if bots_log:
        print(f"\n  Bots detectados ({len(set(u for u,_ in bots_log))} usuarios únicos):")
        seen = set()
        for uid, ip in bots_log[:5]:
            if uid not in seen:
                print(f"    {uid} desde IP {ip}")
                seen.add(uid)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo de streaming en tiempo real")
    parser.add_argument("--rate",      type=int,  default=1000, help="Eventos/segundo (0=max)")
    parser.add_argument("--time",      type=int,  default=30,   help="Duración en segundos")
    parser.add_argument("--workers",   type=int,  default=2,    help="Número de workers")
    parser.add_argument("--no-burst",  action="store_true",     help="Sin pico de tráfico")
    parser.add_argument("--no-dash",   action="store_true",     help="Sin dashboard")
    args = parser.parse_args()

    run(
        rate=args.rate,
        duration_s=args.time,
        workers=args.workers,
        burst=not args.no_burst,
        show_dashboard=not args.no_dash,
    )
