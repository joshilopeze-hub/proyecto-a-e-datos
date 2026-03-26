"""
LiveDashboard - Dashboard en tiempo real en terminal
====================================================
Muestra métricas del sistema actualizándose cada segundo.
"""

import time
import threading
import os
from streaming.realtime_processor import RealtimeStreamProcessor


def _clear():
    os.system("cls" if os.name == "nt" else "clear")


class LiveDashboard:
    """
    Dashboard en terminal que se actualiza en tiempo real.
    Corre en su propio thread para no bloquear el procesamiento.
    """

    def __init__(self, processor: RealtimeStreamProcessor, refresh_s: float = 1.0):
        self.processor = processor
        self.refresh_s = refresh_s
        self._thread: threading.Thread = None
        self._stop = threading.Event()

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Dashboard")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._render()
            time.sleep(self.refresh_s)

    def _render(self) -> None:
        m = self.processor.get_metrics()
        w = self.processor.get_window_stats()
        trending = self.processor.get_trending(5)

        _clear()
        print("=" * 60)
        print("   SISTEMA DE STREAMING — DASHBOARD EN TIEMPO REAL")
        print("=" * 60)

        # Throughput
        bar_len = int(min(m.throughput_eps / 100, 40))
        bar = "█" * bar_len + "░" * (40 - bar_len)
        print(f"\n  Throughput:  [{bar}] {m.throughput_eps:,.0f} eps")
        print(f"  Latencia:    {m.latency_ms:.2f} ms por evento")
        print(f"  Cola buffer: {m.queue_size:,} eventos pendientes")

        print("\n─── Contadores ───────────────────────────────────────")
        print(f"  Procesados:  {m.events_processed:>10,}")
        print(f"  Cache hits:  {m.cache_hits:>10,}  ({_pct(m.cache_hits, m.cache_hits+m.cache_misses)})")
        print(f"  Cache miss:  {m.cache_misses:>10,}")
        print(f"  Bots detect: {m.bots_flagged:>10,}")

        if w:
            print("\n─── Ventana deslizante (último minuto) ───────────────")
            print(f"  Eventos:     {w.get('total_eventos_ventana', 0):,}")
            print(f"  Video hot:   {w.get('video_hot_ahora','—')} "
                  f"({w.get('video_hot_count',0)} plays)")
            tipos = w.get("tipos", {})
            for etype, cnt in list(tipos.items())[:4]:
                print(f"    {etype:<18} {cnt:>6,}")

        if trending:
            print("\n─── Top 5 Trending Ahora ─────────────────────────────")
            for i, (vid, cnt) in enumerate(trending, 1):
                bar_t = "▓" * min(int(cnt / 10), 20)
                print(f"  #{i} {vid:<14} {bar_t} {cnt:,}")

        print("\n" + "=" * 60)
        print("  [Ctrl+C para detener]")


def _pct(a: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{a/total*100:.1f}%"
