"""Watcher simple que reinicia `main.py` cuando detecta cambios en archivos .py.

Uso:
  pip install watchdog
  python scripts\watcher.py

Funciona en Windows y Unix. Usa `sys.executable` para lanzar el mismo intérprete.
"""
import os
import sys
import time
import threading
import subprocess

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except Exception:
    raise SystemExit("Dependencia faltante: instala 'watchdog' (pip install watchdog)")


class RestartHandler(FileSystemEventHandler):
    def __init__(self, restart_callback, patterns=(".py",)):
        super().__init__()
        self.restart = restart_callback
        self._debounce_timer = None
        self._lock = threading.Lock()
        self.patterns = patterns

    def on_any_event(self, event):
        if event.is_directory:
            return
        path = event.src_path
        if not any(path.endswith(p) for p in self.patterns):
            return
        with self._lock:
            if self._debounce_timer:
                self._debounce_timer.cancel()
            # debounce rapid sequence of events
            self._debounce_timer = threading.Timer(0.5, self.restart)
            self._debounce_timer.start()


def run_watcher():
    root = os.path.abspath(os.getcwd())
    python = sys.executable or "python"
    proc = None

    def start_proc():
        nonlocal proc
        if proc is not None and proc.poll() is None:
            return
        print(f"[watcher] Launching: {python} main.py (cwd={root})")
        proc = subprocess.Popen([python, "main.py"], cwd=root)
        print(f"[watcher] Started PID={proc.pid}")

    def stop_proc():
        nonlocal proc
        if proc is None:
            return
        if proc.poll() is None:
            print(f"[watcher] Terminating PID={proc.pid}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("[watcher] KILLing process")
                proc.kill()
        proc = None

    def restart():
        print("[watcher] Cambio detectado: reiniciando aplicación...")
        stop_proc()
        start_proc()

    event_handler = RestartHandler(restart)
    observer = Observer()
    observer.schedule(event_handler, path=root, recursive=True)
    observer.start()

    try:
        start_proc()
        while True:
            time.sleep(1)
            # si proceso terminó inesperadamente, reiniciar
            if proc is not None and proc.poll() is not None:
                print(f"[watcher] Proceso finalizó (code={proc.returncode}), reiniciando...")
                start_proc()
    except KeyboardInterrupt:
        print("[watcher] Interrumpido por usuario, deteniendo...")
    finally:
        try:
            observer.stop()
            observer.join()
        except Exception:
            pass
        stop_proc()


if __name__ == "__main__":
    run_watcher()
