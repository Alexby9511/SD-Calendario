"""Re-establish API connections when the Ray head restarts."""

import threading
from typing import Any, Callable, Optional

from .logging_config import get_logger


class RayConnectionMonitor:
    def __init__(
        self,
        ray_module: Any,
        address: Optional[str],
        connect: Callable[..., None],
        after_reconnect: Callable[[], None],
        interval: float = 2.0,
        timeout: float = 3.0,
    ) -> None:
        self.ray = ray_module
        self.address = address
        self.connect = connect
        self.after_reconnect = after_reconnect
        self.interval = interval
        self.timeout = timeout
        self.logger = get_logger("raft.connection")
        self._stop = threading.Event()
        self._needs_attach = False
        self._thread: Optional[threading.Thread] = None

    def check_once(self) -> bool:
        if not self.ray.is_initialized():
            return self._reconnect()
        try:
            probe = self.ray.remote(num_cpus=0)(lambda: True).remote()
            if not self.ray.get(probe, timeout=self.timeout):
                raise RuntimeError("Ray connection probe returned false")
        except Exception as exc:
            self.logger.warning("Ray connection probe failed reason=%r", exc)
            self._needs_attach = True
            return self._reconnect()
        if self._needs_attach:
            return self._attach_cluster()
        return True

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="ray-connection-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.check_once()
            except Exception:
                self.logger.exception("Ray connection monitor iteration failed")

    def _reconnect(self) -> bool:
        try:
            self.ray.shutdown()
        except Exception as exc:
            self.logger.warning("Ray shutdown before reconnect failed reason=%r", exc)
        try:
            self.connect(self.address, attempts=1)
        except RuntimeError as exc:
            self.logger.warning("Ray reconnect failed reason=%r", exc)
            return False
        self._needs_attach = True
        if not self._attach_cluster():
            return False
        self.logger.info("Ray connection restored")
        return True

    def _attach_cluster(self) -> bool:
        try:
            self.after_reconnect()
        except Exception:
            self.logger.exception("Raft cluster reattachment failed")
            return False
        self._needs_attach = False
        return True
