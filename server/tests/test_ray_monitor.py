from server.ray_app.ray_monitor import RayConnectionMonitor


class FakeRemoteFunction:
    def __init__(self, func):
        self.func = func

    def remote(self):
        return self.func()


class FakeRay:
    def __init__(self, initialized=True, probe_error=None):
        self.initialized = initialized
        self.probe_error = probe_error
        self.shutdown_count = 0

    def is_initialized(self):
        return self.initialized

    def remote(self, **_options):
        return lambda func: FakeRemoteFunction(func)

    def get(self, ref, timeout=None):
        if self.probe_error:
            error = self.probe_error
            self.probe_error = None
            raise error
        return ref

    def shutdown(self):
        self.shutdown_count += 1
        self.initialized = False


def test_healthy_ray_connection_does_not_restart_or_reattach():
    ray = FakeRay()
    attached = []
    connects = []
    monitor = RayConnectionMonitor(
        ray,
        "ray://head:10001",
        lambda *args, **kwargs: connects.append((args, kwargs)),
        lambda: attached.append(True),
    )

    assert monitor.check_once() is True
    assert ray.shutdown_count == 0
    assert connects == []
    assert attached == []


def test_lost_head_connection_reconnects_and_reattaches_raft():
    ray = FakeRay(probe_error=ConnectionError("head unavailable"))
    attached = []
    connects = []

    def connect(address, attempts):
        connects.append((address, attempts))
        ray.initialized = True

    monitor = RayConnectionMonitor(
        ray,
        "ray://head:10001",
        connect,
        lambda: attached.append(True),
    )

    assert monitor.check_once() is True
    assert ray.shutdown_count == 1
    assert connects == [("ray://head:10001", 1)]
    assert attached == [True]


def test_failed_reconnect_is_reported_and_retried_on_next_check():
    ray = FakeRay(probe_error=ConnectionError("head unavailable"))
    attempt_counts = []
    attached = []

    def connect(_address, attempts):
        attempt_counts.append(attempts)
        if len(attempt_counts) == 1:
            raise RuntimeError("head still unavailable")
        ray.initialized = True

    monitor = RayConnectionMonitor(
        ray,
        "ray://head:10001",
        connect,
        lambda: attached.append(True),
    )

    assert monitor.check_once() is False
    assert ray.is_initialized() is False
    assert monitor.check_once() is True
    assert ray.is_initialized() is True
    assert attempt_counts == [1, 1]
    assert attached == [True]
