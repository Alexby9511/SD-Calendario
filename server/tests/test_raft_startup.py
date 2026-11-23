import sys

import pytest

from server.ray_app import raft_startup


class FakeActorAlreadyExists(Exception):
    pass


class FakeRay:
    def __init__(self, existing):
        self.existing = existing
        self.exceptions = type(
            "Exceptions", (), {"ActorAlreadyExistsError": FakeActorAlreadyExists}
        )

    def get_actor(self, name, namespace=None):
        assert name == "sdcal-test-actor"
        assert namespace == "sdcal-test"
        return self.existing


class FakeRemoteMethod:
    def __init__(self, actor):
        self.actor = actor

    def remote(self):
        self.actor.started += 1


class FakeExistingActor:
    def __init__(self):
        self.started = 0
        self.run = FakeRemoteMethod(self)


class FakeOptions:
    def __init__(self, error):
        self.error = error

    def remote(self, *args):
        raise self.error


class FakeActorClass:
    def __init__(self, error):
        self.error = error

    def options(self, **kwargs):
        assert kwargs["name"] == "sdcal-test-actor"
        assert kwargs["namespace"] == "sdcal-test"
        assert kwargs["get_if_exists"] is True
        return FakeOptions(self.error)


def test_create_named_actor_attaches_after_a_creation_race(monkeypatch):
    existing = FakeExistingActor()
    monkeypatch.setattr(raft_startup, "ray", FakeRay(existing))

    actor = raft_startup.create_named_actor(
        FakeActorClass(FakeActorAlreadyExists("already exists")),
        "arg",
        name="sdcal-test-actor",
        namespace="sdcal-test",
        start_method="run",
    )

    assert actor is existing
    assert existing.started == 1


def test_create_named_actor_does_not_hide_other_errors(monkeypatch):
    monkeypatch.setattr(
        raft_startup, "ray", FakeRay(FakeExistingActor())
    )

    with pytest.raises(RuntimeError, match="boom"):
        raft_startup.create_named_actor(
            FakeActorClass(RuntimeError("boom")),
            "arg",
            name="sdcal-test-actor",
            namespace="sdcal-test",
        )


def test_connect_ray_retries_a_transient_client_proxy_failure(monkeypatch):
    from server.cmd import sdcald

    class FakeRay:
        def __init__(self):
            self.attempts = 0

        def init(self, address=None, ignore_reinit_error=False):
            self.attempts += 1
            if self.attempts < 2:
                raise ConnectionAbortedError("client proxy failed")

        def is_initialized(self):
            return False

    fake = FakeRay()
    sleeps = []
    monkeypatch.setitem(sys.modules, "ray", fake)
    monkeypatch.setattr(sdcald.time, "sleep", sleeps.append)

    sdcald.connect_ray("ray://head:10001")

    assert fake.attempts == 2
    assert sleeps == [1]


def test_create_named_actor_retries_get_actor_before_fallback_creation(monkeypatch):
    class FlakyRay(FakeRay):
        def __init__(self):
            super().__init__(FakeExistingActor())
            self.get_attempts = 0

        def get_actor(self, name, namespace=None):
            self.get_attempts += 1
            if self.get_attempts < 2:
                raise RuntimeError("actor not registered yet")
            return self.existing

    class RaceOptions:
        def remote(self, *args):
            raise FakeActorAlreadyExists("already exists")

    class RaceActorClass:
        def options(self, **kwargs):
            return RaceOptions()

    fake = FlakyRay()
    monkeypatch.setattr(raft_startup, "ray", fake)
    monkeypatch.setattr(raft_startup.time, "sleep", lambda _: None)

    actor = raft_startup.create_named_actor(
        RaceActorClass(),
        name="sdcal-test-actor",
        namespace="sdcal-test",
    )

    assert actor is fake.existing
    assert fake.get_attempts == 2


def test_create_named_actor_waits_for_deferred_creation_error(monkeypatch):
    class DeferredRay(FakeRay):
        def __init__(self):
            super().__init__(FakeExistingActor())
            self.get_calls = 0

        def get(self, ref):
            self.get_calls += 1
            if self.get_calls == 1:
                raise FakeActorAlreadyExists("already exists")
            return None

    existing = FakeExistingActor()

    class DeferredOptions:
        def remote(self, *args):
            return existing

    class DeferredActorClass:
        def options(self, **kwargs):
            return DeferredOptions()

    fake = DeferredRay()
    fake.existing = existing
    monkeypatch.setattr(raft_startup, "ray", fake)
    monkeypatch.setattr(raft_startup.time, "sleep", lambda _: None)

    actor = raft_startup.create_named_actor(
        DeferredActorClass(),
        name="sdcal-test-actor",
        namespace="sdcal-test",
        start_method="run",
        wait_for_start=True,
    )

    assert actor is existing


def test_strict_placement_recovery_uses_existing_supervisor_targets(monkeypatch):
    from server.cmd import sdcald

    targets = {"n0": "resource:sdcal-node-0", "n1": "resource:sdcal-node-1"}

    class Ref:
        pass

    class ExistingSupervisor:
        class Placements:
            def remote(self):
                return Ref()

        placements = Placements()

    class FakeRay:
        def get_actor(self, name, namespace=None):
            assert name == "sdcal-supervisor"
            assert namespace == "sdcal-test"
            return ExistingSupervisor()

        def get(self, ref, timeout=None):
            assert isinstance(ref, Ref)
            assert timeout == 2.0
            return targets

    monkeypatch.setitem(sys.modules, "ray", FakeRay())

    assert sdcald._recover_strict_placements(["n0", "n1"], "sdcal-test") == targets


def test_strict_placement_recovery_fails_when_a_replica_target_is_unknown(monkeypatch):
    from server.cmd import sdcald

    class FakeRay:
        def get_actor(self, name, namespace=None):
            raise RuntimeError("actor unavailable")

    monkeypatch.setitem(sys.modules, "ray", FakeRay())

    with pytest.raises(RuntimeError, match="cannot safely recover targets"):
        sdcald._recover_strict_placements(["n0"], "sdcal-test")


def test_connect_ray_exhaustion_shuts_down_initialized_client(monkeypatch):
    from server.cmd import sdcald

    class FakeRay:
        def __init__(self):
            self.initialized = True
            self.shutdown_calls = 0

        def init(self, address=None, ignore_reinit_error=False):
            raise ConnectionAbortedError("client proxy failed")

        def is_initialized(self):
            return self.initialized

        def shutdown(self):
            self.shutdown_calls += 1
            self.initialized = False

    fake = FakeRay()
    monkeypatch.setitem(sys.modules, "ray", fake)
    monkeypatch.setattr(sdcald.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="could not connect"):
        sdcald.connect_ray("ray://head:10001", attempts=1)

    assert fake.shutdown_calls == 1
