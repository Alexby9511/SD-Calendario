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
