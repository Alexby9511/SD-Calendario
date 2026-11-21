import pytest

from server.ray_app.consensus.transport import InMemoryNetwork, TransportError


def test_send_dispatches_to_handler():
    network = InMemoryNetwork()
    network.register("a", lambda message: ("reply", message))
    assert network.send("b", "a", "ping") == ("reply", "ping")
    assert set(network.nodes()) == {"a"}


def test_unknown_node_raises():
    network = InMemoryNetwork()
    with pytest.raises(TransportError):
        network.send("a", "missing", "ping")


def test_register_twice_raises():
    network = InMemoryNetwork()
    network.register("a", lambda message: None)
    with pytest.raises(ValueError):
        network.register("a", lambda message: None)


def test_cut_drops_both_directions_and_heal_restores():
    network = InMemoryNetwork()
    network.register("a", lambda message: "a")
    network.register("b", lambda message: "b")
    network.cut("a", "b")
    with pytest.raises(TransportError):
        network.send("a", "b", "ping")
    with pytest.raises(TransportError):
        network.send("b", "a", "ping")
    network.heal("a", "b")
    assert network.send("a", "b", "ping") == "b"


def test_unregister_removes_node():
    network = InMemoryNetwork()
    network.register("a", lambda message: None)
    network.unregister("a")
    with pytest.raises(TransportError):
        network.send("b", "a", "ping")
