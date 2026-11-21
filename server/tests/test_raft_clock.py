import pytest

from server.ray_app.consensus.clock import FakeClock


def test_fake_clock_only_moves_when_advanced():
    clock = FakeClock(start=10)
    assert clock.now() == 10
    clock.advance(2.5)
    assert clock.now() == 12.5


def test_fake_clock_rejects_backwards_time():
    clock = FakeClock()
    with pytest.raises(ValueError):
        clock.advance(-1)
