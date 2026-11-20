import pytest

ray = pytest.importorskip("ray")


@pytest.mark.skipif(False, reason="requires the local Ray runtime")
def test_ray_coordinator_actor():
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    from server.ray_app.coordinator import CoordinatorActor

    actor = CoordinatorActor.options(name="test-coordinator", get_if_exists=True).remote()
    status = ray.get(actor.status.remote())
    assert status["middleware"] == "ray"
    ray.shutdown()
