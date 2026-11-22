import time
import uuid

import pytest

ray = pytest.importorskip("ray")
from ray.cluster_utils import Cluster

from server.ray_app.raft_actor import start_raft_cluster, stop_raft_cluster
from server.ray_app.raft_cluster import RaftCluster
from server.ray_app.raft_placement import (
    PLACEMENT_STRICT,
    head_node_id,
    plan_node_placement,
)
from server.ray_app.raft_supervisor import start_supervisor, stop_supervisor
from server.ray_app.raft_ticker import (
    DEFAULT_TICKER_PREFIX,
    start_tickers,
)


@pytest.fixture(scope="module")
def multi_node_ray():
    if ray.is_initialized():
        pytest.skip("another Ray runtime is already initialized")

    cluster = Cluster(
        initialize_head=True,
        head_node_args={"num_cpus": 1},
    )
    try:
        for _ in range(4):
            cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address, ignore_reinit_error=True)
        yield
    finally:
        if ray.is_initialized():
            ray.shutdown()
        cluster.shutdown()


def request_command():
    token = "01J00000000000000000000101"
    return {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": token,
        "type": "REQUEST",
        "uid": f"alice:{token}",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-21T00:00:00Z",
        "payload": {"title": "Placement"},
    }


def wait_until(predicate, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


def test_replicas_use_distinct_nodes_and_recreation_keeps_affinity(
    multi_node_ray, tmp_path
):
    namespace = f"sdcal-test-{uuid.uuid4().hex}"
    node_ids = [f"n{index}" for index in range(5)]
    db_paths = {
        node_id: str(tmp_path / f"{node_id}.db") for node_id in node_ids
    }
    assignments = plan_node_placement(
        node_ids, PLACEMENT_STRICT, ray.nodes()
    )
    actors = start_raft_cluster(
        node_ids,
        db_paths,
        namespace=namespace,
        node_assignments=assignments,
    )
    supervisor = start_supervisor(
        node_ids,
        db_paths,
        namespace=namespace,
        interval=0.1,
        node_assignments=assignments,
        supervisor_node_id=head_node_id(ray.nodes()),
    )
    tickers = start_tickers(
        actors,
        namespace=namespace,
        node_assignments=assignments,
    )
    raft = RaftCluster(actors, namespace=namespace)
    try:
        statuses = raft.statuses()
        assert len(statuses) == 5
        assert len({status["ray_node_id"] for status in statuses.values()}) == 5
        for node_id in node_ids:
            assert statuses[node_id]["ray_node_id"] == assignments[node_id]

        ticker_nodes = {
            node_id: ray.get(ticker.node_id.remote())
            for node_id, ticker in tickers.items()
        }
        assert ticker_nodes == assignments
        assert ray.get(supervisor.node_id.remote()) == head_node_id(ray.nodes())

        ray.kill(tickers["n1"])

        def ticker_recovered_on_same_node():
            try:
                ticker = ray.get_actor(
                    f"{DEFAULT_TICKER_PREFIX}n1", namespace=namespace
                )
                return (
                    ray.get(ticker.node_id.remote(), timeout=1.0)
                    == assignments["n1"]
                )
            except Exception:
                return False

        assert wait_until(ticker_recovered_on_same_node)

        assert wait_until(lambda: raft.leader() is not None)
        result = raft.propose(request_command())
        assert result["code"] == 200
        assert wait_until(
            lambda: raft.statuses().get("n2", {}).get("last_log_index") == 1
        )

        victim = "n2"
        target_node = assignments[victim]
        assert (tmp_path / f"{victim}.db").exists()
        ray.kill(actors[victim])

        def recreated_on_same_node():
            status = raft.statuses().get(victim)
            return status is not None and status["ray_node_id"] == target_node

        assert wait_until(recreated_on_same_node)
        recreated = raft.statuses()[victim]
        assert recreated["last_log_index"] == 1
        assert (tmp_path / f"{victim}.db").exists()
    finally:
        stop_supervisor(supervisor)
        for node_id in node_ids:
            try:
                ticker = ray.get_actor(
                    f"{DEFAULT_TICKER_PREFIX}{node_id}", namespace=namespace
                )
                ray.kill(ticker)
            except Exception:
                pass
        stop_raft_cluster(actors)
