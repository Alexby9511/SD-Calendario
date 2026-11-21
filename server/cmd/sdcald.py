import argparse
from pathlib import Path

import uvicorn


def start_raft_cluster(
    replicas: int,
    data_dir: str,
    namespace: str,
) -> None:
    """Start (or attach to) the Raft replicas and wire them into the API.

    The replicas and their tickers are detached Ray actors: they survive the
    death of this API process, so several API instances can serve the same
    cluster.
    """
    from server.ray_app.api import set_cluster
    from server.ray_app.raft_actor import start_raft_cluster as start_replicas
    from server.ray_app.raft_cluster import RaftCluster
    from server.ray_app.raft_ticker import start_tickers

    node_ids = [f"n{index}" for index in range(replicas)]
    base = Path(data_dir)
    db_paths = {
        node_id: str(base / f"{node_id}.db") for node_id in node_ids
    }
    actors = start_replicas(node_ids, db_paths, namespace=namespace)
    start_tickers(actors, namespace=namespace)
    set_cluster(RaftCluster(actors, namespace=namespace))


def main() -> None:
    parser = argparse.ArgumentParser(description="SD Calendario sobre Ray")
    parser.add_argument("--ray-address", default=None)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-ray", action="store_true")
    parser.add_argument(
        "--raft-replicas",
        type=int,
        default=5,
        help="number of Raft replicas (0 disables the Raft cluster)",
    )
    parser.add_argument(
        "--raft-data-dir",
        default="data/raft",
        help="directory that holds one SQLite database per replica",
    )
    parser.add_argument(
        "--raft-namespace",
        default="sdcal",
        help="Ray namespace that holds the Raft actors",
    )
    args = parser.parse_args()

    if not args.no_ray:
        import ray

        ray.init(address=args.ray_address, ignore_reinit_error=True)
        if args.raft_replicas > 0:
            start_raft_cluster(
                args.raft_replicas,
                args.raft_data_dir,
                args.raft_namespace,
            )

    from server.ray_app.api import app

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
