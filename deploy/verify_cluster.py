#!/usr/bin/env python3
"""Verify container networking, Ray membership, and Raft replication."""

import json
import os
import subprocess
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


WORKERS = 5
APIS = 3
NETWORK = os.environ.get("SDCAL_NETWORK", "sdcal")


def fail(message: str) -> None:
    raise SystemExit(f"FALLO: {message}")


def docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        fail(result.stderr.strip() or f"docker {' '.join(args)} falló")
    return result.stdout


def api_request(port: int, path: str, token: str | None = None) -> dict:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(f"http://localhost:{port}{path}", headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response)
    except (HTTPError, URLError, TimeoutError) as exc:
        fail(f"API en puerto {port}, ruta {path}: {exc}")


def main() -> None:
    names = ["ray-head"]
    names.extend(f"ray-worker-{index}" for index in range(1, WORKERS + 1))
    names.extend(f"sdcal-api-{index}" for index in range(1, APIS + 1))
    containers = json.loads(docker("inspect", *names))

    for container in containers:
        name = container["Name"].lstrip("/")
        if not container["State"]["Running"]:
            fail(f"{name} no está ejecutándose")
        if NETWORK not in container["NetworkSettings"]["Networks"]:
            fail(f"{name} no está conectado a la red {NETWORK}")
    print(f"OK: {len(names)} contenedores activos en la red {NETWORK}")

    ray_status = docker(
        "exec", "ray-head", "ray", "status", "--address=127.0.0.1:6379"
    )
    for resource in ["sdcal-node-head"] + [
        f"sdcal-node-{index}" for index in range(1, WORKERS + 1)
    ]:
        if resource not in ray_status:
            fail(f"Ray no registra el recurso {resource}")
    print("OK: Ray registra el head y los cinco workers")

    api_environment = next(
        value
        for value in containers[WORKERS + 1]["Config"]["Env"]
        if value.startswith("SDCAL_API_TOKENS=")
    )
    users_to_tokens = json.loads(api_environment.split("=", 1)[1])
    token = next(
        token
        for configured in users_to_tokens.values()
        for token in ([configured] if isinstance(configured, str) else configured)
    )

    expected_replicas = {f"n{index}" for index in range(WORKERS)}
    leader_ids = set()
    for index in range(1, APIS + 1):
        port = 8080 + index
        ready = api_request(port, "/ready")
        if ready.get("status") != "ready":
            fail(f"API en puerto {port} no está lista: {ready}")

        status = api_request(port, "/v1/status", token)
        replicas = status.get("replicas", {})
        if status.get("middleware") != "ray":
            fail(f"API en puerto {port} no está usando Ray")
        if status.get("alive_replicas") != WORKERS:
            fail(f"API en puerto {port} no ve las {WORKERS} réplicas")
        if status.get("expected_replicas") != WORKERS:
            fail(f"API en puerto {port} espera un número inesperado de réplicas")
        if not status.get("quorum_available"):
            fail(f"API en puerto {port} no tiene quórum")
        if set(replicas) != expected_replicas:
            fail(
                f"API en puerto {port} no ve exactamente las réplicas "
                f"{expected_replicas}"
            )

        leader = status.get("leader_id")
        if (
            not leader
            or leader not in replicas
            or replicas[leader].get("role") != "leader"
            or sum(
                replica.get("role") == "leader" for replica in replicas.values()
            )
            != 1
        ):
            fail(f"API en puerto {port} no informa un líder válido")
        if any(
            replica.get("role") == "follower"
            and replica.get("leader_id") != leader
            for replica in replicas.values()
        ):
            fail(
                f"Las réplicas no reconocen al mismo líder desde la API {port}"
            )

        ray_node_ids = {
            replica.get("ray_node_id") for replica in replicas.values()
        }
        placement_keys = {
            replica.get("placement_key") for replica in replicas.values()
        }
        if len(ray_node_ids) != WORKERS or None in ray_node_ids:
            fail(
                "Las réplicas no están comunicándose desde nodos Ray distintos "
                f"(API {port})"
            )
        expected_placements = {
            f"resource:sdcal-node-{index}" for index in range(1, WORKERS + 1)
        }
        if placement_keys != expected_placements:
            fail(
                "Las réplicas no están colocadas en cada worker esperado "
                f"(API {port})"
            )

        leader_ids.add(leader)
        print(
            f"OK: API {port}, líder {leader}, {len(replicas)} réplicas "
            "en nodos distintos y quórum disponible"
        )

    if len(leader_ids) != 1:
        fail("las APIs no coinciden en el líder Raft")
    print("OK: head, workers, réplicas Raft y APIs están comunicándose")


if __name__ == "__main__":
    main()
