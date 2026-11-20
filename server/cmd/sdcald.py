import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="SD Calendario sobre Ray")
    parser.add_argument("--ray-address", default=None)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-ray", action="store_true")
    args = parser.parse_args()

    if not args.no_ray:
        import ray
        ray.init(address=args.ray_address, ignore_reinit_error=True)

    from server.ray_app.api import app
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
