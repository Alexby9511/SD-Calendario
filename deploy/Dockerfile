FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml .
COPY pkg ./pkg
COPY server ./server
COPY client ./client

RUN pip install --no-cache-dir -e .

EXPOSE 6379 8265 10001
CMD ["ray", "start", "--head", "--port=6379", "--ray-client-server-port=10001", "--dashboard-host=0.0.0.0", "--dashboard-port=8265", "--block"]
