FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml .
COPY pkg ./pkg
COPY server ./server
COPY client ./client

COPY deploy/wheelhouse /wheelhouse
ARG PIP_ARGS="--timeout 60 --retries 10"
RUN pip install --no-cache-dir $PIP_ARGS --find-links=/wheelhouse -e . \
  && rm -rf /wheelhouse

COPY deploy/ray-head.sh /usr/local/bin/ray-head
RUN chmod +x /usr/local/bin/ray-head

EXPOSE 6379 8265 10001
ENTRYPOINT ["/usr/local/bin/ray-head"]
