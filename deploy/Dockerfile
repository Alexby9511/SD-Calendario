# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build

WORKDIR /src

# Dependencies first, so editing code does not invalidate the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY pkg ./pkg
COPY server ./server

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/sdcald ./server/cmd/sdcald

FROM alpine:3.20

RUN adduser -D -u 10001 sdcal && mkdir -p /data && chown sdcal /data
COPY --from=build /out/sdcald /usr/local/bin/sdcald

USER sdcal
VOLUME ["/data"]
EXPOSE 7000 8080

ENTRYPOINT ["/usr/local/bin/sdcald"]
