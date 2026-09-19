# lakewing serve image. Go + CGO (duckdb-go preview bundles the DuckDB 2.0
# static lib); DuckDB extensions (spatial/ducklake) install at runtime, so
# the pod needs egress unless they are baked.
ARG GO=1.25-bookworm
FROM golang:${GO} AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /lakewing ./cmd/lakewing

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /lakewing /usr/local/bin/lakewing
EXPOSE 3000 50051
ENTRYPOINT ["lakewing"]
