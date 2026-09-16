# iron-feather serve image. Links the pinned prebuilt libduckdb from
# .deps/duckdb (populated by `just setup-duckdb`); DuckDB extensions
# (spatial/ducklake/quack) install at runtime, so the pod needs egress.
ARG RUST=1.98-bookworm
FROM rust:${RUST} AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY examples ./examples
COPY docs ./docs
COPY .deps/duckdb /duckdb-lib
ENV DUCKDB_LIB_DIR=/duckdb-lib
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release && \
    cp /src/target/release/iron-feather /iron-feather
# Bake extensions so pods serve without runtime egress to the extension repo.
RUN mkdir -p /duckdb-ext && HOME=/duckdb-ext \
    /duckdb-lib/duckdb :memory: "INSTALL spatial; INSTALL ducklake; INSTALL httpfs; INSTALL quack;"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /iron-feather /usr/local/bin/iron-feather
COPY --from=build /duckdb-lib/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=build /duckdb-ext/.duckdb /root/.duckdb
ENV LD_LIBRARY_PATH=/usr/local/lib
EXPOSE 3000 50051 9494
ENTRYPOINT ["iron-feather"]
