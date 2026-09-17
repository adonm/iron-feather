//! Regression checks exercise real DuckDB, HTTP responses and Flight wire
//! encoding. Fast tier: small deterministic edge-case fixtures (see
//! `common`). Larger real-stack coverage (MinIO → Cachey → servers) lives
//! in tests/stack/ and the 25M-row DuckLake fixture.
mod common;
mod file_list;
mod flight;
mod materialize;
mod ogc;
mod plan;
mod pool;
mod quack;
