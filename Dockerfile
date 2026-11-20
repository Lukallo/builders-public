# Shopping Sessions: native Rust pipeline, no third-party crates.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer (AVX-512).
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN cargo build --release --locked --offline && cp target/release/ss /ss

FROM debian:bookworm-slim
COPY --from=build /ss /usr/local/bin/ss
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/usr/local/bin/ss"]
