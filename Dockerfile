# Shopping Sessions: native Rust pipeline. The scorer builds this natively on linux/amd64.
FROM rust:1.97.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shop /usr/local/bin/shop
ENV HOME=/scratch
USER 1000:1000
CMD ["/usr/local/bin/shop"]
