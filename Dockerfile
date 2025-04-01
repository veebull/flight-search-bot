FROM rust:slim-bullseye as builder

WORKDIR /app
COPY . .

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    cargo build --release

FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y ca-certificates libssl1.1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/flight-checker /app/flight-checker

CMD ["/app/flight-checker"]
