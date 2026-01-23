FROM rust:latest as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin server

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/server .
EXPOSE 8080
CMD ["./server"]
    