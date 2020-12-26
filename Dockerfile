# Create the build container to compile the hello world program
FROM rust:1.48 as builder
RUN USER=root rustup target add x86_64-unknown-linux-musl
RUN USER=root cargo new --bin kafka-healthcheck
WORKDIR ./kafka-healthcheck
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs

ADD src/ ./src
RUN rm ./target/release/deps/kafka_healthcheck*
RUN cargo build --release --target x86_64-unknown-linux-musl && cp /kafka-healthcheck/target/release/kafka-healthcheck /bin

FROM debian:10.6
COPY --from=builder /bin/kafka-healthcheck /bin
CMD ["kafka-healthcheck"]
