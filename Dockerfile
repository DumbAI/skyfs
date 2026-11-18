# skyfs as a container image, for running the daemon as a Kubernetes sidecar
# (README, "In Kubernetes"). The image holds the binary, FUSE's mount helper,
# CA certificates and a shell for wrapper scripts; nothing else.
#
#   docker build -t skyfs:dev .
#   docker run --rm --device /dev/fuse --cap-add SYS_ADMIN skyfs:dev mount ...
#
# Build and runtime stages share one Debian release, so the binary's glibc
# matches.
FROM rust:1.96-trixie AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p skyfs \
 && cp target/release/skyfs /usr/local/bin/skyfs

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/skyfs /usr/local/bin/skyfs
ENTRYPOINT ["skyfs"]
