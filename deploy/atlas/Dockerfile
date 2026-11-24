FROM rust:1.99.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip openssl cmake && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://sqlite.org/2026/sqlite-amalgamation-3510300.zip -o /tmp/sqlite.zip \
 && unzip -q /tmp/sqlite.zip -d /tmp \
 && cd /tmp/sqlite-amalgamation-3510300 \
 && test "$(openssl dgst -sha3-256 -r sqlite3.c | cut -d ' ' -f 1)" = 32d5424f97e0a7fc5ed2f6335afbb58be4e0298bd7117a34e39d345ff13d859e \
 && mkdir -p /opt/sqlite/lib /opt/sqlite/include \
 && cp sqlite3.h sqlite3ext.h /opt/sqlite/include/ \
 && cc -O2 -DSQLITE_THREADSAFE=1 -DSQLITE_OMIT_LOAD_EXTENSION=1 -c sqlite3.c -o sqlite3.o \
 && ar rcs /opt/sqlite/lib/libsqlite3.a sqlite3.o
ENV SQLITE3_STATIC=1 SQLITE3_NO_PKG_CONFIG=1 SQLITE3_LIB_DIR=/opt/sqlite/lib SQLITE3_INCLUDE_DIR=/opt/sqlite/include
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --locked --release \
 && ldd target/release/prime-finder > /tmp/runtime-libs \
 && ! grep -q libsqlite3 /tmp/runtime-libs

FROM debian:bookworm-slim
COPY --from=build /build/target/release/prime-finder /usr/local/bin/prime-finder
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/prime-finder"]
