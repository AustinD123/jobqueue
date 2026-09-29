# ---- builder -------------------------------------------------------------
FROM ubuntu:24.04 AS builder

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential cmake pkg-config libsqlite3-dev nlohmann-json3-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --target broker worker -j"$(nproc)"

# ---- runtime -------------------------------------------------------------
FROM ubuntu:24.04

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libsqlite3-0 \
 && rm -rf /var/lib/apt/lists/*

# /data is created (and chowned) in the image so a fresh named volume
# mounted there inherits the ownership instead of being root-only.
RUN useradd --system --no-create-home jobqueue \
 && mkdir /data && chown jobqueue:jobqueue /data

COPY --from=builder /src/build/broker /src/build/worker /usr/local/bin/

USER jobqueue
ENV JQ_DB_PATH=/data/jobqueue.db
EXPOSE 9000
ENTRYPOINT ["broker"]
CMD ["9000"]
