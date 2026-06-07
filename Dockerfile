FROM debian:bookworm AS deb-builder

WORKDIR /src

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    debhelper \
    dpkg-dev \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3-venv \
    python3-wheel \
    && rm -rf /var/lib/apt/lists/*

COPY . /src

RUN dpkg-buildpackage -us -uc -b \
    && mkdir -p /dist \
    && cp /stopan_*.deb /dist/


FROM debian:bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    systemd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deb-builder /dist/stopan_*.deb /tmp/

RUN apt-get update \
    && apt-get install -y --no-install-recommends /tmp/stopan_*.deb \
    && rm -f /tmp/stopan_*.deb \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 50051

CMD ["stopan", "node"]
