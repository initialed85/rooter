# The binary is cross-compiled by build-tag-and-push.sh rather than built in a
# golang stage, matching the house pattern (camry/fred): the build context then
# carries only the artifact that was tested, and the image needs no toolchain.
# .dockerignore whitelists tmp/rooter out of an otherwise excluded tmp/.
FROM ubuntu:22.04

ENV TZ Australia/Perth
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends tzdata curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN dpkg-reconfigure -f noninteractive tzdata

WORKDIR /srv/

COPY ./tmp/rooter /srv/bin/rooter

EXPOSE 8089

# Default bind is loopback, matching the binary's own default: a bare
# `docker run` of this image is reachable only from its own network namespace.
# The k8s deployment overrides CMD with -listen 0.0.0.0:8089 so the pod IP
# serves traffic; kubelet ignores this HEALTHCHECK and uses its own probes.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["curl", "-fsS", "http://127.0.0.1:8089/rooter/healthz"]

ENTRYPOINT ["/srv/bin/rooter"]
CMD ["-listen", "127.0.0.1:8089"]
