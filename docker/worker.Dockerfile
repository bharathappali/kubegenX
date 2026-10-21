# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY bin/worker-${TARGETARCH} /worker
USER 65532:65532
ENTRYPOINT ["/worker"]
