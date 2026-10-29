# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY bin/converter-${TARGETARCH} /converter
USER 65532:65532
ENTRYPOINT ["/converter"]
