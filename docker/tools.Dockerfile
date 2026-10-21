# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY bin/tools-${TARGETARCH} /tools
USER 65532:65532
ENTRYPOINT ["/tools"]
