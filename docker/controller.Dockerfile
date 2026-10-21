# syntax=docker/dockerfile:1
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY bin/controller-${TARGETARCH} /controller
USER 65532:65532
EXPOSE 8080 9091
ENTRYPOINT ["/controller"]
