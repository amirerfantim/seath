ARG GO_IMAGE=golang:1.25.12
ARG RUNTIME_IMAGE=alpine:3.22
FROM ${GO_IMAGE} AS builder
ARG GO_PROXY=https://proxy.golang.org,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=${GO_PROXY} go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w" -o /seath ./cmd

FROM ${RUNTIME_IMAGE}
ARG APK_REPOSITORIES
RUN if [ -n "$APK_REPOSITORIES" ]; then printf '%s\n' "$APK_REPOSITORIES" > /etc/apk/repositories; fi \
    && apk --no-cache add ca-certificates tzdata \
    && addgroup -S nonroot && adduser -S seath -G nonroot
LABEL org.opencontainers.image.title="Seath" \
      org.opencontainers.image.description="NATS to PostgreSQL recording console" \
      org.opencontainers.image.licenses="MIT"
COPY --from=builder /seath /usr/local/bin/seath
COPY LICENSE NOTICE /usr/share/licenses/seath/
USER seath
EXPOSE 8080
ENTRYPOINT ["seath"]
CMD ["serve"]
