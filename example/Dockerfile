# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /src

# Copy the entire source tree (needed for the replace directive)
COPY . .

# Build the collector using existing make target
RUN make build-collector

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /src/dist/otelcol-oxide /otelcol-oxide

EXPOSE 9091 8888

ENTRYPOINT ["/otelcol-oxide"]
CMD ["--config", "/etc/otelcol/config.yaml"]
