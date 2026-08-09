# Multi-stage build: produces the single ./app binary used by all services.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app ./cmd/app

# Minimal runtime — the app is HTTP-only, no shell needed for normal operation.
FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=build /app /app
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["/app"]
