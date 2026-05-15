FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "-s -w" \
    -o bin/ \
    ./cmd/...

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/bin/ /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/log-pilot-operator"]
