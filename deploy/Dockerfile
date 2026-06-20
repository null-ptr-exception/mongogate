FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mongogate ./cmd/mongogate

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/mongogate .
COPY config.yaml .
ENTRYPOINT ["./mongogate"]
CMD ["--phase", "all"]
