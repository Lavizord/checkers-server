FROM golang:1.23.6-alpine AS builder
WORKDIR /app/

# RUN echo "Files on /app/:" && ls -l /app/
# RUN echo "Files on .:" && ls -l .

COPY go.mod go.sum main.go /app/
COPY messages /app/messages
COPY models /app/models
COPY config /app/config
COPY redisdb /app/redisdb
# RUN echo "Files after copying shared code:" && ls -l /app/
COPY wsapi /app/wsapi
# RUN echo "Files after copying wsapi source code:" && ls -l /app/

RUN go mod tidy
RUN go mod download
RUN go build -o wsapi .

# RUN ls -l /app/wsapi
# RUN ls -la /app/
# RUN ls -l .

# Create the final image with only the binary
FROM alpine:latest
WORKDIR /root/
# Copy the built binary from the builder stage
COPY --from=builder /app/ .
# RUN ls -l .
# RUN ls -lh /root/

EXPOSE 8080

# Run the wsapi service
ENTRYPOINT ["./wsapi/checkers-server"]
