FROM golang:1.23.6-alpine AS builder
WORKDIR /app/

#RUN echo "Files on /app/:" && ls -l /app/
#RUN echo "Files on .:" && ls -l .

COPY go.mod go.sum /app/
COPY messages /app/messages
COPY models /app/models
COPY config /app/config
COPY redisdb /app/redisdb
# RUN echo "Files after copying shared code:" && ls -l /app/
COPY ./roomworker /app/
# RUN echo "Files after copying roomworker source code:" && ls -l /app/

RUN go mod tidy
RUN go mod download
RUN go build -o roomworker .
# RUN ls -l /app/
# RUN ls -l .

# Create the final image with only the binary
FROM alpine:latest
WORKDIR /root/
# Copy the built binary from the builder stage
COPY --from=builder /app/ .
# RUN ls -l .
# RUN ls -lh /root/

# Run the roomworker service
CMD ["./roomworker"]
