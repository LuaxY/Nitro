FROM golang:alpine AS builder
RUN apk add --no-cache gcc libc-dev
WORKDIR /transcode
COPY . .
RUN GOOS=linux go build -o transcode ./cmd/transcode

FROM alpine AS final
RUN apk add --no-cache ffmpeg
WORKDIR /
COPY --from=builder /transcode/transcode transcode
ENTRYPOINT [ "/transcode" ]