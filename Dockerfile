FROM golang:alpine AS builder
RUN apk add --no-cache gcc libc-dev
WORKDIR /nitro
COPY . .
RUN GOOS=linux go build -ldflags="-s -w" -o nitro ./cmd/nitro

FROM google/shaka-packager AS packager
RUN packager -version

FROM alpine AS final
RUN ulimit -l
RUN apk add --no-cache ffmpeg wget
WORKDIR /
COPY --from=builder /nitro/nitro nitro
COPY --from=packager /usr/bin/packager /bin/packager
ENTRYPOINT [ "/nitro" ]