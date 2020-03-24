FROM golang:alpine AS builder
RUN apk add --no-cache gcc libc-dev
WORKDIR /transcode
COPY . .
RUN GOOS=linux go build -o transcode ./cmd/transcode

FROM google/shaka-packager AS packager
RUN packager -version

FROM alpine AS final
RUN apk add --no-cache ffmpeg wget
WORKDIR /
COPY --from=builder /transcode/transcode transcode
COPY --from=packager /usr/bin/packager /bin/packager
ENTRYPOINT [ "/transcode" ]