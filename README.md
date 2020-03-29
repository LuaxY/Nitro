# Transcoder V2

```
docker-compose up --build -d
docker-compose scale encoder=3
```

Publish request in `splitter.request` queue

```yaml
uid: example
input: inputs/mux-video-intro.mp4
chunkTime: 5
params:
  - profile: high
    codec: h264
    width: 1280
    height: 720
    crf: 20
    bitrate: 2500k
    maxrate: 3000k
    minrate: 1000k
    bufsize: 2500k
    preset: faster
    extraArgs:
      - -pix_fmt
      - yuv420p
  - profile: high
    codec: h264
    width: 960
    height: 540
    crf: 20
    bitrate: 1250k
    maxrate: 1600k
    minrate: 1000k
    bufsize: 1250k
    preset: faster
    extraArgs:
      - -pix_fmt
      - yuv420p
```