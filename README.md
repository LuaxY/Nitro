# Nitro

Distributed video encoder pipeline

___

# Deployment

## With Docker Compose

```
docker-compose up --build -d --scale encoder=2
```

# Configuration

Create .env file

```
AMQP=
REDIS=
REDIS_PASSWORD=
AWS_BUCKET=
AWS_REGION=
AWS_ENDPOINT=
AWS_ID=
AWS_SECRET
INFLUXDB=
INFLUXDB_TOKEN=
INFLUXDB_BUCKET=
INFLUXDB_ORG=
```

# Usage


```
Nitro - Distributed video encoder pipeline

Usage:
  nitro [flags]
  nitro [command]

Available Commands:
  encoder     Encode video chunks
  help        Help about any command
  merger      Merge video chunks
  packager    Create streaming manifests
  splitter    Split video into chunks
  watcher     Orchestrate the pipeline
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