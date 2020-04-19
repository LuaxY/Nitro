#!/bin/bash

timeout 30s docker pull rg.fr-par.scw.cloud/nitro/nitro:latest
docker run --rm --name nitro-encoder --env-file /root/nitro.env rg.fr-par.scw.cloud/nitro/nitro:latest encoder

export NAME=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud --quiet compute instances delete "$NAME" --zone="$ZONE"

shutdown -h now