#!/bin/bash

docker kill --signal=SIGINT nitro-encoder

while docker inspect -f '{{.State.Running}}' nitro-encoder; do
   sleep 1
done

echo "Done uploading, shutting down."

export NAME=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud --quiet compute instances delete "$NAME" --zone="$ZONE"