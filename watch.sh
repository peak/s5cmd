#!/usr/bin/env bash
set -e

echo "Start watching"
fswatch -1 --event Created -v -e "/_OUTPUT_/.*" -i "/_OUTPUT_/.*\\.tar\\.gz$" -0 /_OUTPUT_ | xargs -0 -n 1 -I {} echo "File {} changed"
echo "Stop watching... Copy will start in 10s."
sleep 10
s5cmd -r 1 --log verbose  --endpoint-url "https://s3.eu-central-1.amazonaws.com"  cp  /_OUTPUT_/ "s3://grafana-backup-storage-${1}/"
