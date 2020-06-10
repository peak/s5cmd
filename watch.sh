#!/usr/bin/env bash
set -e

ENV="${1}"
DIR_TO_BACKUP="${2:-/_OUTPUT_}"
DIR_TO_BACKUP="${DIR_TO_BACKUP%/}"
BUCKET_PREFIX="${3:-grafana-backup-storage}"
REGION="${4:-eu-central-1}"

if [[ -z "${ENV}" ]]
then
    script_usage
    exit 1
fi

echo "Start watching"
fswatch -1 --event Created -v -e "${DIR_TO_BACKUP}/.*" -i "${DIR_TO_BACKUP}/.*\\.tar\\.gz$" -0 "${DIR_TO_BACKUP}" | xargs -0 -n 1 -I {} echo "File {} changed"
echo "Stop watching... Copy will start in 10s."
echo "Launched command: s5cmd -r 1 --log verbose  --endpoint-url \"https://s3.${REGION}.amazonaws.com\"  cp  \"${DIR_TO_BACKUP}/\" \"s3://${BUCKET_PREFIX}-${ENV}/\""
sleep 10
s5cmd -r 1 --log verbose  --endpoint-url "https://s3.${REGION}.amazonaws.com"  cp  "${DIR_TO_BACKUP}/" "s3://${BUCKET_PREFIX}-${ENV}/"

# DESC: Usage help
# ARGS: None
# OUTS: None
function script_usage() {
    cat << EOF
Usage:
    watch.sh <ENV> [DIR_TO_BACKUP] [BUCKET_PREFIX] [REGION]                 Specifying an ENV is mandatory
    Destination bucket will be forge like this: s3://\${BUCKET_PREFIX}-\${ENV}/
    DIR_TO_BACKUP default "/_OUTPUT_"
    BUCKET_PREFIX default "grafana-backup-storage-"
    REGION default "eu-central-1"
EOF
}
