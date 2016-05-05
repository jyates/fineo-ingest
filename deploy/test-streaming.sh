#!/bin/bash
set -e
id=$RANDOM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KEYS=${AWS_KEYS:-"${DIR}/../../aws-keys"}

./build-jar/lib/build-jar.rb stream-processing \
  --skip-tests --test $id --dynamo-ingest-limit-read 1 --dynamo-ingest-limit-write 1

./deploy-processing//lib/deploy-processing.rb stream-processing \
  -c ${KEYS}/lambda-upload.credentials -f --test true -v
exit 0
