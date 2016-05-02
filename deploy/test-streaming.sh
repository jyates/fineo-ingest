#!/bin/bash
set -e
id=$RANDOM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KEYS=${AWS_KEYS:-"${DIR}/../../aws-keys"}

./build-jar/lib/build-jar.rb stream-processing \
  --skip-tests --test $id --dynamo-read-limit 1 --dynamo-write-limit 1

./deploy-lambda/lib/deploy-lambda.rb stream-processing \
  -c ${KEYS}/lambda-upload.credentials -f --test true -v
exit 0
