#!/bin/bash
set -e
id=$RANDOM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KEYS=${AWS_KEYS:-"${DIR}/../../aws-keys"}

./deploy/build.rb --skip-tests --test $id

./deploy/run.rb -c ${KEYS}/lambda-upload.credentials -f --test true -v
exit 0
