#!/bin/bash
set -e
id=$RANDOM
KEYS=${AWS_KEYS:-"~/dev/iot-startup/aws-keys"}

./deploy/build.rb --test $id

./deploy/run.rb -c ${KEYS}/lambda-upload.credentials -f --test true -v
exit 0
