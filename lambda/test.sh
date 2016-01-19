#!/bin/bash
set -e
./lambda/build.rb --test 1

./deploy/deploy.rb -c ~/dev/iot-startup/aws-keys/lambda-upload.credentials -f --lambda RawToAvro --test true -v
exit 0
