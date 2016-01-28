#!/bin/bash
set -e
id=$RANDOM
./lambda/build.rb --test $id

./deploy/deploy.rb -c ~/dev/iot-startup/aws-keys/lambda-upload.credentials -f --test true -v
exit 0
