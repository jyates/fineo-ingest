#!/usr/bin/env bash
# Build the lambda file with some environment variables
set -e

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
home=`dirname "$this"`
home=`cd "home">/dev/null; pwd`

CONFIG_DIR=${CONFIG_DIR:-"${home}/config"}
CONFIG_FILE=${CONFIG_FILE:-"${CONFIG_DIR}/fineo-kinesis-firehose.properties"}

function help(){
  echo "Build the fineo-lamba project"
  echo "Options:"
  echo " -f <firehose url>"
  echo " -s <delivery stream name>"
  echo " -e <malformed records stream name>"
}

FIREHOSE_URL=${FIREHOSE_URL:-"https://firehose.us-east-1.amazonaws.com"}
STREAM_NAME=${STREAM_NAME:-"fineo-staged-records"}
MALFORMED_STREAM_NAME=${MALFORMED_STREAM_NAME:-"fineo-malformed-records"}


while getopts ":f:s:e:h" OPT; do
  case $OPT in
    f)
      FIREHOSE_URL=${OPT}
    ;;
    s)
      STREAM_NAME=${OPT}
    ;;
    e)
      MALFORMED_STREAM_NAME=${OPT}
    ;;
    h)
      help
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      help
      ;;
  esac
done

# Write the properties file with the expected values
if [ ! -d ${CONFIG_DIR} ]; then
  mkdir -p ${CONFIG_DIR};
fi

echo "fineo.firehose.url=${FIREHOSE_URL}" > ${CONFIG_FILE}
echo "fineo.firehose.stream.name=${STREAM_NAME}" >> ${CONFIG_FILE}
echo "fineo.firehose.stream.malformed=${MALFORMED_STREAM_NAME}" >> ${CONFIG_FILE}

echo "Wrote config file:"
cat ${CONFIG_FILE}

# run the maven build
mvn clean package

file=`ls ${home}/target/fineo-lambda-*.jar`
echo "Upload ${home}/target/$file} to AWS"