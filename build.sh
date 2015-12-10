#!/usr/bin/env bash
# Build the lambda file with some environment variables
set -x
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
home=`cd "$home">/dev/null; pwd`

CONFIG_DIR=${CONFIG_DIR:-"${home}/config"}
CONFIG_FILE=${CONFIG_FILE:-"${CONFIG_DIR}/fineo-lambda.properties"}

function help(){
  echo "Build the fineo-lamba project"
  echo "Options:"
  echo " -k <kinesis endpoint>. Not a URL, just an address - we always connect over TLS"
  echo " -p <parsed stream name>"
  echo
  echo " -f <firehose url>"
  echo " -m <malformed records stream name>"
  echo
  echo " -h show this help"
}

KINESIS_URL=${KINESIS_URL:-"kinesis.us-east-1.amazonaws.com"}
FIREHOSE_URL=${FIREHOSE_URL:-"https://firehose.us-east-1.amazonaws.com"}
PARSED_STREAM=${PARSED_STREAM:-"fineo-parsed-records"}
MALFORMED_STREAM=${MALFORMED_STREAM:-"fineo-malformed-records"}


while getopts ":f:k:p:m:h" OPT; do
  case $OPT in
    f)
      FIREHOSE_URL=${OPT}
    ;;
    p)
      PARSED_STREAM=${OPT}
    ;;
    m)
      MALFORMED_STREAM=${OPT}
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

function add(){
  echo "fineo.${1}=${2}" >> ${CONFIG_FILE}

}
# Kinesis properties
add "kinesis.url" "${KINESIS_URL}"
add "kinesis.parsed" "${PARSED_STREAM}"

# Firehose properties
add "firehose.url" "${FIREHOSE_URL}"
add "firehose.malformed" "${MALFORMED_STREAM}"

echo "Wrote config file:"
cat ${CONFIG_FILE}

# run the maven build
mvn clean package -Ddeploy

file=`ls ${home}/target/fineo-lambda-*.jar`
echo "Upload ${home}/target/$file} to AWS"
