package io.fineo.lambda.util;

import org.junit.rules.ExternalResource;

/**
 *
 */
public class AwsTestRule extends ExternalResource {

  private final String region = System.getProperty("aws-test-region", "us-east-1");

  public String getRegion(){
    return this.region;
  }
  private static final String KINESIS_STREAM_ARN_TO_FORMAT =
    "arn:aws:kinesis:%s:766732214526:stream/%s";
  public String getKinesisArn(String streamName){
    return String.format(KINESIS_STREAM_ARN_TO_FORMAT, getRegion(), streamName);
  }
}
