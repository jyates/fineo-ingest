package io.fineo.lambda;

import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 */
public class TestProperties {

  public static final int ONE_SECOND = 1000;
  public static final int THREE_HUNDRED_SECONDS = 3 * 100 * ONE_SECOND;
  public static final int ONE_MINUTE = ONE_SECOND * 60;

  public static class Firehose{

    /**
     * test role for the Firehose stream -> S3 bucket
     */
    public static String FIREHOSE_TO_S3_ARN_ROLE =
      "arn:aws:iam::766732214526:role/test-lambda-functions";
    public static String S3_BUCKET_NAME = "test.fineo.io";
    public static String S3_BUCKET_ARN = "arn:aws:s3:::" + S3_BUCKET_NAME;
  }

  public static class Kinesis{

    public static final String KINESIS_STREAM_ARN_TO_FORMAT =
      "arn:aws:kinesis:%s:766732214526:stream/%s";
    public static Integer SHARD_COUNT = 1;
  }

  public static class Lambda{

    private static final String RAW_TO_AVRO_ARN_TO_FORMAT =
      "arn:aws:lambda:%s:766732214526:function:RawToAvro";
    private static final String AVRO_TO_STORE_ARN_TO_FORMAT =
      "arn:aws:lambda:%s:766732214526:function:AvroToStorage";

    public static final List<String> STAGES = Lists.newArrayList(LambdaClientProperties.RAW_PREFIX,
      LambdaClientProperties
      .STAGED_PREFIX);

    public static String getRawToAvroArn(String region){
      return String.format(RAW_TO_AVRO_ARN_TO_FORMAT, region);
    }
    public static String getAvroToStoreArn(String region){
      return String.format(AVRO_TO_STORE_ARN_TO_FORMAT, region);
    }
  }
}