package io.fineo.lambda.e2e.resources;

import com.google.common.collect.Lists;
import io.fineo.lambda.configure.LambdaClientProperties;

import java.time.Duration;
import java.util.List;

/**
 * Properties for manage remote test resources
 */
public class TestProperties {

  public static final long ONE_SECOND = Duration.ofSeconds(1).toMillis();
  public static final long FIVE_MINUTES = Duration.ofMinutes(5).toMillis();
  public static final long ONE_MINUTE = Duration.ofMinutes(1).toMillis();

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
