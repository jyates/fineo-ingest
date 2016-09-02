package io.fineo.lambda.batch.processing;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import io.fineo.batch.processing.lambda.LaunchBatchProcessingEmrCluster;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * run the batch cluster launcher locally (rather than as a lambda function). This still runs a
 * full EMR cluster
 */
public class RunBatchEmrLocal {

  @Test
  public void test() throws Exception {
    AWSCredentials credentials = new ProfileCredentialsProvider("jesse-delete-this")
      .getCredentials();
    AmazonElasticMapReduceClient client = new AmazonElasticMapReduceClient(credentials);
    LaunchBatchProcessingEmrCluster launch = new LaunchBatchProcessingEmrCluster(
      "us-east-1",
      "deploy.fineo.io",
      "lambda/Batch/2016-09-0212:51:34-0700/processor/batch-processing-2.0-SNAPSHOT-aws.jar",
      "io.fineo.batch.processing.spark.BatchProcessor",
      "batch-processor",
      client
      );
    launch.handle(new LinkedHashMap<>());
  }
}
