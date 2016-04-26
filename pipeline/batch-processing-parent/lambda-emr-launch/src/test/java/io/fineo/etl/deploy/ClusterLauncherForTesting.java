package io.fineo.etl.deploy;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import io.fineo.batch.processing.lambda.LaunchBatchProcessingEmrCluster;
import io.fineo.batch.processing.spark.BatchProcessor;

/**
 * Launch a cluster for manual testing
 */
public class ClusterLauncherForTesting {

  public static void main(String[] args) {
    new LaunchBatchProcessingEmrCluster(new ProfileCredentialsProvider("launch-emr"), null, BatchProcessor.class.getName(),
      "us-east-1", "test-cluster").deploy();
  }
}
