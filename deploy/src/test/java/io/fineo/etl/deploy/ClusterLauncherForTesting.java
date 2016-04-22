package io.fineo.etl.deploy;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import io.fineo.batch.processing.spark.BatchProcessor;

/**
 * Launch a cluster for manual testing
 */
public class ClusterLauncherForTesting {

  public static void main(String[] args) {
    new LaunchSparkEmrCluster(null, BatchProcessor.class.getName(),
      new ProfileCredentialsProvider("launch-emr"), "us-east-1", "test-cluster").deploy();
  }
}
