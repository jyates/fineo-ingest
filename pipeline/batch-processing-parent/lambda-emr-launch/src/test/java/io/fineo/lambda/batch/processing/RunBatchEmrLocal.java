package io.fineo.lambda.batch.processing;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.google.inject.Guice;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.batch.processing.lambda.LaunchBatchProcessingEmrCluster;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Properties;

import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * run the batch cluster launcher locally (rather than as a lambda function). This still runs a
 * full EMR cluster
 */
public class RunBatchEmrLocal {

  @Test
  public void launchCluster() throws Exception {
    AmazonElasticMapReduceClient client = new AmazonElasticMapReduceClient(credentials());
    LaunchBatchProcessingEmrCluster launch = new LaunchBatchProcessingEmrCluster(
      "us-east-1",
      "deploy.fineo.io",
      "lambda/Batch/2016-09-12_18:53:07_-0700/processor/batch-processing-2.0-SNAPSHOT-aws.jar",
      "io.fineo.batch.processing.spark.BatchProcessor",
      "batch-processor",
      client
    );
    launch.handle(new LinkedHashMap<>());
  }

  @Test
  public void addBatchManifestFiles() throws Exception {
    addFilesToBatchManifest(credentials(), "batch-manifest",
      "s3://external-batch-processing-test.fineo.io/remote-s3-file_metric.json",
      "s3://batch.fineo.io/ingest/test-invoke-api-key_local-s3-file_metric.json"
    );
  }

  private AWSCredentialsProvider credentials() {
    return new ProfileCredentialsProvider("jesse-delete-this");
  }

  private void addFilesToBatchManifest(AWSCredentialsProvider credentials, String tableName,
    String... files) {
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(credentials);
    client.setRegion(Region.getRegion(Regions.US_EAST_1));
    Properties props = new Properties();
    props.setProperty(IngestManifestModule.INGEST_MANIFEST_OVERRIDE, tableName);
    IngestManifest manifest = Guice.createInjector(
      IngestManifestModule.create(props),
      instanceModule(client),
      namedInstance(IngestManifestModule.READ_LIMIT, 1l),
      namedInstance(IngestManifestModule.WRITE_LIMIT, 1l)
    ).getInstance(IngestManifest.class);
    for (String file : files) {
      manifest.add("test-invoke-api-key", file);
    }
    manifest.flush();
  }
}
