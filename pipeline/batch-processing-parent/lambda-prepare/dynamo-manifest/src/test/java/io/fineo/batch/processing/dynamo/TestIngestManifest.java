package io.fineo.batch.processing.dynamo;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.SS;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class TestIngestManifest extends BaseDynamoTableTest {

  @Test
  public void testReadNoTable() throws Exception {
    IngestManifest read = getNewManifest();
    read.load();
    assertEquals(newArrayList(), newArrayList(read.files()));
  }

  @Test
  public void testSimpleReadWriteManifest() throws Exception {
    IngestManifest manifest = getNewManifest();
    manifest.add("org1", "file1");
    manifest.flush();
    assertReadFiles("file1");
  }

  @Test
  public void testReadMultipleFilesPerOrg() throws Exception {
    IngestManifest write = getNewManifest();
    String orgId1 = "org1", orgId2 = "org2";
    write.add(orgId1, "file1");
    write.add(orgId1, "file2");
    write.add(orgId1, "file3");
    write.add(orgId2, "file4");
    write.add(orgId2, "file5");
    write.flush();
    assertReadFiles("file1", "file2", "file3", "file4", "file5");
  }

  @Test
  public void testDuplicateFilesForSameOrg() throws Exception {
    IngestManifest write = getNewManifest();
    String orgId1 = "org1";
    write.add(orgId1, "file1");
    write.add(orgId1, "file2");
    write.add(orgId1, "file2");
    write.add(orgId1, "file2");
    write.flush();
    assertReadFiles("file1", "file2");
  }

  @Test
  public void testUpdateFiles() throws Exception {
    IngestManifest manifest = getNewManifest();
    String org = "org1";
    manifest.add(org, "file1");
    manifest.flush();

    manifest = getNewManifest();
    manifest.add(org, "file2");
    manifest.flush();

    assertReadFiles("file1", "file2");
  }


  @Test
  public void testRemoveNoTableDoesNotThrowException() throws Exception {
    IngestManifest manifest = getNewManifest();
    manifest.remove("org", "file");
    manifest.flush();
  }

  @Test
  public void testRemoveSingleFile() throws Exception {
    IngestManifest manifest = getNewManifest();
    String org = "org1";
    manifest.add(org, "file1");
    manifest.flush();

    manifest.remove(org, "file1");
    manifest.flush();
    assertReadFiles();

    manifest.add(org, "file1");
    manifest.add(org, "file2");
    manifest.flush();

    manifest.remove(org, "file1");
    manifest.flush();
    assertReadFiles("file2");
  }

  @Test
  public void testRemoveMultipleFiles() throws Exception {
    IngestManifest manifest = getNewManifest();
    String org = "org1";
    manifest.add(org, "file1");
    manifest.add(org, "file2");
    manifest.add(org, "file3");
    manifest.flush();

    manifest.remove(org, "file1", "file3");
    manifest.flush();
    assertReadFiles("file2");
  }

  private void assertReadFiles(String... files) {
    IngestManifest read = getNewManifest();
    read.load();
    Collection<String> readFiles = read.files();
    List<String> actual = newArrayList(readFiles);
    Collections.sort(actual);
    List<String> expected = newArrayList(files);
    assertEquals("Wrong values read! Got: " + expected + ", actual: " + actual, expected, actual);
  }

  private IngestManifest getNewManifest() {
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    DynamoDBMapper mapper = new IngestManifestModule().getMapper(client, null);
    ProvisionedThroughput throughput = new ProvisionedThroughput(1L, 1L);
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> submitter =
      new AwsAsyncSubmitter<>(3, client::updateItemAsync);
    return new IngestManifest(mapper, client, () -> throughput, submitter);
  }
}
