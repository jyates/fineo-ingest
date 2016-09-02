package io.fineo.batch.processing.dynamo;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIngestManifest extends BaseDynamoTableTest {

  @Test
  public void testReadNoTable() throws Exception {
    assertReadFiles();
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
  public void testRemoveAllFiles() throws Exception {
    IngestManifest manifest = getNewManifest();
    String org = "org1";
    manifest.add(org, "file1");
    manifest.flush();

    manifest = getNewManifest();
    manifest.remove(org, "file1");
    manifest.flush();

    manifest = getNewManifest();
    assertTrue(manifest.files().isEmpty());
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

  @Test
  public void testRemoveFilesAndUpdateErrors() throws Exception {
    IngestManifest manifest = getNewManifest();
    String org = "org1";
    manifest.add(org, "file1");
    manifest.add(org, "file2");
    manifest.add(org, "file3");
    manifest.flush();

    manifest.remove(org, "file1", "file3");
    FailedIngestFile fail = new FailedIngestFile(org, "file1", "f");
    manifest.addFailure(fail);
    manifest.flush();

    assertReadFiles("file2");
    assertFailures(fail);

    // tried the file again, and it failed hard again
    manifest.files();// force a load
    FailedIngestFile fail2 = new FailedIngestFile(org, "file1", "f2");
    manifest.addFailure(fail2);
    manifest.flush();

    assertFailures(fail, fail2);
  }

  private void assertReadFiles(String... files) {
    List<String> actual = newArrayList(getNewManifest().files().values());
    Collections.sort(actual);
    List<String> expected = newArrayList(files);
    assertEquals("Wrong values read! Got: " + expected + ", actual: " + actual, expected, actual);
  }

  private void assertFailures(FailedIngestFile... allFailures) {
    Multimap<String, FailedIngestFile> expected = ArrayListMultimap.create();
    for (FailedIngestFile f : allFailures) {
      expected.put(f.getOrg(), f);
    }
    assertEquals(expected, getNewManifest().failures(true));
  }

  private IngestManifest getNewManifest() {
    List<Module> modules = new ArrayList<>();
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    modules.add(instanceModule(client));
    modules.add(IngestManifestModule.createForTesting());
    modules.add(InstanceToNamed.namedInstance(IngestManifestModule.READ_LIMIT, 1L));
    modules.add(InstanceToNamed.namedInstance(IngestManifestModule.WRITE_LIMIT, 1L));
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> submitter =
      new AwsAsyncSubmitter<>(3, client::updateItemAsync);
    modules.add(instanceModule(submitter));
    return Guice.createInjector(modules).getInstance(IngestManifest.class);
  }
}
