package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.schema.store.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Test writing avro records to dynamo. Also covers happy path and failure testing for the
 * {@link io.fineo.lambda.aws.AwsAsyncRequest}, which is omitted for time.
 */
@Category(AwsDependentTests.class)
public class TestAvroToDynamoWriter {

  @Test
  public void testFailedWriteDynamoUnreachable() throws Exception {
    AwsCredentialResource credentials = new AwsCredentialResource();
    LocalDynamoTestUtil util = new LocalDynamoTestUtil(credentials);
    try {
      util.start();
      AmazonDynamoDBAsyncClient clientForBasicActions = util.getAsyncClient();
      String prefix = UUID.randomUUID().toString();

      AmazonDynamoDBAsyncClient spy = Mockito.spy(clientForBasicActions);
      Mockito.doThrow(new ResourceNotFoundException("mocked exception"))
             .when(spy).updateItem(Mockito.any());

      long time = System.currentTimeMillis();
      DynamoTableTimeManager manager = new DynamoTableTimeManager(spy, prefix);
      String name = manager.getTableName(time);
      ListTablesResult tables = new ListTablesResult();
      tables.setTableNames(Lists.newArrayList(name));

      DynamoDB dynamo = new DynamoDB(spy);
      DynamoTableCreator creator = new DynamoTableCreator(manager, dynamo, 10L, 10L);
      AvroToDynamoWriter writer = new AvroToDynamoWriter(spy, 1L, creator);

      GenericRecord record = SchemaTestUtils.createRandomRecord("orgId", "metricType",
        time, 1).get(0);
      writer.write(record);
      util.stop();
      MultiWriteFailures<GenericRecord> failures = writer.flush();
      assertTrue(failures.any());
      List<GenericRecord> failed = FailureHandler.getFailedRecords(failures);
      assertEquals(Lists.newArrayList(record), failed);
    } finally {
      util.stop();
    }
  }
}
