package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.Lists;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Test writing avro records to dynamo. Also covers happy path and failure testing for the
 * {@link io.fineo.lambda.aws.AwsAsyncRequest}, which is omitted for time.
 */
public class TestAvroToDynamoWriter {

  @Test
  public void testFailedWriteDynamoUnreachable() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    String prefix = UUID.randomUUID().toString();

    Mockito.when(client.updateItemAsync(Mockito.any(), Mockito.any())).then
      (invocationOnMock -> {
        AsyncHandler<UpdateItemRequest, UpdateItemResult> handler =
          (AsyncHandler<UpdateItemRequest, UpdateItemResult>) invocationOnMock.getArguments()[1];
        handler.onError(new ResourceNotFoundException("mocked exception"));
        return null;
      });

    long time = System.currentTimeMillis();
    Range<Instant> range = DynamoTableManager.getStartEnd(Instant.ofEpochMilli(time));
    String name =
      DynamoTableManager.TABLE_NAME_PARTS_JOINER
        .join(prefix, range.getStart().toEpochMilli(), range.getStart().toEpochMilli());
    ListTablesResult tables = new ListTablesResult();
    tables.setTableNames(Lists.newArrayList(name));
    Mockito.when(client.listTables(Mockito.any(), Mockito.any())).thenReturn(tables);

    AvroToDynamoWriter writer = new AvroToDynamoWriter(client, prefix, 10L, 10L, 1L);

    GenericRecord record = SchemaTestUtils.createRandomRecord("orgId", "metricType",
      time, 1).get(0);
    writer.write(record);
    MultiWriteFailures<GenericRecord> failures = writer.flush();
    assertTrue(failures.any());
    List<GenericRecord> failed = FailureHandler.getFailedRecords(failures);
    assertEquals(Lists.newArrayList(record), failed);
  }
}
