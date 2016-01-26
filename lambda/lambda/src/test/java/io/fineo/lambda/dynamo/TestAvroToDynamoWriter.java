package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


/**
 * Test writing avro records to dynamo. Also covers happy path and failure testing for the
 * {@link io.fineo.lambda.aws.AwsAsyncRequest}, which is omitted for time.
 */
public class TestAvroToDynamoWriter {

  @Test
  public void testFailedWriteDynamoUnreachable() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    LambdaClientProperties props = Mockito.mock(LambdaClientProperties.class);
    Mockito.when(props.getDynamoIngestTablePrefix()).thenReturn(UUID.randomUUID().toString());
    Mockito.when(props.getDynamoMaxRetries()).thenReturn(1L);
    Mockito.when(props.getDynamoReadMax()).thenReturn(10L);
    Mockito.when(props.getDynamoWriteMax()).thenReturn(10L);

    Mockito.when(client.updateItemAsync(Mockito.any(), Mockito.any())).then
      (invocationOnMock -> {
        AsyncHandler<UpdateItemRequest, UpdateItemResult> handler =
          (AsyncHandler<UpdateItemRequest, UpdateItemResult>) invocationOnMock.getArguments()[1];
        handler.onError(new ResourceNotFoundException("mocked exception"));
        return null;
      });
    ListTablesResult tables = new ListTablesResult();
    tables.setTableNames(Lists.newArrayList("t1"));
    Mockito.when(client.listTables(Mockito.any(), Mockito.any())).thenReturn(tables);

    Mockito.when(props.getDynamo()).thenReturn(client);

    AvroToDynamoWriter writer = AvroToDynamoWriter.create(props);
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    writer.write(record);
    MultiWriteFailures<GenericRecord> failures = writer.flush();
    assertTrue(failures.any());
    List<GenericRecord> failed = FailureHandler.getFailedRecords(failures);
    assertEquals(Lists.newArrayList(record), failed);
  }
}
