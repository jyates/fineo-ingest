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
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

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
@Category(AwsDependentTests.class)
public class TestAvroToDynamoWriter {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();

  @After
  public void cleanupTables() throws Exception{
    dynamo.cleanup();
  }

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
    List<GenericRecord> failed = AvroToDynamoWriter.getFailedRecords(failures);
    assertEquals(Lists.newArrayList(record), failed);
  }

  @Test
  public void testSingleWrite() throws Exception {
    // create a basic record to write that is 'avro correct'
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    readWriteRecord(record);
  }

  @Test
  public void testRecordWithNoFields() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord("orgid", "metricId", 10, 1, 0).get(0);
    readWriteRecord(record);
  }

  public void readWriteRecord(GenericRecord record) throws Exception{
    Properties prop = new Properties();
    dynamo.setConnectionProperties(prop);

    // setup the writer
    LambdaClientProperties props = new LambdaClientProperties(prop);
    dynamo.setCredentials(props);
    AvroToDynamoWriter writer = AvroToDynamoWriter.create(props);

    // write it to dynamo and wait for a response
    writer.write(record);
    MultiWriteFailures failures = writer.flush();
    assertFalse("There was a write failure", failures.any());

    // ensure that the expected table got created
    AmazonDynamoDBClient client = dynamo.getClient();
    ListTablesResult tables = client.listTables();
    assertEquals(1, tables.getTableNames().size());

    // ensure that the record we wrote matches what we created
    AvroRecordDecoder decoder = new AvroRecordDecoder(record);
    Table t = new DynamoDB(dynamo.getClient()).getTable(tables.getTableNames().get(0));
    Item i = t.getItem(AvroToDynamoWriter.PARTITION_KEY_NAME,
      decoder.getMetadata().getOrgID() + decoder.getMetadata().getMetricCannonicalType(),
      AvroToDynamoWriter.SORT_KEY_NAME, decoder.getBaseFields().getTimestamp());
    assertNotNull(i);
    // check the other fields that are not part of the PK
    record.getSchema().getFields().stream().filter(field -> field.name().startsWith("n")).forEach
      ( field -> {
        String name = field.name();
        assertEquals(record.get(name), i.get(name));
      });
  }
}
