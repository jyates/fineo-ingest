package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import io.fineo.aws.AwsDependentTests;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test reading/writing avro records to dynamo
 */
@Category(AwsDependentTests.class)
public class TestAvroDynamoIO {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();

  @After
  public void cleanupTables() throws Exception {
    dynamo.cleanup();
  }

  @Test
  public void testSingleWrite() throws Exception {
    // create a basic record to write that is 'avro correct'
    readWriteRecord();
  }

  @Test
  public void testRecordWithNoFields() throws Exception {
    readWriteRecord(10);
  }

  public void readWriteRecord() throws Exception {
    readWriteRecord(1);
  }

  public void readWriteRecord(int fieldCount) throws Exception {
    Properties prop = new Properties();
    dynamo.setConnectionProperties(prop);

    String orgId = "orgid", orgMetric = "metricId";
    long ts = 10;
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    GenericRecord record = SchemaTestUtils.createRandomRecord(store, "orgid", "metricId", ts, 1,
      fieldCount).get(0);

    // setup the writer
    LambdaClientProperties props = new LambdaClientProperties(prop);
    dynamo.setCredentials(props);
    AvroToDynamoWriter writer = AvroToDynamoWriter.create(props);

    // write it to dynamo and wait for a response
    writer.write(record);
    MultiWriteFailures failures = writer.flush();
    assertFalse("There was a write failure", failures.any());

    // ensure that the expected table got created
    AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();
    ListTablesResult tables = client.listTables();
    assertEquals(1, tables.getTableNames().size());
    AvroDynamoReader reader =
      new AvroDynamoReader(store, client, props.getDynamoIngestTablePrefix());
    readAndVerifyRecord(reader, orgId, orgMetric, Range.of(0, 100), record);
  }

  private void readAndVerifyRecord(AvroDynamoReader reader, String orgId,
    String orgMetric,
    Range<Instant> range, GenericRecord... records) {
    // ensure that the record we wrote matches what we created
    Stream<GenericRecord> stream = reader.scan(orgId, orgMetric, range);
    int[] count = new int[1];
    stream.forEach(record -> {
      GenericRecord written = records[count[0]++];
      // we don't store the alias field in the record, so we can't exactly match the base fields.
      // Instead, we have to verify them by hand
      BaseFields base = (BaseFields) written.get(AvroSchemaEncoder.BASE_FIELDS_KEY);
      

      // verify the non-base fields match

      assertEquals(written, record);
    });
    assertEquals(count, records.length);
  }
}