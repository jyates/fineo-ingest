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
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
  public void testMultipleWrites() throws Exception {
    readWriteRecord(5, 2);
  }

  @Test
  public void testOutsidePageSizeNumberOfWrites() throws Exception {

  }

  @Test
  public void testRecordWithNoFields() throws Exception {
    readWriteRecord(0);
  }

  public void readWriteRecord() throws Exception {
    readWriteRecord(1);
  }

  public void readWriteRecord(int fieldCount) throws Exception {
    readWriteRecord(1, fieldCount);
  }

  public void readWriteRecord(int recordCount, int fieldCount) throws Exception {
    Properties prop = new Properties();
    dynamo.setConnectionProperties(prop);

    String orgId = "orgid", orgMetric = "metricId";
    long ts = 10;
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    List<GenericRecord> records =
      SchemaTestUtils.createRandomRecord(store, "orgid", "metricId", ts, recordCount,
        fieldCount);

    // setup the writer
    LambdaClientProperties props = new LambdaClientProperties(prop);
    dynamo.setCredentials(props);
    AvroToDynamoWriter writer = AvroToDynamoWriter.create(props);

    // write it to dynamo and wait for a response
    for (GenericRecord record : records) {
      writer.write(record);
    }
    MultiWriteFailures failures = writer.flush();
    assertFalse("There was a write failure", failures.any());

    // ensure that the expected table got created
    AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();
    ListTablesResult tables = client.listTables();
    assertEquals(1, tables.getTableNames().size());
    AvroDynamoReader reader =
      new AvroDynamoReader(store, client, props.getDynamoIngestTablePrefix());

    // sort records by timestamp to ensure that we verify them correctly
    records.sort((r1, r2) -> {
      long ts1 = RecordMetadata.get(r1).getBaseFields().getTimestamp();
      long ts2 = RecordMetadata.get(r2).getBaseFields().getTimestamp();
      return Long.compare(ts1, ts2);
    });

    readAndVerifyRecords(reader, orgId, orgMetric, Range.of(0, 100),
      records.toArray(new GenericRecord[0]));
  }

  private void readAndVerifyRecords(AvroDynamoReader reader, String orgId,
    String orgMetric,
    Range<Instant> range, GenericRecord... records) {
    // ensure that the record we wrote matches what we created
    List<GenericRecord> stream = reader.scan(orgId, orgMetric, range).collect(Collectors.toList());
    int[] count = new int[1];
    for (GenericRecord actual : stream) {
      assertTrue("More records read [next:" + actual + "than expected [" + records + "]",
        records.length > count[0]);
      GenericRecord expected = records[count[0]++];
      RecordMetadata actualMeta = RecordMetadata.get(actual);
      RecordMetadata expectedMeta = RecordMetadata.get(expected);
      assertEquals("Wrong orgID read", expectedMeta.getOrgID(), actualMeta.getOrgID());
      assertEquals("Wrong canonical metric type read", expectedMeta.getMetricCanonicalType(),
        actualMeta.getMetricCanonicalType());
      assertEquals("Wrong schema read", expectedMeta.getMetricSchema(),
        actualMeta.getMetricSchema());
      // we don't store the alias field in the record, so we can't exactly match the base fields.
      // Instead, we have to verify them by hand
      BaseFields actualBase = actualMeta.getBaseFields();
      BaseFields expectedBase = expectedMeta.getBaseFields();
      assertEquals("Timestamp is wrong", expectedBase.getTimestamp(), actualBase.getTimestamp());
      assertEquals("Unknown fields are wrong", expectedBase.getUnknownFields(), actualBase
        .getUnknownFields());

      // verify the non-base fields match
      Schema schema = expectedMeta.getMetricSchema();
      schema.getFields().stream()
            .filter(field -> !field.name().equals(AvroSchemaEncoder.BASE_FIELDS_KEY))
            .forEach(field -> {
              String name = field.name();
              assertEquals(expected.get(name), actual.get(name));
            });
    }
    assertEquals(count[0], records.length);
  }
}