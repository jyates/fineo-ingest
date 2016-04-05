package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import io.fineo.aws.AwsDependentTests;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.util.SchemaUtil;
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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test reading/writing avro records to dynamo
 */
@Category(AwsDependentTests.class)
public class TestAvroDynamoIO {

  private static final long ONE_WEEK = Duration.ofDays(7).toMillis();
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
    TestRunner runner = createTestRunner(10, 1);
    runner.writeRecords();
    runner.verifyTables();
    runner.verifyRecords((reader, orgId, orgMetricType, range) -> {
      // prefetch after reading 5 records
      reader.setPrefetchSize(5);
      // each scan returns up to 6 records, which should be paged through in two requests
      ScanRequest scan = new ScanRequest();
      scan.setLimit(6);
      return reader.scan(orgId, orgMetricType, range, scan);
    });
  }

  @Test
  public void testReadAcrossTables() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "o", metric = "m";
    List<GenericRecord> records = SchemaTestUtils.createRandomRecord(store, org, metric, 10, 1, 1);
    records
      .addAll(SchemaTestUtils.createRandomRecordForSchema(store, org, metric, ONE_WEEK + 1, 1, 1));
    TestRunner runner = new TestRunner(store, org, metric, records)
      .withTimeRange(0, 2 * ONE_WEEK)
      .withNumberofTables(2);
    runner.writeRecords();
    runner.verifyTables();
    runner.verifyRecords();
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

  public void readWriteRecord(int recordCount, int fieldCount) throws
    Exception {
    TestRunner runner = createTestRunner(recordCount, fieldCount);
    runner.writeRecords();
    runner.verifyTables();
    runner.verifyRecords();
  }

  public static TestRunner createTestRunner(int recordCount, int fieldCount)
    throws Exception {
    String orgId = "orgId", metricID = "metricAlias";
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    List<GenericRecord> records =
      SchemaTestUtils.createRandomRecord(store, orgId, metricID, 10, recordCount,
        fieldCount);
    return new TestRunner(store, orgId, metricID, records);
  }

  private static class TestRunner {
    private final List<GenericRecord> expected;
    private final AvroDynamoReader reader;
    private final AvroToDynamoWriter writer;
    private final String org;
    private final String metric;

    // defaults
    private Range<Instant> range = Range.of(0, 100);
    private int tableCount = 1;

    public TestRunner(SchemaStore store, String orgId, String metricID, List<GenericRecord> records)
      throws Exception {
      this.org = orgId;
      this.metric = metricID;

      // sort records by timestamp to ensure that we verify them correctly
      records.sort((r1, r2) -> {
        long ts1 = RecordMetadata.get(r1).getBaseFields().getTimestamp();
        long ts2 = RecordMetadata.get(r2).getBaseFields().getTimestamp();
        return Long.compare(ts1, ts2);
      });
      this.expected = records;

      Properties prop = new Properties();
      dynamo.setConnectionProperties(prop);

      // setup the writer/reader
      LambdaClientProperties props = new LambdaClientProperties(prop);
      dynamo.setCredentials(props);
      this.writer = AvroToDynamoWriter.create(props);
      AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();
      this.reader =
        new AvroDynamoReader(store, client, props.getDynamoIngestTablePrefix());
    }

    public void writeRecords() throws Exception {
      // write it to dynamo and wait for a response
      for (GenericRecord record : this.expected) {
        writer.write(record);
      }
      MultiWriteFailures failures = writer.flush();
      assertFalse("There was a write failure", failures.any());
    }

    public void verifyTables() {
      // ensure that the expected table got created
      AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();
      ListTablesResult tables = client.listTables();
      assertEquals(this.tableCount, tables.getTableNames().size());
    }

    public void verifyRecords() {
      verifyRecords((reader, orgId, orgMetricType, range) -> reader.scan(orgId, orgMetricType,
        range));
    }

    public void verifyRecords(Reader recordReader) {
      // ensure that the record we wrote matches what we created
      List<GenericRecord> stream =
        recordReader.read(reader, org, metric, range).collect(Collectors.toList());
      int[] count = new int[1];
      for (GenericRecord actual : stream) {
        assertTrue(
          "More records read than expected \nexpected:" + SchemaUtil.toString(expected) +
          "\n --------------------- \n " +
          "actual:" + SchemaUtil.toString(stream),
          expected.size() > count[0]);
        GenericRecord expected = this.expected.get(count[0]++);
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
      assertEquals("Didn't read the correct number of records", expected.size(), count[0]);
    }

    public TestRunner withTimeRange(long start, long end) {
      this.range = Range.of(start, end);
      return this;
    }

    public TestRunner withNumberofTables(int tableCount) {
      this.tableCount = tableCount;
      return this;
    }
  }

  @FunctionalInterface
  private interface Reader {
    Stream<GenericRecord> read(AvroDynamoReader reader, String orgId, String orgMetricType,
      Range<Instant>
        range);
  }

}
