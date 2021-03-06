package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.aws.AwsDependentTests;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.AvroToDynamoModule;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaEncoderFactory;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.SchemaTestUtils;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static io.fineo.schema.store.SchemaTestUtils.getBaseFields;
import static io.fineo.schema.store.TestSchemaManager.commitSimpleType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test reading/writing avro records to dynamo
 */
@Category(AwsDependentTests.class)
public class TestAvroDynamoIO {

  private static final Log LOG = LogFactory.getLog(TestAvroDynamoIO.class);

  private static final long ONE_WEEK = Duration.ofDays(7).toMillis();
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

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
  public void testWriteRecordWithMissingField() throws Exception {
    String orgId = "orgId", metricID = "metricAlias", field = "f1";
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    manager.newOrg(orgId).newMetric().setDisplayName(metricID).newField().withName(field)
           .withType(StoreManager.Type.INTEGER).build().build().commit();

    AvroSchemaEncoderFactory bridge = new StoreClerk(store, orgId).getEncoderFactory();
    Map fields = getBaseFields(orgId, metricID, 1);
    MapRecord record = new MapRecord(fields);
    GenericData.Record gr = bridge.getEncoder(record).encode();

    TestRunner runner = new TestRunner(store, orgId, metricID, Arrays.asList(gr));
    runner.writeRecords();
    runner.verifyRecords();
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
      return reader.scanMetricAlias(orgId, orgMetricType, range);
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
  public void testWriteOverlappingRecords() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), new Pair<>(fieldName, "INTEGER"));
    TestRunner runner = new TestRunner(store, orgId, metricName, new ArrayList<>());
    runner.writeRecords(createRandomRecordForSchema(store, orgId, metricName, 1, 1,
      fieldName, 1));
    LOG.info("---- Starting second request ----");
    runner.writeRecords(createRandomRecordForSchema(store, orgId, metricName, 1, 1, fieldName, 2));
    runner.verifyTables();
    runner.verifyRecords();
  }

  @Test
  public void testWriteOverlappingRecordsConcurrently() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), new Pair<>(fieldName, "INTEGER"));

    List<GenericRecord> records = createRandomRecordForSchema(store, orgId, metricName, 1, 100,
      fieldName, 1);
    records.addAll(createRandomRecordForSchema(store, orgId, metricName, 1, 100, fieldName, 2));
    records.addAll(createRandomRecordForSchema(store, orgId, metricName, 1, 100, fieldName, 3));

    TestRunner runner = new TestRunner(store, orgId, metricName, records);
    runner.writeRecords();
    runner.verifyTables();
    runner.verifyRecords();
  }

  @Test
  public void testResendEventDoesNotCauseTwoWrites() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), new Pair<>(fieldName, "INTEGER"));

    List<GenericRecord> records = createRandomRecordForSchema(store, orgId, metricName, 1, 1,
      fieldName, 1);

    TestRunner runner = new TestRunner(store, orgId, metricName, records);
    runner.writeRecords();
    runner.writeRecords();
    runner.verifyTables();
    runner.verifyRecords();
  }

  @Test
  public void testWriteRecordWithEmptyString() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), new Pair<>(fieldName, "VARCHAR"));
    AvroSchemaEncoderFactory bridge = new StoreClerk(store, orgId).getEncoderFactory();
    Map fields = getBaseFields(orgId, metricName, 1);
    fields.put(fieldName, "");
    MapRecord record = new MapRecord(fields);
    GenericRecord gr = bridge.getEncoder(record).encode();

    TestRunner runner = new TestRunner(store, orgId, metricName, newArrayList(gr));
    runner.writeRecords();
    runner.verifyTables();
    List<GenericRecord> records =
      runner.reader.scanMetricAlias(orgId, metricName, runner.range).collect(
        Collectors.toList());
    assertEquals(1, records.size());
    GenericRecord wrote = records.get(0);
    assertTrue(checkBaseFieldsMatch(gr, wrote));
    // other field should be null
    RecordMetadata expectedMeta = RecordMetadata.get(gr);
    // verify the non-base fields match
    Schema schema = expectedMeta.getMetricSchema();
    boolean empty = schema.getFields().stream()
                          .filter(
                            field -> !field.name().equals(AvroSchemaProperties.BASE_FIELDS_KEY))
                          .map(field -> {
                            String name = field.name();
                            return wrote.get(name);
                          }).allMatch(a -> a == null);
    assertTrue("Found non-null, non-base fields in record!\nRecord:\n" + wrote, empty);
  }

  @Test
  public void testZeroLengthStringInSameTimestampUpdate() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    StoreManager manager = new StoreManager(store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1", f2 = "f2";
    commitSimpleType(manager, orgId, metricName, of(), new Pair<>(fieldName, "VARCHAR"), new
      Pair<>(f2, "INTEGER"));
    AvroSchemaEncoderFactory bridge = new StoreClerk(store, orgId).getEncoderFactory();
    Map fields = getBaseFields(orgId, metricName, 1);
    fields.put(fieldName, "");
    fields.put(f2, 1);
    MapRecord record = new MapRecord(fields);
    GenericRecord gr = bridge.getEncoder(record).encode();

    TestRunner runner = new TestRunner(store, orgId, metricName, newArrayList(gr));
    runner.writeRecords();

    // second record at the same timestamp
    fields.put(f2, 2);
    record = new MapRecord(fields);
    gr = bridge.getEncoder(record).encode();
    runner.writeRecords(newArrayList(gr));

    runner.verifyTables();

    // verify that we read only null for the varchar field
    List<GenericRecord> records =
      runner.reader.scanMetricAlias(orgId, metricName, runner.range).collect(
        Collectors.toList());
    assertEquals(2, records.size());

    // what the name of the field will be in the returned record
    StoreClerk clerk = new StoreClerk(store, orgId);
    String cname = clerk.getMetrics().get(0).getCanonicalNameFromUserFieldName(fieldName);

    // ensure that only one field (whose canonical name we just looked up) is null
    for (GenericRecord wrote : records) {
      // other field should be null
      RecordMetadata expectedMeta = RecordMetadata.get(gr);
      // verify the non-base fields match
      Schema schema = expectedMeta.getMetricSchema();
      boolean empty = schema.getFields().stream()
                            .filter(
                              field -> !field.name().equals(AvroSchemaProperties.BASE_FIELDS_KEY))
                            .map(field -> {
                              String name = field.name();
                              if (name.equals(cname)) {
                                return wrote.get(name) == null;
                              }
                              return wrote.get(name) != null;
                            }).allMatch(a -> a == true);
      assertTrue("Found non-null fields (for the wrong fields) in record!\nRecord:\n" + wrote,
        empty);
    }
  }

  public static List<GenericRecord> createRandomRecordForSchema(SchemaStore store, String orgId,
    String metricType, long startTs, int recordCount, String fieldName, int fieldValue)
    throws SchemaNotFoundException {
    AvroSchemaEncoderFactory bridge = new StoreClerk(store, orgId).getEncoderFactory();
    ArrayList records = new ArrayList(recordCount);

    for (int i = 0; i < recordCount; ++i) {
      Map fields = getBaseFields(orgId, metricType, startTs + (long) i);
      MapRecord record = new MapRecord(fields);
      fields.put(fieldName, fieldValue);

      records.add(bridge.getEncoder(record).encode());
    }

    return records;
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
  }

  private TestRunner createTestRunner(int recordCount, int fieldCount)
    throws Exception {
    String orgId = "orgId", metricID = "metricAlias";
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    List<GenericRecord> records =
      SchemaTestUtils.createRandomRecord(store, orgId, metricID, 10, recordCount,
        fieldCount);
    return new TestRunner(store, orgId, metricID, records);
  }

  private class TestRunner {
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
      Injector injector = Guice.createInjector(
        new PropertiesModule(prop),
        new AvroToDynamoModule(),
        tables.getDynamoModule(),
        instanceModule(store));
      this.writer = injector.getInstance(AvroToDynamoWriter.class);
      this.reader = injector.getInstance(AvroDynamoReader.class);
    }

    public void writeRecords() throws Exception {
      writeRecords(this.expected, false);
    }

    public void writeRecords(List<GenericRecord> records) {
      writeRecords(records, true);
    }

    public void writeRecords(List<GenericRecord> records, boolean addToExpected) {
      if (addToExpected) {
        this.expected.addAll(records);
      }
      // write it to dynamo and wait for a response
      for (GenericRecord record : records) {
        writer.write(record);
      }
      MultiWriteFailures failures = writer.flush();
      assertFalse("There was a write failure! Failures: " + failures.getActions(),
        failures.any());
    }

    public void verifyTables() {
      // ensure that the expected table got created
      AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
      ListTablesResult tables = client.listTables();
      assertEquals(this.tableCount, tables.getTableNames().size());
    }

    public void verifyRecords() throws SchemaNotFoundException {
      verifyRecords((reader, orgId, orgMetricType, range) -> {
        try {
          return reader.scanMetricAlias(orgId, orgMetricType, range);
        } catch (SchemaNotFoundException e) {
          throw new RuntimeException(e);
        }
      });
    }

    public void verifyRecords(Reader recordReader) throws SchemaNotFoundException {
      // ensure that the record we wrote matches what we created
      List<GenericRecord> records =
        recordReader.read(reader, org, metric, range).collect(Collectors.toList());

      LOG.info("Expected records: " + this.expected);
      LOG.info("Got records:      " + records);

      Multimap<Long, GenericRecord> groupedExcepted = groupByTs(expected);
      Multimap<Long, GenericRecord> groupedActual = groupByTs(records);
      assertEquals(
        "Wrong number of actual vs. grouped records!\n Expected: " + groupedExcepted + "\n"
        + " Actual: " + groupedActual, groupedExcepted.size(), groupedActual.size());

      for (Map.Entry<Long, Collection<GenericRecord>> recs : groupedExcepted.asMap().entrySet()) {
        Collection<GenericRecord> actualRecs = groupedActual.get(recs.getKey());
        assertEquals("Wrong number of records for ts: " + recs.getKey(), recs.getValue().size(),
          actualRecs.size());
        boolean found = false;
        for (GenericRecord expected : recs.getValue()) {
          for (GenericRecord actual : actualRecs) {
            if (matches(expected, actual)) {
              found = true;
              break;
            }
          }
          assertTrue("Missing record: " + expected + " from actual records. Seems to have a "
                     + "non-matching record also present in actual: \n" + actualRecs, found);
        }
      }
    }

    private boolean matches(GenericRecord expected, GenericRecord actual) {
      if (!checkBaseFieldsMatch(expected, actual)) {
        return false;
      }

      RecordMetadata expectedMeta = RecordMetadata.get(expected);
      // verify the non-base fields match
      Schema schema = expectedMeta.getMetricSchema();
      return schema.getFields().stream()
                   .filter(field -> !field.name().equals(AvroSchemaProperties.BASE_FIELDS_KEY))
                   .map(field -> {
                     String name = field.name();
                     return Objects.equals(expected.get(name), (actual.get(name)));
                   })
                   .allMatch(match -> match == true);
    }

    private Multimap<Long, GenericRecord> groupByTs(List<GenericRecord> records) {
      Multimap<Long, GenericRecord> grouped = ArrayListMultimap.create();
      for (GenericRecord record : records) {
        RecordMetadata meta = RecordMetadata.get(record);
        Long ts = meta.getBaseFields().getTimestamp();
        grouped.put(ts, record);
      }
      return grouped;
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

  private boolean checkBaseFieldsMatch(GenericRecord expected, GenericRecord actual) {
    RecordMetadata expectedMeta = RecordMetadata.get(expected);
    RecordMetadata actualMeta = RecordMetadata.get(actual);
    if (!expectedMeta.getOrgID().equals(actualMeta.getOrgID())) {
      return false;
    }
    if (!expectedMeta.getMetricCanonicalType().equals(actualMeta.getMetricCanonicalType())) {
      return false;
    }
    if (!expectedMeta.getMetricSchema().equals(actualMeta.getMetricSchema())) {
      return false;
    }

    BaseFields actualBase = actualMeta.getBaseFields();
    BaseFields expectedBase = expectedMeta.getBaseFields();
    if (!expectedBase.equals(actualBase)) {
      return false;
    }
    return true;
  }

  @FunctionalInterface
  private interface Reader {
    Stream<GenericRecord> read(AvroDynamoReader reader, String orgId, String orgMetricType,
      Range<Instant> range) throws SchemaNotFoundException;
  }
}
