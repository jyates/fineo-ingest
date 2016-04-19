package io.fineo.etl.processing;

import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.Lists;
import io.fineo.etl.processing.raw.ProcessJsonToAvro;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

/**
 * Test kinesis parsing with a generic, in-memory schema store that is refreshed after each test.
 */
public class TestProcessRawAvroWithLocalSchemaStore {
  private static final Log LOG = LogFactory.getLog(TestProcessRawAvroWithLocalSchemaStore.class);
  private SchemaStore store;
  private final JSON json = JSON.std;

  @Before
  public void setupStore() throws Exception {
    store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }

  /**
   * Read/write a single event successfully
   *
   * @throws Exception on failure
   */
  @Test
  public void readWriteRecord() throws Exception {
    verifyReadWriteRecordsAsIndividualEvents(LambdaTestUtils.createRecords(1));
  }

  @Test
  public void testReadWriteMultipleRequests() throws Exception {
    verifyReadWriteRecordsAsIndividualEvents(LambdaTestUtils.createRecords(2));
  }

  @SafeVarargs
  private final void verifyReadWriteRecordsAsIndividualEvents(Map<String, Object>... records)
    throws Exception {
    // setup the mocks/fakes
    List<String> events = new ArrayList<>();
    SchemaStore store = null;

    for (Map<String, Object> fields : records) {
      events.add(json.asString(fields));
      store = createSchemaStore(store, fields);
    }
    verifyReadWriteEvents(store, events, records);
  }

  @Test
  public void testReadWriteMultipleRecordInSameRequest() throws Exception {
    Map<String, Object>[] records = LambdaTestUtils.createRecords(2);

    // setup the mocks/fakes
    Pair<String, SchemaStore> created = createStoreAndSingleEvent(records);
    String event = created.getKey();
    SchemaStore store = created.getValue();

    verifyReadWriteEvents(store, Lists.newArrayList(event), records);
  }

  private void verifyReadWriteEvents(SchemaStore store, List<String> events,
    Map<String, Object>[] records) throws Exception {
    OutputWriter producer = Mockito.mock(OutputWriter.class);
    Mockito.when(producer.commit()).thenReturn(new MultiWriteFailures<>(new ArrayList<>()));

    // do the writing
    OutputFirehoseManagerForTests out = new OutputFirehoseManagerForTests();
    List<Message> requests =
      doSetupAndWrite(out, producer, store, events.toArray(new String[0]));

    // one commit for each event, but we may have more than one record written
    Mockito.verify(producer, times(events.size())).commit();
    Mockito.verify(producer, times(records.length)).write(Mockito.any());

    assertEquals("Got wrong number of parsed records. Received: " + requests, records.length,
      requests.size());
    verifyParsedRecords(requests, records);
    out.verifyNoErrors(events.size());
  }

  private Pair<String, SchemaStore> createStoreAndSingleEvent(Map<String, Object>... records)
    throws Exception {
    // add multiple records to the same event
    StringBuilder event = new StringBuilder();
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      event.append(json.asString(fields));
      store = createSchemaStore(store, fields);
    }
    return new Pair<>(event.toString(), store);
  }

  /**
   * Ensure that malformed records are handled by an internal 'malformed' handler, rather than
   * failing the entire event
   */
  @Test
  public void testMalformedRecords() throws Exception {
    for (Map<String, Object> record : createMalformedRecords(100)) {
      verifyMalformedRecordFails(record);
    }
  }

  private void verifyMalformedRecordFails(Map<String, Object> record) throws Exception {
    // setup the mocks/fakes
    OutputWriter producer = Mockito.mock(OutputWriter.class);
    // no failures when we write to firehose/kinesis
    Mockito.when(producer.commit()).thenReturn(new MultiWriteFailures<>(new ArrayList<>()));

    Pair<String, SchemaStore> created = createStoreAndSingleEvent(record);
    String event = created.getKey();
    SchemaStore store = created.getValue();

    OutputFirehoseManagerForTests firehoses = new OutputFirehoseManagerForTests();
    List<ByteBuffer> malformed = firehoses.listenForProcesssingErrors();
    List<Message> requests = doSetupAndWrite(firehoses, producer, store, event);

    // no requests made, but we still try to commit
    assertEquals(0, requests.size());
    Mockito.verify(producer).commit();

    // make sure that we actually did write to the processing error handler, but the commit one
    firehoses.verifyErrors(firehoses.process(), 1);
    Mockito.verifyZeroInteractions(firehoses.commit());

    // check to make sure we wrote the right content
    assertEquals("Only have one event, but got more broken records", 1, malformed.size());
    byte[] data =
      combineRecords(malformed.stream().peek(buff -> assertTrue(buff.hasRemaining())));
    byte[] expected = event.getBytes();
    assertArrayEquals(expected, data);
  }


  private Map<String, Object>[] createMalformedRecords(int count) {
    Map<String, Object>[] events = LambdaTestUtils.createRecords(count);
    long seed = System.currentTimeMillis();
    LOG.info("Using malformed record seed: " + seed);
    Random random = new Random(seed);
    // randomly remove either the org key or the metric type key, creating a 'broken' event
    String[] fields =
      new String[]{AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY};
    for (Map<String, Object> map : events) {
      map.remove(fields[random.nextInt(2)]);
    }
    return events;
  }

  private void verifyParsedRecords(List<Message> requests, Map<String, Object>[] records)
    throws IOException {
    assertEquals("Wrong number of received vs expected records!", records.length, requests.size());
    for (int i = 0; i < records.length; i++) {
      LOG.info("Reading and verifying record: " + i);
      // verify that we read the next record in order of it being written
      GenericRecord avro = requests.get(i).getRecord();
      TestRecordMetadata.verifyRecordMetadataMatchesExpectedNaming(avro);
    }
  }

  /**
   * Combine all the bytebuffer into a single byte[]. Mimics reading a bunch of bytes from S3
   *
   * @param requests
   * @return
   * @throws IOException
   */
  private byte[] combineRecords(Stream<ByteBuffer> requests) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    requests.forEach(data -> {
      byte[] raw = new byte[data.limit()];
      data.get(raw);
      data.clear();
      try {
        bos.write(raw);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    bos.close();
    return bos.toByteArray();
  }

  private List<Message> doSetupAndWrite(
    OutputFirehoseManagerForTests firehose, OutputWriter out,
    SchemaStore store, String... events) throws IOException {
    ProcessJsonToAvro writer =
      new ProcessJsonToAvro(() -> firehose.process(), () -> firehose.commit(), out, store);
    List<Message> parsedRequests = new ArrayList<>();
    Mockito.doAnswer(invoke -> {
      parsedRequests.add((Message) invoke.getArguments()[0]);
      return null;
    }).when(out).write(Mockito.any(GenericRecord.class));

    // actually run the test
    for (String event : events) {
      writer.handle(event);
    }

    return parsedRequests;
  }

  /**
   * Create a schema that matches the fields in the event. Assumes all fields are string type
   *
   * @return new store with that event
   */
  private SchemaStore createSchemaStore(SchemaStore store, Map<String, Object> event)
    throws Exception {
    LambdaClientProperties props = getClientProperties();
    if (store == null) {
      store = props.createSchemaStore();
    }
    try {
      EndToEndTestRunner.updateSchemaStore(store, event);
    } catch (IllegalArgumentException e) {
      return null;
    }
    return store;
  }

  protected LambdaClientProperties getClientProperties() throws Exception {
    Properties props = getMockProps();
    return LambdaClientProperties.createForTesting(props, store);
  }

  protected Properties getMockProps() {
    Properties props = new Properties();
    props.put(LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME, "stream");
    return props;
  }
}
