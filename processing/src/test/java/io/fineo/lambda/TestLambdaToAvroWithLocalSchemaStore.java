package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.FirehoseRecordWriter;
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

/**
 * Test kinesis parsing with a generic, in-memory schema store that is refreshed after each test.
 */
public class TestLambdaToAvroWithLocalSchemaStore {
  private static final Log LOG = LogFactory.getLog(TestLambdaToAvroWithLocalSchemaStore.class);
  private SchemaStore store;

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
    List<KinesisEvent> events = new ArrayList<>();
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      events.add(LambdaTestUtils.getKinesisEvent(fields));
      store = createSchemaStore(store, fields);
    }
    verifyReadWriterEventsWithoutMalformed(store, events, records);
  }

  @Test
  public void testReadWriteMultipleRecordInSameRequest() throws Exception {
    Map<String, Object>[] records = LambdaTestUtils.createRecords(2);

    // setup the mocks/fakes
    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    verifyReadWriterEventsWithoutMalformed(store, Lists.newArrayList(event), records);
  }

  private void verifyReadWriterEventsWithoutMalformed(SchemaStore store, List<KinesisEvent> events,
    Map<String, Object>[] records) throws Exception {
    LambdaClientProperties props = getClientProperties();
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    Mockito.when(producer.flush()).thenReturn(new MultiWriteFailures<>(new ArrayList<>()));

    // do the writing
    OutputFirehoseManager out = new OutputFirehoseManager();
    List<KinesisRequest> requests =
      doSetupAndWrite(props, out, producer, store, events.toArray(new KinesisEvent[events.size()]));

    assertEquals("Got wrong number of parsed records. Received: " + requests, records.length,
      requests.size());
    verifyParsedRecords(requests, records);

    // kinesis write verification, beyond the added records tracking... just in case
    Mockito.verify(producer, Mockito.times(records.length))
           .add(Mockito.anyString(), Mockito.anyString(), Mockito.any(GenericRecord.class));
    Mockito.verify(producer, Mockito.times(events.size())).flush();
  }

  private Pair<KinesisEvent, SchemaStore> createStoreAndSingleEvent(Map<String, Object>[] records)
    throws Exception {
    // add multiple records to the same event
    KinesisEvent event = null;
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      if (event == null) {
        event = LambdaTestUtils.getKinesisEvent(fields);
      } else {
        event.getRecords().addAll(LambdaTestUtils.getKinesisEvent(fields).getRecords());
      }
      store = createSchemaStore(store, fields);
    }
    return new Pair<>(event, store);
  }

  /**
   * Ensure that malformed records are handled by an internal 'malformed' handler, rather than
   * failing the entire event
   */
  @Test
  public void testMalformedRecords() throws Exception {
    int recordCount = 2;
    Map<String, Object>[] records = createMalformedRecords(recordCount);

    // setup the mocks/fakes
    LambdaClientProperties props = getClientProperties();
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    // no failures when we write to firehose/kinesis
    Mockito.when(producer.flush()).thenReturn(new MultiWriteFailures<>(new ArrayList<>()));

    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    OutputFirehoseManager out = new OutputFirehoseManager().withProcessingErrors();
    List<ByteBuffer> malformed = out.listenForProcesssingErrors();
    List<KinesisRequest> requests =
      doSetupAndWrite(props, out, producer, store, event);
    assertEquals(0, requests.size());

    assertEquals(recordCount, malformed.size());
    byte[] data =
      combineRecords(malformed.stream().peek(buff -> assertTrue(buff.hasRemaining())));
    byte[] expected =
      combineRecords(event.getRecords().stream().map(e -> e.getKinesis().getData()));
    assertArrayEquals(expected, data);

    // verify the mocks
    Mockito.verify(out.process(), Mockito.times(2)).addToBatch(Mockito.any());
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

  private void verifyParsedRecords(List<KinesisRequest> requests, Map<String, Object>[] records)
    throws IOException {
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    byte[] data = combineRecords(requests.stream()
                                         .map(request -> {
                                           try {
                                             return writer.write(request.buff);
                                           } catch (IOException e) {
                                             throw new RuntimeException(e);
                                           }
                                         }));

    FirehoseRecordReader<GenericRecord> reader = FirehoseRecordReader.create(ByteBuffer.wrap(data));
    for (int i = 0; i < records.length; i++) {
      LOG.info("Reading and verifying record: " + i);
      // verify that we read the next record in order of it being written
      GenericRecord avro = reader.next();
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

  private List<KinesisRequest> doSetupAndWrite(
    LambdaClientProperties props, OutputFirehoseManager firehose, KinesisProducer producer,
    SchemaStore store, KinesisEvent... events) throws IOException {
    LambdaRawRecordToAvro writer = new LambdaRawRecordToAvro();
    // update the writer with the test properties
    writer.setupForTesting(props, store, producer, firehose.archive(), firehose.process(),
      firehose.commit());

    List<KinesisRequest> parsedRequests = new ArrayList<>();
    Mockito.doAnswer(invoke -> {
      parsedRequests.add(new KinesisRequest(invoke.getArguments()));
      return Futures.immediateFuture(null);
    }).when(producer).add(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(GenericRecord.class));

    // actually run the test
    for (KinesisEvent event : events) {
      writer.handleEventInternal(event);
    }

    return parsedRequests;
  }

  private class KinesisRequest {
    GenericRecord buff;
    String stream;
    String key;

    public KinesisRequest(String stream, String key, GenericRecord buff) {
      this.buff = buff;
      this.stream = stream;
      this.key = key;
    }

    public KinesisRequest(Object[] arguments) {
      this((String) arguments[0], (String) arguments[1], (GenericRecord) arguments[2]);
    }
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
    props.put(LambdaClientProperties.FIREHOSE_URL, "url");
    props.put(LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME, "stream");
    return props;
  }
}
