package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.MockOnNullInstanceModule;
import io.fineo.lambda.configure.NullableNamedInstanceModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.e2e.state.EndToEndTestRunner;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.SchemaStore;
import io.fineo.lambda.avro.FirehoseRecordReader;
import io.fineo.lambda.avro.FirehoseRecordWriter;
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

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test kinesis parsing with a generic, in-memory schema store that is refreshed after each test.
 */
public class TestLambdaRawToAvroWithLocalSchemaStore {
  private static final Log LOG = LogFactory.getLog(TestLambdaRawToAvroWithLocalSchemaStore.class);
  protected Provider<SchemaStore> store;

  @Before
  public void setupStore() throws Exception {
    store = getStoreProvider();
  }

  protected Provider<SchemaStore> getStoreProvider() throws Exception {
    return Providers.of(new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY)));
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
    for (Map<String, Object> fields : records) {
      events.add(LambdaTestUtils.getKinesisEvent(fields));
      createSchemaStore(fields);
    }
    verifyReadWriterEventsWithoutMalformed(events, records);
  }

  @Test
  public void testReadWriteMultipleRecordInSameRequest() throws Exception {
    Map<String, Object>[] records = LambdaTestUtils.createRecords(2);

    // setup the mocks/fakes
    KinesisEvent event = createStoreAndSingleEvent(records);
    verifyReadWriterEventsWithoutMalformed(newArrayList(event), records);
  }

  private void verifyReadWriterEventsWithoutMalformed(
    List<KinesisEvent> events,
    Map<String, Object>[] records) throws Exception {
    Properties props = getClientProperties();
    IKinesisProducer producer = Mockito.mock(KinesisProducer.class);
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

  private KinesisEvent createStoreAndSingleEvent(Map<String, Object>[] records)
    throws Exception {
    // addFirehose multiple records to the same event
    KinesisEvent event = null;
    for (Map<String, Object> fields : records) {
      if (event == null) {
        event = LambdaTestUtils.getKinesisEvent(fields);
      } else {
        event.getRecords().addAll(LambdaTestUtils.getKinesisEvent(fields).getRecords());
      }
      createSchemaStore(fields);
    }
    return event;
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
    Properties props = getClientProperties();
    IKinesisProducer producer = Mockito.mock(KinesisProducer.class);
    // no failures when we write to firehose/kinesis
    Mockito.when(producer.flush()).thenReturn(new MultiWriteFailures<>(new ArrayList<>()));

    KinesisEvent event = createStoreAndSingleEvent(records);
    Provider<SchemaStore> store =
      Providers.of(MockOnNullInstanceModule.throwingMock(SchemaStore.class));

    OutputFirehoseManager out = new OutputFirehoseManager().withProcessingErrors();
    List<ByteBuffer> malformed = out.listenForProcesssingErrors();
    List<KinesisRequest> requests = doSetupAndWrite(props, out, producer, store, event);
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
    Properties props, OutputFirehoseManager firehose, IKinesisProducer producer,
    Provider<SchemaStore> store, KinesisEvent... events) throws IOException {
    LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> writer =
      getLambda(props, firehose, producer, store);

    List<KinesisRequest> parsedRequests = new ArrayList<>();
    Mockito.doAnswer(invoke -> {
      parsedRequests.add(new KinesisRequest(invoke.getArguments()));
      return Futures.immediateFuture(null);
    }).when(producer).add(Mockito.anyString(), Mockito.anyString(),
      Mockito.any(GenericRecord.class));

    // actually run the test
    for (KinesisEvent event : events) {
      writer.handle(event);
    }

    return parsedRequests;
  }

  private LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> getLambda(Properties props,
    OutputFirehoseManager firehoses,
    IKinesisProducer producer, Provider<SchemaStore> store) {
    return new RawStageWrapper(newArrayList(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(SchemaStore.class).toProvider(store);
        }
      },
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, firehoses.archive(),
        FirehoseBatchWriter.class),
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM,
        firehoses.commit(), FirehoseBatchWriter.class),
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM,
        firehoses.process(), FirehoseBatchWriter.class),
      new MockOnNullInstanceModule<>(producer, IKinesisProducer.class),
      new PropertiesModule(props)));
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
  private void createSchemaStore(Map<String, Object> event)
    throws Exception {
    try {
      EndToEndTestRunner.updateSchemaStore(store.get(), event);
    } catch (IllegalArgumentException e) {
      LOG.warn("Error updating store!", e);
      return;
    }
  }

  protected Properties getClientProperties() throws Exception {
    Properties props = new Properties();
    props.put(FineoProperties.KINESIS_URL, "kurl");
    props.put(FineoProperties.FIREHOSE_URL, "url");
    props.put(FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME, "stream");
    return props;
  }
}
