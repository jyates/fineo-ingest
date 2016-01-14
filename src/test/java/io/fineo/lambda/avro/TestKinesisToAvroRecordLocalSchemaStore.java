package io.fineo.lambda.avro;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.fineo.internal.customer.Malformed;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaStore;
import javafx.util.Pair;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.SeekableByteArrayInput;
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
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test kinesis parsing with a generic, in-memory schema store that is refreshed after each test.
 */
public class TestKinesisToAvroRecordLocalSchemaStore {
  private static final Log LOG = LogFactory.getLog(TestKinesisToAvroRecordLocalSchemaStore.class);
  private SchemaStore store;
  protected boolean storeTableCreated = true;

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
    verifyReadWriteRecordsAsIndividualEvents(createRecords(1));
  }

  @Test
  public void testReadWriteMultipleRequests() throws Exception {
    verifyReadWriteRecordsAsIndividualEvents(createRecords(2));
  }

  @SafeVarargs
  private final void verifyReadWriteRecordsAsIndividualEvents(Map<String, Object>... records)
    throws Exception {
    // setup the mocks/fakes
    List<KinesisEvent> events = new ArrayList<>();
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      events.add(getEvent(fields));
      store = createSchemaStore(store, fields);
    }
    verifyReadWriterEventsWithoutMalformed(store, events, records);
  }

  @Test
  public void testReadWriteMultipleRecordInSameRequest() throws Exception {
    Map<String, Object>[] records = createRecords(2);

    // setup the mocks/fakes
    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    verifyReadWriterEventsWithoutMalformed(store, Lists.newArrayList(event), records);
  }

  private void verifyReadWriterEventsWithoutMalformed(SchemaStore store, List<KinesisEvent> events,
    Map<String, Object>[] records) throws Exception {
    FirehoseClientProperties props = getClientProperties();
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);

    // do the writing
    Pair<List<KinesisRequest>, List<PutRecordBatchRequest>> requests =
      doSetupAndWrite(props, null, producer, store, null,
        events.toArray(new KinesisEvent[events.size()]));

    List<PutRecordBatchRequest> malformed = requests.getValue();
    assertEquals("There were some malformed requests: " + malformed, 0, malformed.size());

    List<KinesisRequest> parsed = requests.getKey();
    assertEquals("Got wrong number of parsed records. Received: " + parsed, records.length,
      parsed.size());
    verifyParsedRecords(parsed, records);

    // kinesis write verification, beyond the added records tracking... just in case
    Mockito.verify(producer, Mockito.times(records.length))
           .addUserRecord(Mockito.anyString(), Mockito.anyString(), Mockito.any(ByteBuffer.class));
    Mockito.verify(producer, Mockito.times(events.size())).flushSync();
    usedStore();
  }

  private Pair<KinesisEvent, SchemaStore> createStoreAndSingleEvent(Map<String, Object>[] records)
    throws Exception {
    // add multiple records to the same event
    KinesisEvent event = null;
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      if (event == null) {
        event = getEvent(fields);
      } else {
        event.getRecords().addAll(getEvent(fields).getRecords());
      }
      store = createSchemaStore(store, fields);
    }
    return new Pair<>(event, store);
  }

  /**
   * Batch puts only support 500 records, so we ensure that we flush the writes before we get to
   * 500.
   */
  @Test
  public void testEarlyFlush() throws Exception {
    int recordCount = 500;
    Map<String, Object>[] records = createMalformedRecords(recordCount);

    // setup the mocks/fakes
    FirehoseClientProperties props = getClientProperties();
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    Mockito.when(result.getFailedPutCount()).thenReturn(0);

    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    Pair<List<KinesisRequest>, List<PutRecordBatchRequest>> requests =
      doSetupAndWrite(props, client, producer, store, result, event);
    assertEquals(0, requests.getKey().size());

    assertEquals(recordCount - 10, requests.getValue().get(0).getRecords().size());
    assertEquals(10, requests.getValue().get(1).getRecords().size());
    verifyMalformedRecords(requests.getValue(), event);

    // verify the mocks
    Mockito.verify(client, Mockito.times(2))
           .putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    Mockito.verify(result, Mockito.times(2)).getFailedPutCount();
    didNotUseStore();
  }

  private Map<String, Object>[] createMalformedRecords(int count) {
    Map<String, Object>[] events = createRecords(count);
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


  @Test
  public void testRetryFailedRequests() throws Exception {
    int recordCount = 5;
    int failureCount = 2;
    Map<String, Object>[] records = createMalformedRecords(recordCount);

    // setup the mocks/fakes
    FirehoseClientProperties props = getClientProperties();
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);

    // setup the responses as 2 failures and then all success
    PutRecordBatchResult result = new PutRecordBatchResult();
    PutRecordBatchResponseEntry success = new PutRecordBatchResponseEntry()
      .withRecordId("written-id");
    PutRecordBatchResponseEntry failure =
      new PutRecordBatchResponseEntry().withErrorCode("1").withErrorMessage("Some error");
    result.withRequestResponses(success, failure, success, success, failure);
    result.setFailedPutCount(failureCount);

    PutRecordBatchResult secondResult = new PutRecordBatchResult();
    secondResult.withRequestResponses(success, success);
    secondResult.setFailedPutCount(0);

    List<PutRecordBatchResult> results = Lists.newArrayList(result, secondResult);
    List<Pair<PutRecordBatchRequest, List<Record>>> requests = new ArrayList<>();
    Mockito.when(client.putRecordBatch(Mockito.any(PutRecordBatchRequest.class)))
           .then(invocation -> {
             PutRecordBatchRequest request = (PutRecordBatchRequest) invocation.getArguments()[0];
             List<Record> records1 = Lists.newArrayList(request.getRecords());
             requests.add(new Pair<>(request, records1));
             return results.remove(0);
           });

    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    // update the writer with the test properties
    LambdaRawRecordToAvro writer = new LambdaRawRecordToAvro();
    writer.setupForTesting(props, client, store, producer);

    // actually run the test
    writer.handleEvent(event);

    assertEquals(2, requests.size());
    assertTrue("Didn't reuse batch request for second write",
      requests.get(0).getKey() == requests.get(1).getKey());
    assertEquals(recordCount, requests.get(0).getValue().size());
    assertEquals(failureCount, requests.get(1).getValue().size());
    // verify the records we wrote
    PutRecordBatchRequest copyRequest =
      new PutRecordBatchRequest().withRecords(requests.get(0).getValue());
    verifyMalformedRecords(Lists.newArrayList(copyRequest), event);
    // failed records are the only ones retained
    event.getRecords().remove(3);
    event.getRecords().remove(2);
    event.getRecords().remove(0);

    verifyMalformedRecords(Lists.newArrayList(requests.get(1).getKey()), event);

    // verify the mocks
    Mockito.verify(client, Mockito.times(2))
           .putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    Mockito.verify(producer).flushSync();
    didNotUseStore();
  }

  private void verifyParsedRecords(List<KinesisRequest> requests, Map<String, Object>[] records)
    throws IOException {
    byte[] data = combineRecords(requests.stream().map(request -> request.buff));

    FirehoseRecordReader<GenericRecord> reader =
      new FirehoseRecordReader<>(new SeekableByteArrayInput(data));
    for (int i = 0; i < records.length; i++) {
      LOG.info("Reading and verifying record: " + i);
      // verify that we read the next record in order of it being written
      GenericRecord avro = reader.next();
      AvroRecordDecoder decoder = new AvroRecordDecoder(avro);
      String orgId = decoder.getMetadata().getOrgID();
      String expectedPrefix = SchemaNameUtils.getCustomerNamespace(orgId);
      String fullName = avro.getSchema().getFullName();
      assertTrue("Expected schema full name (" + fullName + ") to start with " + expectedPrefix,
        fullName.startsWith(expectedPrefix));
    }
  }

  private void verifyMalformedRecords(List<PutRecordBatchRequest> requests,
    KinesisEvent event) throws IOException, InterruptedException {
    byte[] data =
      combineRecords(requests
        .stream()
        .flatMap(request ->
          request.getRecords().stream())
        .map(Record::getData));

    FirehoseRecordReader<Malformed> reader = new FirehoseRecordReader<>(new SeekableByteArrayInput
      (data), Malformed.class);
    List<KinesisEvent.KinesisEventRecord> sent = event.getRecords();
    for (KinesisEvent.KinesisEventRecord aSent : sent) {
      Malformed record = reader.next();
      ByteBuffer bytes = record.getRecordContent();
      assertEquals(aSent.getKinesis().getData(), bytes);
    }

  }

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

  private Pair<List<KinesisRequest>, List<PutRecordBatchRequest>> doSetupAndWrite(
    FirehoseClientProperties props, AmazonKinesisFirehoseClient client, KinesisProducer producer,
    SchemaStore store, PutRecordBatchResult result, KinesisEvent... events) throws IOException {
    boolean noClient = false, noResult = false;
    if (client == null) {
      noClient = true;
      client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    }

    if (result == null) {
      noResult = true;
      result = Mockito.mock(PutRecordBatchResult.class);
    }

    final PutRecordBatchResult fResult = result;
    LambdaRawRecordToAvro writer = new LambdaRawRecordToAvro();
    // update the writer with the test properties
    writer.setupForTesting(props, client, store, producer);

    List<PutRecordBatchRequest> malformedRequests = new ArrayList<>();
    Mockito.when(client.putRecordBatch(Mockito.any(PutRecordBatchRequest.class)))
           .then(invocation -> {
             malformedRequests.add((PutRecordBatchRequest) invocation.getArguments()[0]);
             return fResult;
           });

    List<KinesisRequest> parsedRequests = new ArrayList<>();
    Mockito.when(producer.addUserRecord(Mockito.anyString(), Mockito.anyString(), Mockito.any
      (ByteBuffer.class))).then(invoke -> {
      parsedRequests.add(new KinesisRequest(invoke.getArguments()));
      return Futures.immediateFuture(null);
    });

    // actually run the test
    for (KinesisEvent event : events) {
      writer.handleEvent(event);
    }

    if (noClient)
      Mockito.verifyZeroInteractions(client);
    if (noResult)
      Mockito.verifyZeroInteractions(result);

    return new Pair<>(parsedRequests, malformedRequests);
  }

  private class KinesisRequest {
    ByteBuffer buff;
    String stream;
    String key;

    public KinesisRequest(String stream, String key, ByteBuffer buff) {
      this.buff = buff;
      this.stream = stream;
      this.key = key;
    }

    public KinesisRequest(Object[] arguments) {
      this((String) arguments[0], (String) arguments[1], (ByteBuffer) arguments[2]);
    }
  }

  /**
   * Create a schema that matches the fields in the event. Assumes all fields are string type
   *
   * @return new store with that event
   */
  private SchemaStore createSchemaStore(SchemaStore store, Map<String, Object> event)
    throws Exception {
    String orgId = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
    String metricType = (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
    if (orgId == null || metricType == null) {
      return null;
    }

    FirehoseClientProperties props = getClientProperties();
    if (store == null) {
      store = props.createSchemaStore();
    }

    SchemaTestUtils.addNewOrg(store, orgId, metricType);
    return store;
  }

  protected FirehoseClientProperties getClientProperties() throws Exception {
    Properties props = getMockProps();
    return FirehoseClientProperties.createForTesting(props, store);
  }

  protected Properties getMockProps() {
    Properties props = new Properties();
    props.put(FirehoseClientProperties.FIREHOSE_URL, "url");
    props.put(FirehoseClientProperties.PARSED_STREAM_NAME, "stream");
    props.put(FirehoseClientProperties.FIREHOSE_MALFORMED_STREAM_NAME, "malformed");
    return props;
  }

  private KinesisEvent getEvent(Map<String, Object> fields) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JSON.std.write(fields, baos);
    baos.close();
    ByteBuffer data = ByteBuffer.wrap(baos.toByteArray());
    KinesisEvent event = Mockito.mock(KinesisEvent.class);
    KinesisEvent.KinesisEventRecord kinesisRecord = new KinesisEvent.KinesisEventRecord();
    KinesisEvent.Record record = new KinesisEvent.Record();
    record.setData(data);
    kinesisRecord.setKinesis(record);
    Mockito.when(event.getRecords()).thenReturn(Lists.newArrayList(kinesisRecord));
    return event;
  }

  private static Map<String, Object>[] createRecords(int count) {
    Map[] records = new Map[count];
    for (int i = 0; i < count; i++) {
      String uuid = UUID.randomUUID().toString();
      LOG.debug("Using UUID - " + uuid);
      records[i] = SchemaTestUtils.getBaseFields("org" + i + "_" + uuid, "mt" + i + "_" + uuid);
    }
    return (Map<String, Object>[]) records;
  }

  private void usedStore() {
    this.storeTableCreated = true;
  }

  private void didNotUseStore() {
    this.storeTableCreated = false;
  }
}
