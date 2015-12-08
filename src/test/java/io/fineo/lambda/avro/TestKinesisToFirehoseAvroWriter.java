package io.fineo.lambda.avro;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.Lists;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.avro.SchemaUtils;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import javafx.util.Pair;
import org.apache.avro.file.FirehoseReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test running the lambda function locally and that we we resuscitate the records
 */
public class TestKinesisToFirehoseAvroWriter {
  private static final Log LOG = LogFactory.getLog(TestKinesisToFirehoseAvroWriter.class);

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

  private void verifyReadWriteRecordsAsIndividualEvents(Map<String, Object>... records)
    throws Exception {
    // setup the mocks/fakes
    FirehoseClientProperties props = getBasicProperties();
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);

    List<KinesisEvent> events = new ArrayList<>();
    SchemaStore store = null;
    for (Map<String, Object> fields : records) {
      events.add(getEvent(fields));
      store = createSchemaStore(store, fields);
    }

    List<PutRecordBatchRequest> requests =
      doSetupAndWrite(props, client, store, result, events.toArray(new
        KinesisEvent[0]));

    assertEquals(records.length, requests.size());
    for (int i = 0; i < records.length; i++) {
      // verify number of records/requests
      PutRecordBatchRequest request = requests.get(i);
      assertEquals(1, request.getRecords().size());
    }
    verifyRecords(requests, records);

    // ensure that we did everything we should have via mocks
    Mockito.verify(client, Mockito.times(records.length))
           .putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    Mockito.verify(result, Mockito.times(records.length)).getFailedPutCount();
  }

  @Test
  public void testReadWriteMultipleRecordInSameRequest() throws Exception {
    Map<String, Object>[] records = createRecords(2);

    // setup the mocks/fakes
    FirehoseClientProperties props = getBasicProperties();
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);

    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    List<PutRecordBatchRequest> requests = doSetupAndWrite(props, client, store, result, event);
    assertEquals(1, requests.size());
    assertEquals(2, requests.get(0).getRecords().size());
    verifyRecords(requests, records);

    // verify the mocks
    Mockito.verify(client, Mockito.times(1))
           .putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    Mockito.verify(result, Mockito.times(1)).getFailedPutCount();
  }

  private Pair<KinesisEvent, SchemaStore> createStoreAndSingleEvent(Map<String, Object>[] records)
    throws IOException, OldSchemaException {
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
    Map<String, Object>[] records = createRecords(recordCount);

    // setup the mocks/fakes
    FirehoseClientProperties props = getBasicProperties();
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);

    Pair<KinesisEvent, SchemaStore> created = createStoreAndSingleEvent(records);
    KinesisEvent event = created.getKey();
    SchemaStore store = created.getValue();

    List<PutRecordBatchRequest> requests = doSetupAndWrite(props, client, store, result, event);
    assertEquals(2, requests.size());
    assertEquals(recordCount-10, requests.get(0).getRecords().size());
    assertEquals(10, requests.get(1).getRecords().size());
    verifyRecords(requests, records);

    // verify the mocks
    Mockito.verify(client, Mockito.times(2))
           .putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    Mockito.verify(result, Mockito.times(2)).getFailedPutCount();
  }


  @Test
  public void testRetryFailedRequests() throws Exception {

  }

  private void verifyRecords(List<PutRecordBatchRequest> requests, Map<String, Object>[] records)
    throws IOException, InterruptedException {
    byte[] data = combineRecords(requests);
    FirehoseReader<GenericRecord> reader = new FirehoseReader<>(new SeekableByteArrayInput(data));
    for (int i = 0; i < records.length; i++) {
      LOG.info("Reading and verifying record: "+i);
      // verify that we read the next record in order of it being written
      GenericRecord avro = reader.next();
      String orgId = (String) records[i].get(SchemaBuilder.ORG_ID_KEY);
      String expectedPrefix = SchemaUtils.getCustomerNamespace(orgId);
      String fullName = avro.getSchema().getFullName();
      assertTrue("Expected schema full name (" + fullName + ") to start with " + expectedPrefix,
        fullName.startsWith(expectedPrefix));
    }
  }

  private List<PutRecordBatchRequest> doSetupAndWrite(FirehoseClientProperties props,
    AmazonKinesisFirehoseClient
      client, SchemaStore store, PutRecordBatchResult result, KinesisEvent... events)
    throws IOException {
    KinesisToFirehoseAvroWriter writer = new KinesisToFirehoseAvroWriter();
    // update the writer with the test properties
    writer.setupForTesting(props, client, store);

    List<PutRecordBatchRequest> requests = new ArrayList<>();
    Mockito.when(result.getFailedPutCount()).thenReturn(0);
    Mockito.when(client.putRecordBatch(Mockito.any(PutRecordBatchRequest.class)))
           .then(invocation -> {
             requests.add((PutRecordBatchRequest) invocation.getArguments()[0]);
             return result;
           });

    // actually run the test
    for (KinesisEvent event : events) {
      writer.handleEvent(event);
    }
    return requests;
  }


  private byte[] combineRecords(List<PutRecordBatchRequest> requests) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (PutRecordBatchRequest request : requests) {
      for (Record record : request.getRecords()) {
        ByteBuffer data = record.getData();
        byte[] raw = new byte[data.limit()];
        data.get(raw);
        data.clear();
        bos.write(raw);
      }
    }
    bos.close();
    return bos.toByteArray();
  }

  /**
   * Create a schema that matches the fields in the event. Assumes all fields are string type
   *
   * @param store
   * @param event
   * @return new store with that event
   */
  private SchemaStore createSchemaStore(SchemaStore store, Map<String, Object> event)
    throws IOException, OldSchemaException {
    String orgId = (String) event.get(SchemaBuilder.ORG_ID_KEY);
    String metricType = (String) event.get(SchemaBuilder.ORG_METRIC_TYPE_KEY);
    if (store == null) {
      return SchemaTestUtils.createStoreWithBooleanFields(orgId, metricType);
    }
    SchemaTestUtils.addNewOrg(store, orgId, metricType);
    return store;
  }

  private FirehoseClientProperties getBasicProperties() {
    Properties props = new Properties();
    props.put(FirehoseClientProperties.FIREHOSE_URL, "url");
    props.put(FirehoseClientProperties.FIREHOSE_STREAM_NAME, "stream");
    props.put(FirehoseClientProperties.FIREHOSE_MALFORMED_STREAM_NAME, "malformed");
    return new FirehoseClientProperties(props);
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

  private Map<String, Object>[] createRecords(int count) {
    Map<String, Object>[] records = new Map[count];
    for (int i = 0; i < count; i++) {
      String uuid = UUID.randomUUID().toString();
      LOG.debug("Using UUID - " + uuid);
      records[i] = SchemaTestUtils.getBaseFields("org" + i + "_" + uuid, "mt" + i + "_" + uuid);
    }
    return records;
  }
}
