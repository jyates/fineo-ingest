package io.fineo.lambda;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.avro.FirehoseBatchWriter;
import io.fineo.lambda.avro.FirehoseClientProperties;
import io.fineo.lambda.avro.LambdaRawRecordToAvro;
import io.fineo.lambda.storage.AvroToDynamoWriter;
import io.fineo.lambda.storage.LambdaAvroToStorage;
import io.fineo.lambda.storage.MultiWriteFailures;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class TestEndToEndLambda {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambda.class);

  /**
   * Path where there are no issues with records.
   *
   * @throws Exception on failure
   */
  @Test
  public void testHappyPath() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    Map<String, Object> json = createRecord(store);

    // handle amazon's conversion from data to a new kinesis event
    ByteBuffer dataOut = convertToAvroEncodedBytes(json, store);
    assertTrue(dataOut.remaining() > 0);
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(dataOut);

    GenericRecord record = storeAvroEncodedBytes(event);

    // Validation
    // -----------
    // org/schema naming
    LambdaTestUtils.verifyRecordMatchesExpectedNaming(store, record);
    LOG.debug("Comparing \nJSON: " + json + "\nRecord: " + record);

    // rest of the field validation
    AvroRecordDecoder decoder = new AvroRecordDecoder(record);
    Metric metric = store.getMetricMetadata(decoder.getMetadata());
    Map<String, List<String>> names =
      metric.getMetadata().getCanonicalNamesToAliases();
    json.entrySet()
        .stream()
        .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()))
        .forEach(entry -> {
          // search through each of the aliases to find a matching name in the record
          String aliasName = entry.getKey();
          String cname = null;
          for (Map.Entry<String, List<String>> nameToAliases : names.entrySet()) {
            if (nameToAliases.getValue().contains(aliasName)) {
              cname = nameToAliases.getKey();
              break;
            }
          }
          // ensure the value matches
          assertNotNull(cname);
          assertEquals("JSON: " + json + "\nRecord: " + record,
            entry.getValue(), record.get(cname));
        });
  }

  private GenericRecord storeAvroEncodedBytes(KinesisEvent event) throws IOException {
    FirehoseBatchWriter errors = Mockito.mock(FirehoseBatchWriter.class);
    FirehoseBatchWriter s3 = Mockito.mock(FirehoseBatchWriter.class);
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);
    MultiWriteFailures failures = Mockito.mock(MultiWriteFailures.class);
    List<GenericRecord> received = new ArrayList<>(1);
    Mockito.doAnswer(invocationOnMock -> {
      received.add((GenericRecord) invocationOnMock.getArguments()[0]);
      return null;
    }).when(dynamo).write(Mockito.any(GenericRecord.class));
    Mockito.when(failures.any()).thenReturn(false);
    Mockito.when(dynamo.flush()).thenReturn(failures);

    LambdaAvroToStorage avroToStorage = new LambdaAvroToStorage();
    avroToStorage.setupForTesting(s3, errors, dynamo);
    avroToStorage.handleEventInternal(event);

    Mockito.verify(s3).addToBatch(Mockito.any());
    Mockito.verify(s3).flush();

    Mockito.verify(dynamo).flush();
    Mockito.verify(dynamo).write(Mockito.any());

    Mockito.verifyZeroInteractions(errors);
    return received.get(0);
  }

  private ByteBuffer convertToAvroEncodedBytes(Map<String, Object> json, SchemaStore store)
    throws IOException {
    LambdaRawRecordToAvro rawToAvro = new LambdaRawRecordToAvro();
    FirehoseBatchWriter errors = Mockito.mock(FirehoseBatchWriter.class);
    KinesisProducer pipe = Mockito.mock(KinesisProducer.class);
    FirehoseClientProperties props = getTestProperties();
    rawToAvro.setupForTesting(props, null, store, pipe, errors);

    // catch the record in the pipe
    List<ByteBuffer> received = new ArrayList<>();
    Mockito.when(pipe.addUserRecord(Mockito.any(), Mockito.any(), Mockito.any()))
           .then(invocationOnMock -> {
             received.add((ByteBuffer) invocationOnMock.getArguments()[2]);
             return null;
           });

    // send the event to the lambda instance
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(json);
    rawToAvro.handleEventInternal(event);

    // ensure we got the data we expected
    assertEquals(1, received.size());
    Mockito.verify(pipe).addUserRecord(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    Mockito.verify(errors).flush();
    return received.get(0);
  }

  private Map<String, Object> createRecord(SchemaStore store) throws Exception {
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    // add the event to the managed schema
    LambdaTestUtils.updateSchemaStore(store, json);
    return json;
  }

  private FirehoseClientProperties getTestProperties() {
    Properties props = new Properties();
    props.put(FirehoseClientProperties.PARSED_STREAM_NAME, "parsed-stream");
    return new FirehoseClientProperties(props);
  }
}