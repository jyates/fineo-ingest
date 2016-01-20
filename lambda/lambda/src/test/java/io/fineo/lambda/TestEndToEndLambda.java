package io.fineo.lambda;

import io.fineo.internal.customer.Metric;
import io.fineo.lambda.util.EndToEndTestUtil;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static io.fineo.lambda.LambdaClientProperties.StreamType.*;
import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.getFirehoseStreamProperty;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class TestEndToEndLambda {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambda.class);
  public static final String AVRO_TO_STORAGE_STREAM_NAME = "parsed-stream";

  /**
   * Path where there are no issues with records.
   *
   * @throws Exception on failure
   */
  @Test
  public void testHappyPath() throws Exception {
    // Setup
    //-------
    Properties props = new Properties();
    // firehose outputs
    String malformed = "mal", dynamoErrors = "dynamoErrors", archived = "archived";
    props.setProperty(getFirehoseStreamProperty(RAW_PREFIX, PROCESSING_ERROR), malformed);
    props.setProperty(getFirehoseStreamProperty(STAGED_PREFIX, PROCESSING_ERROR), dynamoErrors);
    props.setProperty(getFirehoseStreamProperty(STAGED_PREFIX, ARCHIVE), archived);

    // between stage stream
    props.setProperty(LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME,
      AVRO_TO_STORAGE_STREAM_NAME);

    // Run
    // -----
    EndToEndTestUtil test = new EndToEndTestUtil(props);
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    test.run(json);

    // Validation
    // -----------
    // ensure that we didn't write any errors
    assertNull(test.getFirehoseWrites(malformed));
    assertNull(test.getFirehoseWrites(dynamoErrors));

    // ensure that we archived a single message
    assertEquals(1, test.getFirehoseWrites(archived).size());
    assertTrue(test.getFirehoseWrites(archived).get(0).hasRemaining());

    List<GenericRecord> records = test.getDynamoWrites();
    assertEquals("Got unexpected records: " + records, 1, records.size());
    GenericRecord record = records.get(0);
    SchemaStore store = test.getStore();

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
}