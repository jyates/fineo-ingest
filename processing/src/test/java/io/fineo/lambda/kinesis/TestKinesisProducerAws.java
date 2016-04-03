package io.fineo.lambda.kinesis;

import io.fineo.lambda.e2e.resources.kinesis.KinesisStreamManager;
import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.util.AwsTestRule;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Run the kinesis producer against AWS
 */
@Category(AwsDependentTests.class)
public class TestKinesisProducerAws {

  private static final Duration TIMEOUT = Duration.ofMinutes(2);
  private static final Duration INTERVAL = Duration.ofSeconds(10);
  @ClassRule
  public static AwsCredentialResource credentials = new AwsCredentialResource();
  @ClassRule
  public static AwsTestRule tests = new AwsTestRule();
  private static KinesisStreamManager manager;

  @BeforeClass
  public static void setup() throws Exception {
    manager = new KinesisStreamManager(credentials.getProvider(), new
      ResultWaiter.ResultWaiterFactory(TIMEOUT, INTERVAL));
  }

  @AfterClass
  public static void teardown() throws Exception {
    manager.deleteStreams();
  }

  @Test
  public void readWriteStream() throws Exception {
    String streamName = "integration-test-" + UUID.randomUUID().toString();
    manager.setup(tests.getRegion(), streamName, 1);

    // write some data to the stream
    GenericRecord data = SchemaTestUtils.createRandomRecord();
    KinesisProducer producer = new KinesisProducer(manager.getKinesis(), 1);
    producer.add(streamName, "a", data);
    MultiWriteFailures<GenericRecord> failures = producer.flush();
    assertFalse("Some actions failed: " + failures.getActions(), failures.any());

    // verify that the data we wrote is what we read back in
    List<ByteBuffer> writes = manager.getEvents(streamName, false);
    assertEquals(1, writes.size());

    // verify the data actually matches the record
    List<GenericRecord> read = LambdaTestUtils.readRecords(writes.get(0));
    assertEquals("Wrong number of records in kinesis stream", Lists.newArrayList(read.get(0)),
      read);
    assertEquals("Read record doesn't match written", data, read.get(0));
  }
}