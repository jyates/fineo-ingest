package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Mock based testing
 */
public class TestDynamoTableTimeManager {

  private String prefix = "table_prefix";

  /**
   * Closer to if a customer sends in a timestamp in seconds
   */
  @Test
  public void testShortTimestamp() throws Exception {
    long ts = 1475001526;
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    DynamoTableTimeManager time = new DynamoTableTimeManager(client, prefix, 10);
    String name = time.getTableName(ts);
    DynamoTableNameParts parts = DynamoTableNameParts.parse(name, false);
    assertTrue(ts > parts.getStart());
    assertTrue(ts < parts.getEnd());
    Duration duration = Duration.between(Instant.ofEpochMilli(parts.getStart()), Instant
      .ofEpochMilli(parts.getEnd()));
    assertEquals(7, duration.toDays());
  }
}
