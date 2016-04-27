package io.fineo.etl.lambda.chore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import io.fineo.lambda.handle.LambdaHandler;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Cleanup dynamo tables that have a write time exceeding the configured amount.
 * <p>
 * To prevent accidental misconfiguration of a 'too low' write time, the minimum amount of time a
 * table is retained is <b>1 week</b>
 * </p>
 */
public class CleanOldDynamoTablesImpl implements LambdaHandler<Object> {

  private static final Duration MIN_DURATION = Duration.of(1, ChronoUnit.WEEKS);
  private final Duration expirationTime;
  private final DynamoDB db;

  public CleanOldDynamoTablesImpl(DynamoDB dynamo, Duration expirationTime) {
    this.expirationTime =
      Duration.of(Math.max(MIN_DURATION.toMillis(), expirationTime.toMillis()), ChronoUnit.MILLIS);
    this.db = dynamo;
  }


  @Override
  public void handle(Object tick) throws IOException {
    Instant now = Instant.now();
    Instant retained = now.minus(expirationTime);
    TableCollection<ListTablesResult> tables = db.listTables();
  }
}
