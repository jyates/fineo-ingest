package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Manages the actual table creation + schema
 */
public class DynamoTableManager {

  private static final Log LOG = LogFactory.getLog(DynamoTableManager.class);
  public static final String SEPARATOR = "_";
  static final Joiner TABLE_NAME_PARTS_JOINER = Joiner.on(SEPARATOR);
  private static final Duration TABLE_TIME_LENGTH = Duration.ofDays(7);
  private final String prefix;
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneOffset ZONE = ZoneOffset.UTC;

  private final AmazonDynamoDBAsyncClient client;

  public DynamoTableManager(AmazonDynamoDBAsyncClient client, String prefix) {
    this.prefix = prefix;
    this.client = client;
  }

  /**
   * Split the full name into its prefix and start time, essentially the non-inlcusive prefix of
   * the table.
   *
   * @param fullTableName full name of the table, in the format described in the
   *                      {@link DynamoTableManager}
   * @return the non-inlcusive prefix of the table table to search in AWS
   */
  @VisibleForTesting
  static String getPrefixAndStart(String fullTableName) {
    return fullTableName.substring(0, fullTableName.lastIndexOf(SEPARATOR));
  }

  /**
   * @param range
   * @return all table names that are required to cover the time range
   */
  public List<Pair<String, Range<Instant>>> getExistingTableNames(Range<Instant> range) {
    List<Pair<String, Range<Instant>>> tables = new ArrayList<>();
    Instant start = range.getStart();
    while (start.isBefore(range.getEnd())) {
      Range<Instant> startEnd = getStartEnd(start);
      String tableName = getTableName(startEnd);
      try {
        client.describeTable(tableName);
        tables.add(new ImmutablePair<>(tableName, startEnd));
      } catch (ResourceNotFoundException e) {
        LOG.debug("Skipping table: " + tableName + " because it doesn't exist!");
        // check to see if we are asking too far in the future at which point we should stop looking
        ListTablesResult currentTables = client.listTables(tableName);
        if (currentTables.getTableNames().size() == 0 ||
            !currentTables.getTableNames().get(0).startsWith(prefix))
          break;
      }
      start = startEnd.getEnd();
    }

    return tables;
  }

  @VisibleForTesting
  String getTableName(long ts) {
    // map this time to the start of the week
    return getTableName(getStartEnd(Instant.ofEpochMilli(ts)));
  }

  private String getTableName(Range<Instant> range) {
    long start = range.getStart().toEpochMilli();
    long end = range.getEnd().toEpochMilli();
    // build the table name
    return TABLE_NAME_PARTS_JOINER.join(prefix, start, end);
  }

  @VisibleForTesting
  static Range<Instant> getStartEnd(Instant rowTime) {
    LocalDateTime time = LocalDateTime.ofInstant(rowTime, UTC).truncatedTo(ChronoUnit.DAYS);
    int day = time.getDayOfYear();
    // day of the year starts at 1, not 0, so we adjust to offset to make mod work nice
    int weekOffset = (day - 1) % 7;
    Instant start = time.minusDays(weekOffset).toInstant(ZONE);
    return new Range<>(start, start.plus(TABLE_TIME_LENGTH));
  }

  public boolean tableExists(String fullTableName) {
    String tableAndStart = DynamoTableManager.getPrefixAndStart(fullTableName);
    Stream<String> tables = TableUtils.getTables(client, tableAndStart, 1);
    // we have the table already, we are done
    return tables.count() > 0;
  }
}
