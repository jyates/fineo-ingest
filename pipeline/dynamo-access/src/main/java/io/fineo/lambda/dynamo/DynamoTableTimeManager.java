package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.TableNamePager;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.time.Instant.now;

/**
 * Manages the names of the tables based on the time:
 * <ol>
 * <li>Fineo Write Time (FWT): time we are making the write (generally, "now")</li>
 * <li>Client Write Time (CWT): timestamp of the write itself</li>
 * </ol>
 * The current, hardcoded, implementation (#startup) groups FWT by month and CWT by week.
 */
public class DynamoTableTimeManager {

  private static final Log LOG = LogFactory.getLog(DynamoTableTimeManager.class);
  public static final String SEPARATOR = "_";
  static final Joiner TABLE_NAME_PARTS_JOINER = Joiner.on(SEPARATOR);
  private static final Duration TABLE_TIME_LENGTH = Duration.ofDays(7);
  private final String prefix;
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneOffset ZONE = ZoneOffset.UTC;

  private final AmazonDynamoDBAsyncClient client;

  public DynamoTableTimeManager(AmazonDynamoDBAsyncClient client, String prefix) {
    this.prefix = prefix;
    this.client = client;
  }

  /**
   * Split the full name into its prefix and start time, essentially the non-inlcusive prefix of
   * the table.
   *
   * @param fullTableName full name of the table, in the format described in the
   *                      {@link DynamoTableTimeManager}
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
  public List<Pair<String, Range<Instant>>> getCoveringTableNames(Range<Instant> range) {
    List<Pair<String, Range<Instant>>> tables = new ArrayList<>();
    Instant start = range.getStart();
    Range<Instant> startEnd = getClientTimestampStartEnd(start);
    String startKey =
      TABLE_NAME_PARTS_JOINER.join(prefix, start.toEpochMilli(), startEnd.getEnd().toEpochMilli());
    TableNamePager pager = new TableNamePager(prefix, startKey, client, 5);
    Iterator<String> names = new PagingIterator<>(5, new PageManager<>(
      Lists.newArrayList(pager)));
    while (start.isBefore(range.getEnd()) && names.hasNext()) {
      String name = names.next();
      String[] parts = name.split(SEPARATOR);
      Instant tableStart = Instant.ofEpochMilli(Long.parseLong(parts[1]));
      start = Instant.ofEpochMilli(Long.parseLong(parts[2]));
      startEnd = new Range<>(tableStart, start);
      tables.add(new ImmutablePair<>(name, startEnd));
    }

    return tables;
  }

  public String getTableName(long ts) {
    return getTableName(now(), ts);
  }

  public String getTableName(Instant writeTime, long dataTimestamp) {
    TableTimeInfo info = getTableInfo(writeTime, dataTimestamp);
    long writeMonth = info.writeTimeRange.getKey().getValue();
    long writeYear = info.writeTimeRange.getValue();
    long start = info.dataTimeRange.getStart().toEpochMilli();
    long end = info.dataTimeRange.getEnd().toEpochMilli();
    // build the table name
    return TABLE_NAME_PARTS_JOINER.join(prefix, start, end, writeMonth, writeYear);
  }

  private TableTimeInfo getTableInfo(Instant writeTime, long dataTimestamp) {
    TableTimeInfo info = new TableTimeInfo();
    info.dataTimeRange = getClientTimestampStartEnd(Instant.ofEpochMilli(dataTimestamp));
    info.writeTimeRange = getFineoWriteTimestampStartEnd(writeTime);
    return info;
  }

  private static Pair<Month, Integer> getFineoWriteTimestampStartEnd(Instant rowTime) {
    LocalDateTime time = LocalDateTime.ofInstant(rowTime, UTC).truncatedTo(ChronoUnit.MONTHS);
    return new ImmutablePair<>(time.getMonth(), time.getYear());
  }

  private static Range<Instant> getClientTimestampStartEnd(Instant rowTime) {
    LocalDateTime time = LocalDateTime.ofInstant(rowTime, UTC).truncatedTo(ChronoUnit.DAYS);
    int day = time.getDayOfYear();
    // day of the year starts at 1, not 0, so we adjust to offset to make mod work nice
    int weekOffset = (day - 1) % 7;
    Instant start = time.minusDays(weekOffset).toInstant(ZONE);
    return new Range<>(start, start.plus(TABLE_TIME_LENGTH));
  }

  public boolean tableExists(String fullTableName) {
    String tableAndStart = DynamoTableTimeManager.getPrefixAndStart(fullTableName);
    Stream<String> tables = TableUtils.getTables(client, tableAndStart, 1);
    // we have the table already, we are done
    return tables.count() > 0;
  }

  public class TableTimeInfo {
    private Pair<Month, Integer> writeTimeRange;
    private Range<Instant> dataTimeRange;
  }
}
