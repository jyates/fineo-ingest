package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeUnit;

import static java.time.Instant.now;

/**
 * Manages the names of the tables based on the time:
 * <ol>
 * <li>Fineo Write Time (FWT): time we are making the write (generally, "now")</li>
 * <li>Client Write Time (CWT): timestamp of the write itself</li>
 * </ol>
 * The current, hardcoded, implementation (#startup) groups FWT by month and CWT by week, with
 * the generic name appearing as:
 * <tt>[prefix]_[start_millis]_[end_millis]_[write_month]_[write_year]</tt>
 */
public class DynamoTableTimeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoTableTimeManager.class);

  private final LoadingCache<String, Table> cache;
  public static final String SEPARATOR = "_";
  static final Joiner TABLE_NAME_PARTS_JOINER = Joiner.on(SEPARATOR);
  private static final Duration TABLE_TIME_LENGTH = Duration.ofDays(7);
  private final String prefix;
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneOffset ZONE = ZoneOffset.UTC;

  private final AmazonDynamoDBAsyncClient client;
  private final DynamoDB dynamo;

  @Inject
  public DynamoTableTimeManager(AmazonDynamoDBAsyncClient client,
    @Named(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX) String prefix,
    @Named(FineoProperties.DYNAMO_TABLE_MANAGER_CACHE_TIME) long timeoutMs) {
    this.prefix = Preconditions.checkNotNull(prefix,
      "Dynamo client write tables must have a prefix!");
    this.client = client;
    this.dynamo = new DynamoDB(client);
    this.cache = CacheBuilder.newBuilder()
                             .expireAfterWrite(timeoutMs, TimeUnit.MILLISECONDS)
                             .build(new CacheLoader<String, Table>() {
                               @Override
                               public Table load(String key) throws Exception {
                                 // create the table and attempt to describe it, "warming" the
                                 // result
                                 Table table = dynamo.getTable(key);
                                 try {
                                   table.describe();
                                 } catch (ResourceNotFoundException e) {
                                   LOG.info("Table: {} doesn't exist!", key);
                                 }
                                 return table;
                               }
                             });
  }

  public DynamoTableTimeManager(AmazonDynamoDBAsyncClient client, String prefix) {
    this(client, prefix, 10);
  }

  /**
   * Split the full name into its prefix and start time, essentially the non-inclusive prefix of
   * the table.
   *
   * @param fullTableName full name of the table, in the format described in the
   *                      {@link DynamoTableTimeManager}
   * @return the non-inclusive prefix of the table for table name AWS table name scanning
   */
  @VisibleForTesting
  static String getPrefixAndStart(String fullTableName) {
    return fullTableName.substring(0, fullTableName.lastIndexOf(SEPARATOR));
  }

  /**
   * @param range
   * @return all table names that are required to cover the time range
   */
  public List<Pair<String, Range<Instant>>> getCoveringTableNames(final Range<Instant> range) {
    List<Pair<String, Range<Instant>>> tables = new ArrayList<>();
    Range<Instant> clientInitialRange = getClientTimestampStartEnd(range.getStart());
    String startKey = TABLE_NAME_PARTS_JOINER
      .join(prefix, clientInitialRange.getStart().toEpochMilli(),
        clientInitialRange.getEnd().toEpochMilli());
    Iterator<String> results = TableUtils.getTables(client, startKey, prefix, 5);
    if (!results.hasNext()) {
      return tables;
    }
    Instant tableStart;
    Instant tableEnd;
    while (results.hasNext()) {
      String name = results.next();
      DynamoTableNameParts parts = DynamoTableNameParts.parse(prefix, name);
      tableStart = Instant.ofEpochMilli(parts.getStart());
      tableEnd = Instant.ofEpochMilli(parts.getEnd());
      if (!overlaps(range, tableStart, tableEnd)) {
        break;
      }
      Range tableRange = new Range<>(tableStart, tableEnd);
      tables.add(new ImmutablePair<>(name, tableRange));
    }

    return tables;
  }

  private boolean overlaps(Range<Instant> range, Instant start, Instant end) {
    Instant rangeStart = range.getStart();
    Instant rangeEnd = range.getEnd();
    return rangeStart.isBefore(end) && start.isBefore(rangeEnd);
  }

  public String getTableName(long ts) {
    return getTableName(now(), ts);
  }

  public String getTableName(Instant writeTime, long dataTimestamp) {
    TableTimeInfo info = getTableInfo(writeTime, dataTimestamp);
    return DynamoTableNameParts.create(prefix, info).getName();
  }

  private TableTimeInfo getTableInfo(Instant writeTime, long dataTimestamp) {
    TableTimeInfo info = new TableTimeInfo();
    info.dataTimeRange = getClientTimestampStartEnd(Instant.ofEpochMilli(dataTimestamp));
    info.writeTimeRange = getFineoWriteTimestampStartEnd(writeTime);
    return info;
  }

  private static Pair<Month, Integer> getFineoWriteTimestampStartEnd(Instant rowTime) {
    LocalDateTime time = LocalDateTime.ofInstant(rowTime, UTC);
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

  public boolean tableExists(String name) {
    try {
      // use a cache so we can avoid lots of network calls (which can cause throttling)
      Table table = cache.getUnchecked(name);
      // it has a description and we checked within the specified timeout
      if (table.getDescription() != null) {
        return true;
      }
      TableDescription desc = table.describe();
      return desc != null;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }

  public void updateTableReference(Table table) {
    this.cache.put(table.getTableName(), table);
  }

  public class TableTimeInfo {
    Pair<Month, Integer> writeTimeRange;
    Range<Instant> dataTimeRange;
  }
}
