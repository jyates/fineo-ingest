package io.fineo.lambda.dynamo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.dynamo.DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER;

/**
 * Handles parsing the name of a table
 * <p>
 * In short, there are four main pieces:
 * <ol>
 * <li>prefix</li>
 * <li>start time  - millis since epoch</li>
 * <li>end time  - millis since epoch</li>
 * <li>month - when writes were made to the table</li>
 * <li>year - when writes were made to the table</li>
 * </ol>
 * This layout allows us to manage tables that have not been written to recently (effectively
 * time-stepping the tables and allowing us to delete older tables, saving capacity and money)
 * and also see if we have 'bad' tenants that are writing to tables that are far back in time.
 * </p>
 */
public class DynamoTableNameParts {
  private String prefix;
  private long start, end, writeMonth, writeYear;

  public static DynamoTableNameParts create(String prefix, DynamoTableTimeManager.TableTimeInfo
    info) {
    long writeMonth = info.writeTimeRange.getKey().getValue();
    long writeYear = info.writeTimeRange.getValue();
    long start = info.dataTimeRange.getStart().toEpochMilli();
    long end = info.dataTimeRange.getEnd().toEpochMilli();
    return new DynamoTableNameParts(prefix, start, end, writeMonth, writeYear);
  }

  /**
   * Pare the table name without prefix validation. Its assumed that you did the validation
   * somewhere previously... didn't you?
   *
   * @param tableName     name to parse
   * @param includePrefix if we should attempt to parse the prefix out of the table name. This is
   *                      extra overhead that you may or may not care about.
   * @return the parts of the table name that are relevant to the time range
   */
  public static DynamoTableNameParts parse(String tableName, boolean includePrefix) {
    String[] parts = tableName.split(DynamoTableTimeManager.SEPARATOR);
    Iterator<String> iter = Lists.reverse(newArrayList(parts)).iterator();
    long year = Long.valueOf(iter.next());
    long month = Long.valueOf(iter.next());
    long end = Long.valueOf(iter.next());
    long start = Long.valueOf(iter.next());
    String prefix = null;
    if (includePrefix) {
      String suffix = TABLE_NAME_PARTS_JOINER.join(start, end, month, year);
      prefix = tableName.substring(0, tableName.length() - 1 - suffix.length());
    }
    return new DynamoTableNameParts(prefix, start, end, month, year);
  }

  public static DynamoTableNameParts parse(String prefix, String tableName) {
    Preconditions.checkState(tableName.substring(0, prefix.length()).equals(prefix),
      "Table %s does not start with expected prefix: %s", tableName, prefix);
    // skip past the separator between the prefix and name
    tableName = tableName.substring(prefix.length() + 1);
    String[] parts = tableName.split(DynamoTableTimeManager.SEPARATOR);
    if (parts.length != 4) {
      throw new IllegalArgumentException("Table: " + tableName + " is not a managed table!");
    }
    Iterator<String> iter = Iterators.forArray(parts);
    return new DynamoTableNameParts(prefix,
      Long.valueOf(iter.next()),
      Long.valueOf(iter.next()),
      Long.valueOf(iter.next()),
      Long.valueOf(iter.next()));
  }

  public DynamoTableNameParts(String prefix, long start, long end, long writeMonth,
    long writeYear) {
    this.prefix = prefix;
    this.start = start;
    this.end = end;
    this.writeMonth = writeMonth;
    this.writeYear = writeYear;
  }

  public String getName() {
    // build the table name
    return TABLE_NAME_PARTS_JOINER.join(prefix, start, end, writeMonth, writeYear);
  }

  public String getPrefix() {
    return prefix;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getWriteMonth() {
    return writeMonth;
  }

  public long getWriteYear() {
    return writeYear;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoTableNameParts))
      return false;

    DynamoTableNameParts that = (DynamoTableNameParts) o;

    if (start != that.start)
      return false;
    if (end != that.end)
      return false;
    if (writeMonth != that.writeMonth)
      return false;
    if (writeYear != that.writeYear)
      return false;
    return prefix != null ? prefix.equals(that.prefix) : that.prefix == null;

  }

  @Override
  public int hashCode() {
    int result = prefix != null ? prefix.hashCode() : 0;
    result = 31 * result + (int) (start ^ (start >>> 32));
    result = 31 * result + (int) (end ^ (end >>> 32));
    result = 31 * result + (int) (writeMonth ^ (writeMonth >>> 32));
    result = 31 * result + (int) (writeYear ^ (writeYear >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "DynamoTableNameParts{" +
           "prefix='" + prefix + '\'' +
           ", start=" + start +
           ", end=" + end +
           ", writeMonth=" + writeMonth +
           ", writeYear=" + writeYear +
           '}';
  }
}
