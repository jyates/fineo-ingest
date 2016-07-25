package io.fineo.lambda.dynamo;

import static io.fineo.lambda.dynamo.DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER;

/**
 *
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

  public static DynamoTableNameParts parse(String tableName) {
    String[] parts = tableName.split(DynamoTableTimeManager.SEPARATOR);
    if (parts.length != 5) {
      throw new IllegalArgumentException("Table: " + tableName + " is not a managed table!");
    }
    return new DynamoTableNameParts(parts[0], Long.valueOf(parts[1]), Long.valueOf(parts[2]),
      Long.valueOf(parts[3]), Long.valueOf(parts[4]));
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
