package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

/**
 * Page through table names matching the given prefix
 */
public class TableNamePager extends BasePager<String> {

  private static final Logger LOG = LoggerFactory.getLogger(TableNamePager.class);

  private final String prefix;
  private final AmazonDynamoDBAsyncClient dynamo;
  private final int pageSize;
  private String pageStart;

  public TableNamePager(String prefix, AmazonDynamoDBAsyncClient dynamo, int pageSize) {
    this(prefix, null, dynamo, pageSize);
  }

  /**
   * @param prefix
   * @param startKey non-inclusive start key. Must be: (a) at least 3 characters long, and (b)
   *                 begin with the specified prefix
   * @param dynamo
   * @param pageSize
   */
  public TableNamePager(String prefix, String startKey, AmazonDynamoDBAsyncClient dynamo,
    int pageSize) {
    this.dynamo = dynamo;
    this.pageSize = pageSize;
    this.prefix = prefix;
    if (startKey != null) {
      Preconditions.checkArgument(startKey.startsWith(prefix),
        "Start key [%s] does not start with prefix [%s]", prefix, startKey);
      Preconditions
        .checkArgument(startKey.length() > 3, "Start key must be at least 3 characters long!");
      this.pageStart = startKey;
    } else {
      this.pageStart = prefix;
    }
  }

  @Override
  public void page(Queue<String> queue) {
    LOG.trace("Paging next batch of tables. Prefix: " + this.prefix + ", start: " + pageStart);
    ListTablesResult tables = this.pageStart == null || this.pageStart.length() < 3 ?
                              dynamo.listTables(pageSize) : dynamo.listTables(pageStart, pageSize);
    LOG.trace("Got next page: " + tables);

    if (prefix == null || prefix.equals("")) {
      queue.addAll(tables.getTableNames());
    } else {
      boolean passedPrefix = false;
      for (String name : tables.getTableNames()) {
        if (name.startsWith(prefix)) {
          queue.add(name);
        } else if (name.compareTo(prefix) > 0) {
          passedPrefix = true;
          break;
        }
      }
      if (passedPrefix) {
        complete();
        return;
      }
    }
    if (noMoreTables(tables)) {
      complete();
      return;
    }
    pageStart = tables.getLastEvaluatedTableName();
    batchComplete();
  }

  private boolean noMoreTables(ListTablesResult tables) {
    return tables.getLastEvaluatedTableName() == null ||
           tables.getLastEvaluatedTableName().isEmpty();
  }
}
