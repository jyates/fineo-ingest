package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Queue;

/**
 * Page through table names matching the given prefix
 */
public class TableNamePager extends BasePager<String> {

  private static final Log LOG = LogFactory.getLog(TableNamePager.class);

  private final String prefix;
  private final AmazonDynamoDBAsyncClient dynamo;
  private final int pageSize;
  private String startName;
  private String start;

  public TableNamePager(String prefix, AmazonDynamoDBAsyncClient dynamo, int pageSize) {
    this.prefix = prefix;
    this.dynamo = dynamo;
    this.start = prefix;
    this.pageSize = pageSize;
    this.startName = prefix;
  }

  @Override
  public void page(Queue<String> queue) {
    LOG.trace("Paging next batch of tables. Prefix: " + this.prefix + ", start: " + start);
    ListTablesResult tables = this.startName == null || this.startName.length() < 3 ?
                              dynamo.listTables(pageSize) : dynamo.listTables(startName, pageSize);
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
    startName = tables.getLastEvaluatedTableName();
    batchComplete();
  }

  private boolean noMoreTables(ListTablesResult tables) {
    return tables.getLastEvaluatedTableName() == null ||
           tables.getLastEvaluatedTableName().isEmpty();
  }
}
