package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Page through table names matching the given prefix
 */
public class TableNamePager extends BasePager<String> {

  private static final Log LOG = LogFactory.getLog(TableNamePager.class);

  private final String prefix;
  private final AmazonDynamoDBAsyncClient dynamo;
  private final int pageSize;
  private String start;

  public TableNamePager(String prefix, AmazonDynamoDBAsyncClient dynamo, int pageSize) {
    this.prefix = prefix;
    this.dynamo = dynamo;
    this.start = prefix;
    this.pageSize = pageSize;
  }

  @Override
  public void page(Pipe<String> queue) {
    LOG.trace("Paging next batch of tables. Prefix: " + this.prefix + ", start: " + start);
    ListTablesResult tables = dynamo.listTables(start, pageSize);
    LOG.trace("Got next page: " + tables);
    int[] counter = new int[1];
    tables.getTableNames().stream().filter(name -> name.startsWith(prefix)).forEach(name -> {
      counter[0]++;
      queue.add(name);
    });
    // if we went off the end of the prefix, then we are done
    if ((counter[0] != tables.getTableNames().size()) ||
        tables.getLastEvaluatedTableName() == null ||
        tables.getLastEvaluatedTableName().isEmpty()) {
      complete();
      return;
    } else {
      this.start = tables.getLastEvaluatedTableName();
    }
  }
}
