package io.fineo.lambda.dynamo.iter;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

@Category(AwsDependentTests.class)
public class TestScanPager {
  private static final Log LOG = LogFactory.getLog(TestScanPager.class);
  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tableResource = new AwsDynamoTablesResource(dynamoResource);

  private final String primaryKey = "pk";

  @Test(expected = ResourceNotFoundException.class)
  public void testNoTablePresent() throws Exception {
    read(new ScanRequest(tableResource.getTestTableName()));
  }

  @Test
  public void testReadOneRow() throws Exception {
    String name = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    write(createStringKeyTable(), item);
    assertResultsEqualsItems(item, read(new ScanRequest(name)));
  }

  @Test
  public void testStopRow() throws Exception {
    String name = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    Item item2 = new Item().withString(primaryKey, "vey");
    write(createStringKeyTable(), item, item2);
    assertResultsEqualsItems(item, read(new ScanRequest(name), "s"));
  }

  /**
   * Regression test. If we don't order the batch complete and pager complete indications correctly,
   * e.g. batch before pager, then we can kick off a new batch when the pager is complete and, in
   * some very bad cases, get another result from the new batch before we can indicate that that
   * pager is complete since the scan pager runs asynchronously. This was a concurrency bug, so
   * its hard to force without lots of injections in the {@link PagingIterator}, so we simulate
   * by running it a lot of times.
   *
   * @throws Exception
   */
  @Test
  public void testScanBatchPageCompleteOrdering() throws Exception {
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    AmazonDynamoDBAsyncClient spy = Mockito.spy(client);
    AtomicInteger counter = new AtomicInteger(0);
    Mockito.doAnswer(invocationOnMock -> {
      counter.incrementAndGet();
      return invocationOnMock.callRealMethod();
    }).when(spy).scanAsync(any(ScanRequest.class), any(AsyncHandler.class));
    String table = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    Item item2 = new Item().withString(primaryKey, "key2");
    Item item3 = new Item().withString(primaryKey, "key3");
    write(createStringKeyTable(), item, item2, item3);
    for (int i = 0; i < 3000; i++) {
      if (i % 250 == 0) {
        LOG.info("Batch " + i + " successful");
      }
      read(spy, new ScanRequest(table), null);
      assertEquals(i + ") Wrong number of get requests made!", 1, counter.getAndSet(0));
    }
  }

  /**
   * The local complement to {@link #testScanBatchPageCompleteOrdering}
   *
   * @throws Exception on failure
   */
  @Test
  public void testScanPagerVsBatchComplete() throws Exception {
    String table = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    write(createStringKeyTable(), item);
    ScanRequest request = new ScanRequest(table);
    ScanPager pager = new ScanPager(tableResource.getAsyncClient(), request, primaryKey, null);
    Queue results = new LinkedTransferQueue<>();
    CountDownLatch done = new CountDownLatch(2);
    String paged = "page done", batched = "batch complete";
    List<String> steps = Collections.synchronizedList(newArrayList());
    VoidCallWithArg<PagingRunner<ResultOrException<Map<String, AttributeValue>>>> doneNotifier =
      Mockito.mock(VoidCallWithArg.class);
    Mockito.doAnswer(call -> {
      steps.add(paged);
      return null;
    }).when(doneNotifier).call(any());
    Runnable batchComplete = Mockito.mock(Runnable.class);
    Mockito.doAnswer(call -> {
      steps.add(batched);
      done.countDown();
      return null;
    }).when(batchComplete).run();

    pager.page(results, doneNotifier, batchComplete);
    done.await();
    assertEquals("Wrong page/batch complete ordering!", newArrayList(paged, batched, batched),
      steps);
  }


  private void assertResultsEqualsItems(Item item, List<Map<String, AttributeValue>> values) {
    assertResultsEqualsItems(newArrayList(item), values);
  }

  private void assertResultsEqualsItems(List<Item> items,
    List<Map<String, AttributeValue>> values) {
    Map<String, AttributeValue> map = new HashMap<>();
    for (Item item : items) {
      for (Map.Entry<String, Object> val : item.asMap().entrySet()) {
        map.put(val.getKey(), new AttributeValue().withS((String) val.getValue()));
      }
    }
    assertEquals(newArrayList(map), values);
  }

  private Table createStringKeyTable() {
    return createStringKeyTable(tableResource.getTestTableName());
  }

  private Table createStringKeyTable(String name) {
    CreateTableRequest create =
      new CreateTableRequest().withTableName(name)
                              .withKeySchema(new KeySchemaElement(primaryKey, KeyType.HASH))
                              .withAttributeDefinitions(
                                new AttributeDefinition(primaryKey, ScalarAttributeType.S))
                              .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    DynamoDB dynamo = new DynamoDB(client);
    return dynamo.createTable(create);
  }

  private void write(Table table, Item... items) {
    for (Item item : items) {
      table.putItem(item);
    }
  }

  public List<Map<String, AttributeValue>> read(ScanRequest request) {
    return read(request, null);
  }

  public List<Map<String, AttributeValue>> read(ScanRequest request, String stopKey) {
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    return read(client, request, stopKey);
  }

  public List<Map<String, AttributeValue>> read(AmazonDynamoDBAsyncClient client,
    ScanRequest request, String stopKey) {
    ScanPager pager = new ScanPager(client, request, primaryKey, stopKey);
    Iterable<ResultOrException<Map<String, AttributeValue>>> iter =
      () -> new PagingIterator<>(1, new PageManager(pager));
    return
      StreamSupport.stream(iter.spliterator(), false).map(re -> {
        re.doThrow();
        return re.getResult();
      }).collect(toList());
  }
}
