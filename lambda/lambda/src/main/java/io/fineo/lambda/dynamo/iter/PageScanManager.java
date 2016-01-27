package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.fineo.lambda.dynamo.ResultOrException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

/**
 *
 */
public class PageScanManager implements BiConsumer<Queue<ResultOrException<Map<String,
  AttributeValue>>>, PagingIterator<ResultOrException<Map<String,
  AttributeValue>>>> {

  private List<PagingScanRunner> runners;

  public PageScanManager(List<PagingScanRunner> runners) {
    this.runners = runners;
  }

  @Override
  public void accept(Queue<ResultOrException<Map<String, AttributeValue>>> results,
    PagingIterator<ResultOrException<Map<String, AttributeValue>>>
      iterator) {
    // get the next runner
    PagingScanRunner runner = null;
    while (runners.size() > 0 && runner == null) {

      // get the first runner with more data to read
      PagingScanRunner next = runners.get(0);
      // the scanner has no more data, remove the runner
      if (next.complete()) {
        runners.remove(0);
        continue;
      }
      runner = next;
    }

    // no more runners left, so no more results to page
    if (runner == null) {
      iterator.done();
      return;
    }

    // run the runner
    PagingScanRunner ran = runner;
    ran.page(new Pipe<ResultOrException<Map<String, AttributeValue>>>() {
      @Override
      public void add(ResultOrException<Map<String, AttributeValue>> e) {
        results.add(e);
        iterator.completedBatch();
      }

      @Override
      public void addAll(
        Collection<ResultOrException<Map<String, AttributeValue>>> resultOrExceptions) {
        results.addAll(resultOrExceptions);
        iterator.completedBatch();
      }
    });
  }
}