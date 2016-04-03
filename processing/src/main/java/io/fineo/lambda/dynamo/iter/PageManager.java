package io.fineo.lambda.dynamo.iter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.function.BiConsumer;

public class PageManager<T> implements BiConsumer<Queue<T>, PagingIterator<T>> {
  private static final Log LOG = LogFactory.getLog(PageManager.class);
  private List<PagingRunner<T>> runners;

  public PageManager(List<PagingRunner<T>> runners) {
    this.runners = runners;
  }

  @Override
  public void accept(Queue<T> results, PagingIterator<T> iterator) {
    // run the runner
    PagingRunner ran = getNextRunner();
    try {
      ran.page(new Pipe<T>() {
        @Override
        public void add(T e) {
          results.add(e);
          iterator.completedBatch();
        }

        @Override
        public void addAll(
          Collection<T> resultOrExceptions) {
          results.addAll(resultOrExceptions);
          iterator.completedBatch();
        }
      });
    } catch (Exception e) {
      LOG.error("Failed to load next page: ", e);
      throw e;
    } finally {
      // if this is the last runner, check to see if its done, in which case
      if (getNextRunner() == null) {
        iterator.done();
      }
    }
  }

  private PagingRunner getNextRunner() {
    PagingRunner runner = null;
    while (runners.size() > 0 && runner == null) {
      // get the first runner with more data to read
      PagingRunner next = runners.get(0);
      // the scanner has no more data, remove the runner
      if (next.complete()) {
        runners.remove(0);
        continue;
      }
      runner = next;
    }
    return runner;
  }
}