package io.fineo.lambda.dynamo.iter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;

public class PageManager<T> implements VoidCallWithArg<PagingRunner<T>> {
  private static final Log LOG = LogFactory.getLog(PageManager.class);
  private List<PageTracker> runners;
  private PagingIterator<T> parent;

  public PageManager(PagingRunner<T> runner) {
    this(newArrayList(runner));
  }

  public PageManager(List<PagingRunner<T>> runners) {
    this.runners = runners.stream().map(runner -> new PageTracker(runner)).collect(toList());
  }

  public void prepare(PagingIterator<T> parent) {
    this.parent = parent;
  }

  public void update(Queue<T> results) {
    // run the runner
    PagingRunner pager = getNextPager();
    if (pager == null) {
      parent.done();
      return;
    }

    try {
      pager.page(new Pipe<T>() {
        @Override
        public void add(T e) {
          results.add(e);
          parent.completedBatch();
        }

        @Override
        public void addAll(Collection<T> resultOrExceptions) {
          results.addAll(resultOrExceptions);
          parent.completedBatch();
        }
      }, this);
    } catch (Exception e) {
      LOG.error("Failed to load next page: ", e);
      throw e;
    }
  }

  public synchronized void call(PagingRunner runner) {
    // mark the runner as complete
    for (PageTracker tracker : runners) {
      if (tracker.runner == runner) {
        tracker.done();
      }
    }
    if (runners.stream().filter(t -> !t.complete).count() == 0) {
      this.parent.done();
    }
  }

  private synchronized PagingRunner getNextPager() {
    List<PageTracker> completed = new ArrayList<>(1);
    try {
      for (PageTracker tracker : runners) {
        if (!tracker.complete) {
          return tracker.runner;
        }
        completed.add(tracker);
      }

      return null;
    } finally {
      runners.remove(completed);
    }
  }

  private class PageTracker {

    private final PagingRunner runner;
    private boolean complete = false;

    public PageTracker(PagingRunner runner) {
      this.runner = runner;
    }

    public void done() {
      this.complete = true;
    }
  }
}
