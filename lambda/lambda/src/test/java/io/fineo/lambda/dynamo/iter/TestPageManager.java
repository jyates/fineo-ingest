package io.fineo.lambda.dynamo.iter;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TestPageManager {
  @Test(timeout = 1000)
  public void testNoItemsToFetch() throws Exception {
    PagingRunner<String> runner = new PagingRunner<String>() {
      boolean complete = false;

      @Override
      public boolean complete() {
        return complete;
      }

      @Override
      public void page(Pipe<String> queue) {
        this.complete = true;
        return;
      }
    };
    PagingIterator<String> iter = new PagingIterator<>(1, new PageManager<>(Lists.newArrayList
      (runner)));
    assertFalse(iter.hasNext());
  }
}