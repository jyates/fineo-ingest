package io.fineo.lambda.dynamo.iter;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertFalse;

public class TestPageManager {

  @Test(timeout = 1000)
  public void testNoItemsToFetch() throws Exception {
    PagingRunner<String> runner = new BasePager<String>() {

      @Override
      public void page(Queue<String> queue) {
        complete();
        return;
      }
    };
    PagingIterator<String> iter = new PagingIterator<>(1, new PageManager<>(Lists.newArrayList
      (runner)));
    assertFalse(iter.hasNext());
  }
}
