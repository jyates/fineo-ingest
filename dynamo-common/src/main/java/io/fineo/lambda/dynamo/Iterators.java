package io.fineo.lambda.dynamo;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.function.Predicate;

public class Iterators {

  private Iterators() {
  }

  public static <T> Iterator<T> whereStop(Iterator<T> iter, Predicate<T> predicate) {
    return new AbstractIterator<T>() {

      @Override
      protected T computeNext() {
        if (!iter.hasNext()) {
          endOfData();
          return null;
        }

        T next = iter.next();
        if (next == null || !predicate.test(next)) {
          endOfData();
        }
        return next;
      }
    };
  }
}
