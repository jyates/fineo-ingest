package io.fineo.lambda.dynamo;


import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.time.temporal.Temporal;

/**
 * A range of time between two fixed points
 */
public class Range<TIME extends Temporal> {

  private final Pair<TIME, TIME> pair;

  public Range(TIME start, TIME end) {
    this.pair = new ImmutablePair<>(start, end);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Range))
      return false;

    Range<Instant> range = (Range<Instant>) o;

    return pair.equals(range.pair);

  }

  @Override
  public int hashCode() {
    return pair.hashCode();
  }

  public TIME getStart() {
    return pair.getKey();
  }

  public TIME getEnd() {
    return pair.getValue();
  }

  public static Range<Instant> of(long startEpochMillis, long endEpochMillis) {
    return new Range<>(Instant.EPOCH.plusMillis(startEpochMillis),
      Instant.EPOCH.plusMillis(endEpochMillis));
  }
}
