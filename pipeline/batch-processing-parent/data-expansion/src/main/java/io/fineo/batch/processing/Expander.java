package io.fineo.batch.processing;

import com.google.common.collect.AbstractIterator;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


class Expander implements Function2<Integer, Iterator<Row>, Iterator<Row>> {
  private final DataExpanderOptions opts;
  private final Broadcast<String[]> broadcastType;
  private final Broadcast<Tuple2<Object, Object>> bcMinMax;
  private final Broadcast<Integer> bcRowReuse;
  private Broadcast<long[]> broadcastTime;

  public Expander(DataExpanderOptions opts, Broadcast<String[]> broadcastType,
    Broadcast<Tuple2<Object, Object>> bcMinMax, Broadcast<long[]> broadcastTime,
    Broadcast<Integer> bcRowReuse) {
    this.opts = opts;
    this.broadcastType = broadcastType;
    this.bcMinMax = bcMinMax;
    this.broadcastTime = broadcastTime;
    this.bcRowReuse = bcRowReuse;
  }

  @Override
  public Iterator<Row> call(Integer chunk, Iterator<Row> iter) throws Exception {
    Instant start = Instant.ofEpochMilli(broadcastTime.getValue()[0]);
    Instant temp_end = Instant.ofEpochMilli(broadcastTime.getValue()[0] + broadcastTime
      .getValue()[3]);
    if (temp_end.isAfter(Instant.ofEpochMilli(broadcastTime.getValue()[1]))) {
      temp_end = Instant.ofEpochMilli(broadcastTime.getValue()[1]);
    }
    Instant end = temp_end;
    Duration step = Duration.ofMillis(broadcastTime.getValue()[2]);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(opts.timeformat);
    return new AbstractIterator<Row>() {
      private int reuseCount = 0;
      private Instant next;
      private Row original;

      @Override
      protected Row computeNext() {
        next = start.plus(step);
        if (next.isAfter(end) || !iter.hasNext()) {
          endOfData();
          return null;
        }
        // should we go onto the next row as the basis for the data?
        if (original == null || reuseCount++ == 0) {
          original = iter.next();
          if (reuseCount > bcRowReuse.getValue()) {
            reuseCount = 0;
          }
        }

        // set the row fields to match the current schema
        List<Object> fields = new ArrayList<>();
        StructType schema = original.schema();
        Map<String, Object> values =
          JavaConversions.asJavaMap(original.getValuesMap(JavaConversions.asScalaBuffer(Arrays
            .asList(schema.fieldNames()))));
        for (Map.Entry<String, Object> value : values.entrySet()) {
          Object val = value.getValue();
          if (value.getKey().equals(opts.timeField)) {
            // replace the timestamp field with the new timestamp
            val = formatter.format(next);
          } else if (value.getKey().equals(broadcastType.getValue()[0])) {
            // replace the value with one from a range of values
            val = rand();
          }
          fields.set((Integer) schema.getFieldIndex(value.getKey()).get(), val);
        }

        return RowFactory.create(fields.toArray());
      }

      private Object rand() {
        switch (broadcastType.getValue()[1]) {
          case "SHORT":
          case "INTEGER":
          case "INT":
            int min = (int) bcMinMax.getValue()._1();
            int max = (int) bcMinMax.getValue()._2();
            return ThreadLocalRandom.current().nextInt(min, max);
          case "DOUBLE":
          case "DECIMAL":
            double minD = (double) bcMinMax.getValue()._1();
            double maxD = (double) bcMinMax.getValue()._2();
            return ThreadLocalRandom.current().nextDouble(minD, maxD);
          case "LONG":
            long minL = (long) bcMinMax.getValue()._1();
            long maxL = (long) bcMinMax.getValue()._2();
            return ThreadLocalRandom.current().nextLong(minL, maxL);
          case "FLOAT":
            float minF = (float) bcMinMax.getValue()._1();
            float maxF = (float) bcMinMax.getValue()._2();
            return ThreadLocalRandom.current().nextFloat() * (maxF - minF) + minF;
          default:
            throw new UnsupportedOperationException("Does not support type: " + broadcastType
              .getValue()[1]);
        }
      }
    };
  }

  private Object rand() {
    switch (broadcastType.getValue()[1]) {
      case "SHORT":
      case "INTEGER":
      case "INT":
        int min = (int) bcMinMax.getValue()._1();
        int max = (int) bcMinMax.getValue()._2();
        return ThreadLocalRandom.current().nextInt(min, max);
      case "DOUBLE":
      case "DECIMAL":
        double minD = (double) bcMinMax.getValue()._1();
        double maxD = (double) bcMinMax.getValue()._2();
        return ThreadLocalRandom.current().nextDouble(minD, maxD);
      case "LONG":
        long minL = (long) bcMinMax.getValue()._1();
        long maxL = (long) bcMinMax.getValue()._2();
        return ThreadLocalRandom.current().nextLong(minL, maxL);
      case "FLOAT":
        float minF = (float) bcMinMax.getValue()._1();
        float maxF = (float) bcMinMax.getValue()._2();
        return ThreadLocalRandom.current().nextFloat() * (maxF - minF) + minF;
      default:
        throw new UnsupportedOperationException("Does not support type: " + broadcastType
          .getValue()[1]);
    }
  }
}
