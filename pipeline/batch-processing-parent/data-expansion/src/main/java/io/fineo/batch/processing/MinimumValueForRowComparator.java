package io.fineo.batch.processing;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.BiFunction;
import java.util.function.Function;

class MinimumValueForRowComparator implements Serializable, Comparator<Row> {
  private Broadcast<String[]> broadcastType;

  public MinimumValueForRowComparator(Broadcast<String[]> broadcastType) {
    this.broadcastType = broadcastType;
  }

  @Override
  public int compare(Row row1, Row row2) {
    int index1 = row1.fieldIndex(broadcastType.getValue()[0]);
    int index2 = row2.fieldIndex(broadcastType.getValue()[0]);
    switch (broadcastType.getValue()[1].toUpperCase()) {
      case "SHORT":
      case "INTEGER":
      case "INT":
        return Integer.compare(row1.getAs(index1), row2.getAs(index2));
      case "DOUBLE":
      case "DECIMAL":
        return Double.compare(row1.getAs(index1), row2.getAs(index2));
      case "LONG":
        return Long.compare(row1.getAs(index1), row2.getAs(index2));
      case "FLOAT":
        return Float.compare(row1.getFloat(index1), row2.getFloat(index2));
      default:
        throw new UnsupportedOperationException("Do not support expanding on type: "
                                                + "" + broadcastType.getValue()[1]);
    }
  }
}
