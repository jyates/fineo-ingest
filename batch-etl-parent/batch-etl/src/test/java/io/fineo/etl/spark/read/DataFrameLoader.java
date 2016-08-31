package io.fineo.etl.spark.read;

import io.fineo.etl.spark.SparkETL;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.fineo.schema.avro.AvroSchemaEncoder.*;
import static java.util.stream.Collectors.toList;

/**
 * Helper class to load data frames for spark
 */
public class DataFrameLoader {

  private final JavaSparkContext context;
  private final FileSystem fs;

  /**
   * @param context
   * @throws IOException if we cannot create a filesystem from the underlying hadooop configuration
   */
  public DataFrameLoader(JavaSparkContext context) throws IOException {
    this.context = context;
    this.fs = FileSystem.get(context.hadoopConfiguration());
  }

  public List<Pair<PartitionKey, DataFrame>> loadFrameForKnownSchema(String baseDir)
    throws IOException {
    return loadFrameForKnownSchemaByDate(new Date(0), new Date(Long.MAX_VALUE), baseDir);
  }

  public List<Pair<PartitionKey, DataFrame>> loadFrameForKnownSchemaByDate(Date start, Date stop,
    String baseDir)
    throws IOException {
    return getUnknownSchemaDataByDateAndFormat(SparkETL.FORMAT, start, stop, baseDir);
  }

  public List<Pair<PartitionKey, DataFrame>> loadFrameForUnknownSchema(String baseDir)
    throws IOException {
    return loadFrameForUnknownSchemaByDate(new Date(0), new Date(Long.MAX_VALUE), baseDir);
  }

  public List<Pair<PartitionKey, DataFrame>> loadFrameForUnknownSchemaByDate(Date start, Date stop,
    String baseDir)
    throws IOException {
    return getUnknownSchemaDataByDateAndFormat(SparkETL.UNKNOWN_DATA_FORMAT, start, stop, baseDir);
  }

  private List<Pair<PartitionKey, DataFrame>> getUnknownSchemaDataByDateAndFormat(String format,
    Date
      start, Date stop,
    String baseDir) throws IOException {
    Map<PartitionKey, DataFrame> map = new HashMap<>();
    Path base = new Path(baseDir);
    Path formatDir = new Path(base, format);
    SQLContext sql = new SQLContext(this.context);
    // walk the first three levels of the heirarchy to get the org, metric and date
    forChildren(formatDir, org -> {
      PartitionKey key = new PartitionKey();
      key.setOrg(org.getPath().getName());
      forChildren(org.getPath(), metric -> {
        PartitionKey metricKey = new PartitionKey(key);
        metricKey.setMetricId(metric.getPath().getName());
        forChildren(metric.getPath(), dateStatus -> {
          Date date = Date.valueOf(dateStatus.getPath().getName());

          if (date.after(start) && date.before(stop)) {
            DataFrame frame = map.get(metricKey);
            DataFrameReader reader = sql.read().format(format);
            // enable schema merging to read from parquet files. Necessary since the schema we pick
            // without merging seems to be entirely based on the semantic ordering of the files
            if(format.equals(SparkETL.FORMAT)){
              reader.option("mergeSchema", "true");
            }
            DataFrame load = reader.load(dateStatus.getPath().toString());
            if (frame != null) {
              load = frame.unionAll(load);
            }
            map.put(metricKey, load);
          }
        });
      });
    });

    return map.entrySet().stream().map(entry -> new ImmutablePair<>(entry.getKey(), entry
      .getValue().sort(new Column(TIMESTAMP_KEY)))).collect(Collectors.toList());
  }

  private void forChildren(Path path, Consumer<FileStatus> consumer) {
    FileStatus[] files;
    try {
      files = fs.listStatus(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (FileStatus child : files) {
      consumer.accept(child);
    }
  }
}
