package io.fineo.etl.spark.read;

import io.fineo.etl.spark.SparkETL;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

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

  public DataFrame loadFrameForKnownSchema(String baseDir) throws IOException {
    return loadFrameForKnownSchemaByDate(new Date(0), new Date(Long.MAX_VALUE), baseDir);
  }

  public DataFrame loadFrameForKnownSchemaByDate(Date start, Date stop, String baseDir)
    throws IOException {
    return getUnknownSchemaDataByDateAndFormat(SparkETL.FORMAT, start, stop, baseDir);
  }

  public DataFrame loadFrameForUnknownSchema(String baseDir) throws IOException {
    return loadFrameForUnknownSchemaByDate(new Date(0), new Date(Long.MAX_VALUE), baseDir);
  }

  public DataFrame loadFrameForUnknownSchemaByDate(Date start, Date stop, String baseDir)
    throws IOException {
    return getUnknownSchemaDataByDateAndFormat(SparkETL.UNKNOWN_DATA_FORMAT, start, stop, baseDir);
  }

  private DataFrame getUnknownSchemaDataByDateAndFormat(String format, Date start, Date stop,
    String baseDir) throws IOException {
    Path base = new Path(baseDir);
    Path dir = new Path(base, format);
    SQLContext sql = new SQLContext(this.context);
    sql.setConf("spark.sql.parquet.mergeSchema", "true");
    List<String> days = Arrays.asList(fs.listStatus(dir, path -> {
      String date = path.getName();
      Date day = Date.valueOf(date);
      return day.after(start) && day.before(stop);
    })).stream().map(status -> status.getPath().toString()).collect(toList());
    return sql.read().format(format).load(days.toArray(new String[0]));
  }
}
