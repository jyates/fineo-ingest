package io.fineo.batch.processing;

import com.beust.jcommander.JCommander;
import io.fineo.spark.avro.AvroSparkUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Expand data for a specific field for a given time range
 */
public class DataExpander {

  public static final String SPARK_CSV_FORMAT = "com.databricks.spark.csv";

  void run(JavaSparkContext context, DataExpanderOptions opts) throws URISyntaxException,
    IOException {
    SQLContext sqlContext = new SQLContext(context);

    FileSystem fs = null;
    List<DataFrame> frames = new ArrayList<>(opts.paths.size());
    // all the files assumed to be on the same fs
    for (String path : opts.paths) {
      URI root = new URI(path);
      if (fs == null) {
        fs = FileSystem.get(root, context.hadoopConfiguration());
      }
      Path p = fs.resolvePath(new Path(root.getPath()));
      frames.add(sqlContext.read()
                           .format(SPARK_CSV_FORMAT)
                           .option("inferSchema", "true")
                           .option("header", "true")
                           .load(p.toString()));
    }

    DataFrame frame = frames.remove(0);
    for (DataFrame union : frames) {
      frame = frame.unionAll(union);
    }

    // find the min/max for each row
    Broadcast<String[]> broadcastType =
      context.broadcast(new String[]{opts.intervalField, opts.fieldType});

    JavaRDD<Row> rows = frame.toJavaRDD();
    Comparator<Row> minCompare = new MinimumValueForRowComparator(broadcastType);
    Comparator<Row> maxCompare = minCompare.reversed();
    Row min = rows.min(minCompare);
    Row max = rows.max(maxCompare);
    Broadcast<Tuple2<Object, Object>> bcMinMax = context.broadcast(new Tuple2<>(min.getAs
      (opts.intervalField), max.getAs(opts.intervalField)));

    // how big is each chunk we are processing?
    long chunkWidth = (opts.end - opts.start) / opts.chunks;
    Broadcast<long[]> broadcastTime =
      context.broadcast(new long[]{opts.start, opts.end, opts.interval, chunkWidth});

    // what happens when we parallelize?
    long rowsPerChunk = rows.count() / opts.chunks;
    if (rowsPerChunk == 0) {
      rowsPerChunk = 1;
    }
    // number of rows that we are generating in each chunk
    long genRowsPerChunk = chunkWidth / opts.interval;
    if (genRowsPerChunk == 0) {
      genRowsPerChunk = 1;
    }

    // how many times do we need to reuse a row?
    double genPerRowInChunk = ((double) genRowsPerChunk) / rowsPerChunk;
    int rowsToGeneratePerIncomingRowInChunk = (int) Math.ceil(genPerRowInChunk);
    Broadcast<Integer> bcRowReuse = context.broadcast(rowsToGeneratePerIncomingRowInChunk);

    // process the chunks
    rows.repartition(opts.chunks); // ensure we partition into the number of chunks
    JavaRDD<Row> expanded = rows.mapPartitionsWithIndex(
      new Expander(opts, broadcastType, bcMinMax, broadcastTime, bcRowReuse), true);

    sqlContext.createDataFrame(expanded, min.schema()).write()
              .format(SPARK_CSV_FORMAT)
              .option("header", "true")
              .save(opts.output);
  }

  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName(DataExpander.class.getName());
    AvroSparkUtils.setKyroAvroSerialization(conf);

    final JavaSparkContext context = new JavaSparkContext(conf);
    context.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2");

    // run the job
    DataExpander processor = new DataExpander();
    DataExpanderOptions opts = new DataExpanderOptions();
    JCommander commander = new JCommander(opts);
    commander.parse(args);

    try {
      processor.run(context, opts);
    } catch (Error e) {
      System.out.println("Got error!");
      System.out.println(Arrays.toString(e.getStackTrace()));
      // need to print the trace since Spark is not handling it correctly in 1.6.2
      e.printStackTrace();
      System.exit(1000);
      throw e;
    }
  }

}
