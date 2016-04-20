package io.fineo.batch.processing.spark;

import io.fineo.batch.processing.spark.convert.RecordConverter;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.batch.processing.spark.write.DynamoWriter;
import io.fineo.batch.processing.spark.write.StagedFirehoseWriter;
import io.fineo.etl.spark.fs.RddLoader;
import io.fineo.lambda.JsonParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Run the stream processing pipeline in 'batch mode' as a spark job.
 */
public class BatchProcessor {

  private final BatchOptions opts;

  public BatchProcessor(BatchOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context) throws IOException, URISyntaxException {
    RddLoader loader = new RddLoader(context, opts.ingest());
    loader.load();
    JavaPairRDD<String, PortableDataStream>[] stringRdds = loader.getRdds();
    // convert the records
    JavaRDD<GenericRecord>[] records = parse(stringRdds);
    // write the records
    List<JavaFutureAction> actions = new ArrayList<>(records.length *2);
    for(JavaRDD<GenericRecord> rdd: records){
      actions.add(rdd.foreachPartitionAsync(new DynamoWriter(opts.props())));
      // we write them separately to firehose because we don't have a way of just saving files
      // directly to S3 without them being sequence files
      actions.add(rdd.foreachPartitionAsync(new StagedFirehoseWriter(opts.props())));
    }
  }

  private JavaRDD<GenericRecord>[] parse(JavaPairRDD<String, PortableDataStream>[] stringRdds) {
    JavaRDD<GenericRecord>[] records = new JavaRDD[stringRdds.length];
    for (int i = 0; i < stringRdds.length; i++) {
      JavaPairRDD<String, PortableDataStream> strings = stringRdds[i];
      JsonParser parser = new JsonParser();
      JavaRDD<Map<String, Object>> json = strings.flatMap(tuple -> parser.parse(tuple._2().open()));
      records[i] = json.map(new RecordConverter(opts.props()));
      records[i].persist(StorageLevel.MEMORY_AND_DISK());
    }
    return records;
  }
}
