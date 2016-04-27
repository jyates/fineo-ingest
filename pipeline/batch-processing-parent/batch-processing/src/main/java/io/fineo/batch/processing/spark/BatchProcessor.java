package io.fineo.batch.processing.spark;

import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.spark.convert.RecordConverter;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.batch.processing.spark.write.DynamoWriter;
import io.fineo.batch.processing.spark.write.StagedFirehoseWriter;
import io.fineo.etl.spark.fs.RddLoader;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Run the stream processing pipeline in 'batch mode' as a spark job.
 */
public class BatchProcessor {

  private static final Log LOG = LogFactory.getLog(BatchProcessor.class);
  private final BatchOptions opts;

  public BatchProcessor(BatchOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context)
    throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    Multimap<String, String> sources = loadManifest();
    if (sources.size() == 0) {
      LOG.warn("No input sources found!");
      return;
    }
    runJob(context, sources.values());
    clearManifest(sources);
  }

  private void clearManifest(Multimap<String, String> sources) {
    IngestManifest manifest = opts.getManifest();
    for (Map.Entry<String, Collection<String>> files : sources.asMap().entrySet()) {
      manifest.remove(files.getKey(), files.getValue());
    }
    manifest.flush();
  }

  private Multimap<String, String> loadManifest() {
    IngestManifest manifest = opts.getManifest();
    return manifest.files();
  }

  private void runJob(JavaSparkContext context, Collection<String> sources)
    throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    RddLoader loader = new RddLoader(context, sources);
    loader.load();
    JavaPairRDD<String, PortableDataStream>[] stringRdds = loader.getRdds();
    // convert the records
    JavaRDD<GenericRecord>[] records = parse(stringRdds);
    // write the records
    List<JavaFutureAction> actions = new ArrayList<>(records.length * 2);
    for (JavaRDD<GenericRecord> rdd : records) {
      actions.add(rdd.foreachPartitionAsync(new DynamoWriter(opts.props())));
      // we write them separately to firehose because we don't have a way of just saving files
      // directly to S3 without them being sequence files
      actions.add(rdd.foreachPartitionAsync(new StagedFirehoseWriter(opts.props())));
    }

    // wait for all the actions to complete
    for (JavaFutureAction action : actions) {
      action.get();
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

  public static void main(String[] args) throws Exception {
    // parse arguments and load options
    BatchOptions opts = new BatchOptions();
    opts.setProps(PropertiesLoaderUtil.load());

    // setup spark
    SparkConf conf = new SparkConf().setAppName(BatchProcessor.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);

    // run the job
    BatchProcessor processor = new BatchProcessor(opts);
    processor.run(context);
  }
}