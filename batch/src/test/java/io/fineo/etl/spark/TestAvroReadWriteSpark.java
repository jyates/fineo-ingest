package io.fineo.etl.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import io.fineo.aws.AwsDependentTests;
import io.fineo.etl.options.ETLOptions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.collection.JavaConversions;
import scala.collection.convert.Wrappers$;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.spark.SparkETL.DATE_KEY;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark extends SharedJavaSparkContext {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);

  @Rule
  public TestOutput folder = new TestOutput(false);

  @Before
  public void runBefore() {
    conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "io.fineo.etl.AvroKyroRegistrator");
    super.runBefore();
  }

  @After
  public void cleanupMetastore() throws IOException {
    File file = new File("metastore_db");
    FileUtils.deleteDirectory(file);
  }

  @Test
  public void testSingleRecord() throws Exception {
    TestEndToEndLambdaLocal.TestState state = TestEndToEndLambdaLocal.runTest();
    ResourceManager resources = state.getResources();

    // save the record(s) to a file
    File ingest = folder.newFolder("ingest");
    File file = new File(ingest, "output");
    File file2 = new File(ingest, "output2");

    LambdaClientProperties props = state.getRunner().getProps();
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    writeStreamToFile(resources, stream, file);
    writeStreamToFile(resources, stream, file2);

    // open the file in spark and get the instances
    File archive = folder.newFolder("archive");
    File archiveOut = new File(archive, "output");
    ETLOptions opts = new ETLOptions();
    String base = "file://";
    opts.archive(base + archiveOut.getAbsolutePath());
    opts.root(base + ingest.getAbsolutePath());

    // reuse the same store from the test runner
    props.setStoreProvider(() -> state.getResources().getStore());
    opts.setProps(props);

    // actually run the job
    SparkETL etl = new SparkETL(opts);
    etl.run(jsc());

    // remove any previous history, so we don't try and read the old metastore
    cleanupMetastore();

    // read all the rows that we stored
    HiveContext sql = new HiveContext(jsc());
    DataFrame records = sql.read().format("orc").load(opts.archive());
    records.registerTempTable("table");
    Row[] rows = sql.sql("SELECT * FROM table").collect();
    assertEquals("Wrong number of rows! Got: " + Arrays.toString(rows), 1, rows.length);

    // check the content against the object
    Row row = rows[0];
    Map<String, Object> msg = state.getRunner().getProgress().getJson();
    int i = 0;
    String orgId = (String) msg.get(ORG_ID_KEY);

    Map<String, Object> fields = new HashMap<>();
    fields.put(ORG_ID_KEY, orgId);
    SchemaStore store = props.createSchemaStore();
    Metadata metadata = store.getSchemaTypes(orgId);
    String metricName = metadata.getCanonicalNamesToAliases().keySet().iterator().next();
    Metric metric = store.getMetricMetadata(orgId, metricName);
    Map<String, String> aliasToCName = AvroSchemaManager.getAliasRemap(metric);
    fields.put(ORG_METRIC_TYPE_KEY, metricName);
    long ts = (long) msg.get(TIMESTAMP_KEY);
    fields.put(TIMESTAMP_KEY, ts);
    fields.put(DATE_KEY, new Date(ts).toString());
    // add non-base fields
    for (String field : msg.keySet()) {
      if (AvroSchemaEncoder.IS_BASE_FIELD.negate().test(field)) {
        String cname = aliasToCName.get(field);
        fields.put(cname, msg.get(field));
      }
    }

    scala.collection.Map<String, Object> rowFields =
      row.getValuesMap(JavaConversions.asScalaBuffer(newArrayList(fields.keySet())));
    assertEquals(fields.size(), rowFields.size());
    for (Map.Entry<String, Object> field : fields.entrySet()) {
      assertEquals("Mismatch for " + field.getKey(), field.getValue(),
        rowFields.get(field.getKey()).get());
    }
  }


  @Test
  @Ignore("Not yet implemented")
  public void testNonStringTypesInUnknownFields() throws Exception {
  }

  private void ensureOnlyOneRecordWithData(File archive, String... paths)
    throws IOException {
    Configuration conf = new Configuration();
    GzipCodec codec = new GzipCodec();
    codec.setConf(conf);

    List<Pair<File, Integer>> files = new ArrayList<>(paths.length);
    for (String path : paths) {
      File file = new File(archive, path);
      FileInputStream stream = new FileInputStream(file);
      files.add(new ImmutablePair<>(file, codec.createInputStream(stream).read()));
    }
    assertEquals("Expected only one output file with data, but got " + files, 1,
      files.stream().map(pair -> pair.getRight()).filter(bytes -> bytes > 0).count());
  }

  private void writeStreamToFile(ResourceManager resources, String stream, File file)
    throws IOException {
    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    resources.getFirehoseWrites(stream).stream().forEach(buff -> {
      buff.flip();
      try {
        channel.write(buff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    channel.close();

  }
}
