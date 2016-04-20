package io.fineo.batch.processing.spark;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.dynamo.MockAvroToDynamo;
import io.fineo.lambda.e2e.resources.firehose.LocalFirehoseStreams;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import io.fineo.test.rule.TestOutput;
import org.apache.spark.api.java.JavaSparkContext;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

public class SparkResourceManager implements ResourceManager {

  private final SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  private final JavaSparkContext context;
  private final TestOutput output;
  private Properties props;
  private BatchOptions opts;
  private BatchProcessor processor;
  private ModuleLoader modules;
  private LocalFirehoseStreams firehoses;
  private MockKinesisStreams kinesis;
  private MockAvroToDynamo dynamo;

  public SparkResourceManager(JavaSparkContext jsc, TestOutput output) {
    this.context = jsc;
    this.output = output;
  }

  @Override
  public void setup(LambdaClientProperties properties) throws Exception {
    this.props = properties.getRawPropertiesForTesting();
    this.opts = new BatchOptions();
    opts.setProps(this.props);
    this.processor = new BatchProcessor(opts);

    this.kinesis = new MockKinesisStreams();
    this.firehoses = new LocalFirehoseStreams();
    firehoses.setup(properties);

    this.dynamo = new MockAvroToDynamo(store);
    this.modules =
      prepareModules(store, kinesis.getProducer(), firehoses, opts.props(), dynamo.getWriter());
  }

  private ModuleLoader prepareModules(SchemaStore schemaStore, IKinesisProducer kp,
    LocalFirehoseStreams firehoses,
    Properties properties, AvroToDynamoWriter writer) {
    ModuleLoader loader = new ModuleLoader();

    PropertiesModule props = new PropertiesModule(properties);
    SingleInstanceModule<AvroToDynamoWriter> dynamo =
      new SingleInstanceModule<>(writer, AvroToDynamoWriter.class);
    SingleInstanceModule<SchemaStore> store = SingleInstanceModule.instanceModule(schemaStore);
    SingleInstanceModule<IKinesisProducer> kinesis =
      new SingleInstanceModule<>(kp, IKinesisProducer.class);

    loader.set(RawJsonToRecordHandler.class, newArrayList(props, kinesis, store));
    loader.set(RecordToDynamoHandler.class, newArrayList(dynamo));
    loader.set(FirehoseBatchWriter.class, newArrayList(new LocalFirehoseStreamsModule(firehoses)));

    return loader;
  }

  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    byte[] start = LambdaTestUtils.asBytes(json);
    // write the bytes to a file
    File file = output.newFile();
    try (FileOutputStream out = new FileOutputStream(file)) {
      out.write(start);
    }

    // set the file as the input to the job
    opts.setInput(file.getAbsolutePath());

    // run the job
    processor.run(context, modules);

    return start;
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws Exception {
    this.firehoses.cleanup();
    this.dynamo.cleanup();
    this.kinesis.reset();
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    return this.firehoses.getFirehoseWrites(streamName);
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String streamName) {
    List<ByteBuffer> data = new ArrayList<>();
    for (List<ByteBuffer> buffs : this.kinesis.getEventQueue(streamName)) {
      data.addAll(buffs);
    }
    return data;
  }

  @Override
  public SchemaStore getStore() {
    return this.store;
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {

  }
}
