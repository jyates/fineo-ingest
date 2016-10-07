package io.fineo.batch.processing.spark.options;

import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.AvroToDynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.schema.inject.CachingDynamoDBRepositoryProvider;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import io.fineo.lambda.handle.staged.FirehosePropertyBridge;
import io.fineo.lambda.kinesis.IKinesisProducer;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.Serializable;
import java.time.Instant;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME;
import static java.time.Instant.now;

/**
 * Bean class to handleEvent the actual options for the batch processing
 */
public class BatchOptions implements Serializable {

  public static final String VERSION = "v0";
  public static final String BATCH_ERRORS_OUTPUT_DIR_KEY = "fineo.batch.errors.dir";
  protected Properties props;

  public void setProps(Properties props) {
    this.props = props;
  }

  public IngestManifest getManifest() {
    props.list(System.out);
    return Guice.createInjector(
      new DefaultCredentialsModule(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new PropertiesModule(props),
      IngestManifestModule.create(props)).getInstance(IngestManifest.class);
  }

  public RecordToDynamoHandler getDynamoHandler() {
    return Guice.createInjector(newArrayList(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      new DynamoModule(),
      new AvroToDynamoModule(),
      new DynamoRegionConfigurator()
    )).getInstance(RecordToDynamoHandler.class);
  }

  public IFirehoseBatchWriter getFirehoseWriter() {
    return Guice.createInjector(newArrayList(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      new FirehoseFunctions(),
      // just load the archive functions - errors we just fail the job for right now
      new FirehoseModule().withArchive(),
      new FirehosePropertyBridge().withArchive()
    )).getInstance(
      Key.get(IFirehoseBatchWriter.class, Names.named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM)));
  }

  public RawJsonToRecordHandler getRawJsonToRecordHandler(IKinesisProducer queue) {
    return Guice.createInjector(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      // override the property - we don't actually care where it writes, it all goes to the queue
      InstanceToNamed.property(KINESIS_PARSED_RAW_OUT_STREAM_NAME, "raw-archive"),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new SchemaStoreModule(CachingDynamoDBRepositoryProvider.class),
      new SingleInstanceModule<>(queue, IKinesisProducer.class)
    ).getInstance(RawJsonToRecordHandler.class);
  }

  public String getErrorDirectory() {
    String path = props.getProperty(BATCH_ERRORS_OUTPUT_DIR_KEY);
    return new Path(new Path(path, VERSION), Long.toString(now().toEpochMilli())).toString();
  }
}
