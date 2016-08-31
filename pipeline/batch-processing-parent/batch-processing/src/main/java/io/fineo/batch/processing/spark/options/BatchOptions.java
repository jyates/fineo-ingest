package io.fineo.batch.processing.spark.options;

import com.google.inject.Guice;
import com.google.inject.Injector;
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
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.staged.FirehosePropertyBridge;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;

import java.io.Serializable;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Bean class to handleEvent the actual options for the batch processing
 */
public class BatchOptions implements Serializable {

  private Properties props;
  private Injector injector;

  public Properties props() {
    return props;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public IngestManifest getManifest() {
    if (this.injector == null) {
      this.injector = Guice.createInjector(
        new DefaultCredentialsModule(),
        new DynamoModule(),
        new DynamoRegionConfigurator(),
        new PropertiesModule(props),
        new DynamoProvisionedThroughputModule(),
        IngestManifestModule.create(props));
    }
    return injector.getInstance(IngestManifest.class);
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

  public FirehoseBatchWriter getFirehoseWriter() {
    return Guice.createInjector(newArrayList(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      new FirehoseModule(), new FirehoseFunctions(), new FirehosePropertyBridge()
    )).getInstance(
      Key.get(FirehoseBatchWriter.class, Names.named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM)));
  }
}
