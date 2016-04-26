package io.fineo.batch.processing.spark.options;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.NullableNamedInstanceModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;

import java.util.Properties;

/**
 * Bean class to handle the actual options for the batch processing
 */
public class BatchOptions {

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
        new IngestManifestModule(),
        // no table override
        new NullableNamedInstanceModule<>(IngestManifestModule.INGEST_MANIFEST_OVERRIDE, null,
          DynamoDBMapperConfig.TableNameOverride.class));
    }
    return injector.getInstance(IngestManifest.class);
  }
}
