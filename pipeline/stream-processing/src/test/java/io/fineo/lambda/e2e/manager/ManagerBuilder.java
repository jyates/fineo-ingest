package io.fineo.lambda.e2e.manager;

import com.google.inject.Module;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.e2e.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.manager.collector.LoggingCollector;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.lambda.e2e.util.TestProperties;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.store.SchemaStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.util.Arrays.asList;

/**
 * Builder to create a custom resource manager
 */
public class ManagerBuilder {

  private Module awsCredentials;
  private LambdaKinesisConnector connector;
  private IDynamoResource dynamo;
  private Optional<SchemaStore> store;
  private IFirehoseResource firehose;
  private IKinesisStreams streams;
  private List<Module> additionalModules = new ArrayList<>();
  private String region;
  private LambdaClientProperties props;
  private OutputCollector collector;
  private boolean cleanup = true;

  public ManagerBuilder withCleanup(boolean cleanup) {
    this.cleanup = cleanup;
    return this;
  }

  public ManagerBuilder withProps(LambdaClientProperties props) {
    this.props = props;
    return this;
  }

  public ManagerBuilder withRegion(String region) {
    this.region = region;
    return this;
  }

  public String getRegion() {
    return region;
  }

  public ManagerBuilder withAdditionalModules(List<Module> additionalModules) {
    this.additionalModules.addAll(additionalModules);
    return this;
  }

  public ManagerBuilder withAwsCredentials(Module awsCredentials) {
    this.awsCredentials = awsCredentials;
    return this;
  }

  public ManagerBuilder withConnector(LambdaKinesisConnector connector) {
    this.connector = connector;
    return this;
  }

  public ManagerBuilder withDynamo(IDynamoResource dynamo, Module... supplementalModules) {
    this.dynamo = dynamo;
    withAdditionalModules(asList(supplementalModules));
    return this;
  }

  public ManagerBuilder withStore(SchemaStore store) {
    this.store = Optional.ofNullable(store);
    return this;
  }

  public SchemaStore getStore() {
    return store.get();
  }

  public ManagerBuilder withFirehose(IFirehoseResource firehose, Module... supplementalModules) {
    this.firehose = firehose;
    withAdditionalModules(asList(supplementalModules));
    return this;
  }

  public ManagerBuilder withStreams(IKinesisStreams streams, Module... supplementalModules) {
    this.streams = streams;
    withAdditionalModules(asList(supplementalModules));
    return this;
  }

  public ManagerBuilder withCollector(OutputCollector collector) {
    this.collector = collector;
    return this;
  }

  public ResourceManager build() {
    List<Module> modules = new ArrayList<>();
    if (awsCredentials != null) {
      modules.add(awsCredentials);
    }
    addIfNotNull(dynamo, IDynamoResource.class, modules);
    if (store == null) {
      addIfNotNull(null, SchemaStore.class, modules, new SchemaStoreModule());
    } else if (store.isPresent()) {
      addIfNotNull(store.get(), SchemaStore.class, modules);
    }
    addIfNotNull(firehose, IFirehoseResource.class, modules);
    addIfNotNull(streams, IKinesisStreams.class, modules);
    addIfNotNull(connector, LambdaKinesisConnector.class, modules);

    ResultWaiter.ResultWaiterFactory waiter = new ResultWaiter.ResultWaiterFactory(
      TestProperties.FIVE_MINUTES, TestProperties.ONE_SECOND);
    addIfNotNull(waiter, ResultWaiter.ResultWaiterFactory.class, modules);

    if (props != null) {
      modules.add(instanceModule(props.getRawPropertiesForTesting()));
      modules.add(new PropertiesModule(props.getRawPropertiesForTesting()));
    }

    // removing any previous definitions that we are replacing with the additional modules
    for (Module m : additionalModules) {
      if (m instanceof SingleInstanceModule) {
        Class c = ((SingleInstanceModule) m).getClazz();
        SingleInstanceModule existing = null;
        for (Module present : modules) {
          if (present instanceof SingleInstanceModule &&
              ((SingleInstanceModule) present).getClazz().equals(c)) {
            existing = (SingleInstanceModule) present;
            break;
          }
        }
        modules.remove(existing);
      }
      modules.add(m);
    }

    if (collector == null) {
      collector = new LoggingCollector();
    }
    return new ResourceManager(modules, collector, cleanup);
  }

  private <T> void addIfNotNull(T object, Class<T> clazz, List<Module> modules, Module...
    ifNull) {
    if (object == null) {
      if (ifNull != null) {
        modules.addAll(asList(ifNull));
      }
    } else {
      modules.add(new SingleInstanceModule<>(object, clazz));
    }
  }
}
