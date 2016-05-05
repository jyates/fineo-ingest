package io.fineo.lambda.e2e.aws;

import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.aws.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.util.LambdaTestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.handle.util.HandlerUtils.getHandler;

/**
 * Runs the lambda functions themselves locally, but all the other AWS services are run against
 * AWS itself
 */
@Category(AwsDependentTests.class)
public class ITEndToEndLambdaSemiLocal extends BaseITEndToEndAwsServices {

  public ITEndToEndLambdaSemiLocal() {
    super(true);
  }

  @Test(timeout = 800000)
  public void testEndToEndSuccess() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    Properties props = setProperties(uuid);

    // setup the stages
    List<Module> modules = updateCredentials(RawStageWrapper.getModules(props));
    RawStageWrapper start = new RawStageWrapper(modules);
    modules = updateCredentials(AvroToStorageWrapper.getModules(props));
    AvroToStorageWrapper storage = new AvroToStorageWrapper(modules);

    // create the connections between stages
    String kinesisIngest = uuid + "ingest";
    String kinesisConnector = getProps().getRawToStagedKinesisStreamName();
    Map<String, List<IngestUtil.Lambda>> mapping =
      IngestUtil.newBuilder()
                .then(kinesisIngest, start, getHandler(start))
                .then(kinesisConnector, storage, getHandler(storage))
                .build();
    LambdaKinesisConnector connector = new LocalLambdaRemoteKinesisConnector();
    connector.configure(mapping, kinesisIngest);
    run(connector, LambdaTestUtils.createRecords(1, 1));
  }

  /**
   * Remove the default credentials and use the ones provided by the test class
   */
  private List<Module> updateCredentials(List<Module> modules) {
    List<Module> ret = new ArrayList<>(modules.size() - 1);
    for (Module module : modules) {
      if (module instanceof DefaultCredentialsModule) {
        continue;
      }
      ret.add(module);
    }
    ret.add(getCredentialsModule());

    return ret;
  }
}
