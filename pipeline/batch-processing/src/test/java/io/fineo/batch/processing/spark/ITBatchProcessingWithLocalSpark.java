package io.fineo.batch.processing.spark;

import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.ITEndToEndLambdaLocal;
import io.fineo.lambda.e2e.aws.BaseITEndToEndAwsServices;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Properties;

@Category(AwsDependentTests.class)
public class ITBatchProcessingWithLocalSpark extends BaseITEndToEndAwsServices {

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule();
  @Rule
  public TestOutput output = new TestOutput(false);

  public ITBatchProcessingWithLocalSpark() {
    super(true);
  }

  @Test
  public void testHappyPath() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    Properties props = setProperties(uuid);

    SparkResourceManager manager = new SparkResourceManager(spark.jsc(), output);
    EndToEndTestRunner runner = new EndToEndTestRunner(props, manager);
    ITEndToEndLambdaLocal.TestState state =
      new ITEndToEndLambdaLocal.TestState(null, runner, manager);
    Map<String, Object> event = LambdaTestUtils.createRecords(1, 1)[0];
    ITEndToEndLambdaLocal.run(state, event);
  }
}
