package io.fineo.batch.processing.spark;

import io.fineo.aws.AwsDependentTests;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Properties;

/**
 * Batch processor testing against local resources
 */
@Category(AwsDependentTests.class)
public class TestBatchProcessor {

  private static final String REGION = "us-east-1";
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);
  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule();
  @Rule
  public TestOutput output = new TestOutput(false);

  @Test
  public void testProcessJson() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(FineoProperties.DYNAMO_REGION, REGION);

    RecordToDynamoHandler dynamo = Mockito.mock(RecordToDynamoHandler.class);
    FirehoseBatchWriter firehose = Mockito.mock(FirehoseBatchWriter.class);
    LocalMockBatchOptions options = new LocalMockBatchOptions(){
      @Override
      public RecordToDynamoHandler getDynamoHandler() {
        return dynamo;
      }

      @Override
      public FirehoseBatchWriter getFirehoseWriter() {
        return firehose;
      }
    };
    options.setInput("resources/single-row.json");

    BatchProcessor processor = new BatchProcessor(options);
    processor.run(spark.jsc());

    // validate the output
  }


}
