package io.fineo.batch.processing.spark;

import io.fineo.aws.AwsDependentTests;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.dynamo.avro.IAvroToDynamoWriter;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.avro.generic.GenericRecord;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Properties;

import static java.lang.String.format;

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
        return new RecordToDynamoHandler(new IAvroToDynamoWriter(){

          @Override
          public void write(GenericRecord record) {
            System.out.println("Got record: "+record);
          }

          @Override
          public MultiWriteFailures<GenericRecord> flush() {
            return new MultiWriteFailures<>(Collections.EMPTY_LIST);
          }
        });
      }

      @Override
      public FirehoseBatchWriter getFirehoseWriter() {
        return new FirehoseBatchWriter()
      }
    };
    withInput(options, "single-row.json");

    BatchProcessor processor = new BatchProcessor(options);
    processor.run(spark.jsc());

    // validate the output
  }

  private void withInput(LocalMockBatchOptions options, String fileInTestResources){
    options.setInput(format("pipeline/batch-processing-parent/batch-processing/src/test/resources"
                            + "/%s", fileInTestResources));
  }

}
