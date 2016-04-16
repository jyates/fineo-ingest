package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Writes avro encoded files into the correct storage locations.
 * <p>
 * Avro files are written to two locations:
 * <ol>
 * <li>Firehose Staging: avro records are encoded one at a time and written to the configured
 * AWS Kinesis Firehose stream for 'archived' records. These are processed at a later time for
 * schema + bulk ingest. They can be processed with the standard
 * {@link FirehoseRecordReader}
 * </li>
 * <li>DynamoDB: Tables are created based on the timerange, with new tables created for every new
 * week as needed. See the dynamo writer for particular around table naming and schema.
 * </li>
 * </ol>
 * </p>
 *
 * @see AvroToDynamoWriter for dynamo particulars
 */
public class LambdaAvroToStorage extends IngestBaseLambda {

  private AvroToDynamoWriter dynamo;

  public LambdaAvroToStorage() {
    super(LambdaClientProperties.STAGED_PREFIX);
  }

  @Override
  void handleEvent(KinesisEvent.KinesisEventRecord record) throws IOException {
    ByteBuffer data = record.getKinesis().getData();
    // convert the raw bytes to a GenericRecord and let the writer deal with writing it
    FirehoseRecordReader<GenericRecord> recordReader = FirehoseRecordReader.create(data);
    GenericRecord reuse = recordReader.next();
    while (reuse != null) {
      this.dynamo.write(reuse);
      reuse = recordReader.next(reuse);
    }
  }

  @Override
  MultiWriteFailures<GenericRecord> commit() throws IOException {
    // get any failed writes and flush them into the right firehose for failures
    return this.dynamo.flush();
  }

  @Override
  protected void setup() throws IOException {
    this.dynamo = AvroToDynamoWriter.create(props);
  }

  @VisibleForTesting
  public void setupForTesting(LambdaClientProperties props, AvroToDynamoWriter dynamo,
    FirehoseBatchWriter archive, FirehoseBatchWriter processingErrors,
    FirehoseBatchWriter commitFailures) {
    super.setupForTesting(archive, processingErrors, commitFailures);
    this.props = props;
    this.dynamo = dynamo;
  }
}
