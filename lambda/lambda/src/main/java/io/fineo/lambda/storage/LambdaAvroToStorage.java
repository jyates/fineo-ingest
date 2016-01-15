package io.fineo.lambda.storage;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.lambda.avro.FirehoseBatchWriter;
import io.fineo.lambda.avro.LambdaClientProperties;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Writes avro encoded files into the correct storage locations.
 * <p>
 * Avro files are written to two locations:
 * <ol>
 * <li>Firehose Staging: avro records are encoded one at a time and written to the configured
 * AWS Kinesis Firehose stream for 'staged' records. These are processed at a later time for
 * schema + bulk ingest. They can be processed with the standard
 * {@link FirehoseRecordReader}
 * </li>
 * <li>DynamoDB: Tables are created based on the timerange, with new tables created for every new
 * week as needed. See the dynamo writer for particular around table naming and schema.
 * </li>
 * </ol>
 * </p>
 */
public class LambdaAvroToStorage implements TestableLambda {

  private static final Log LOG = LogFactory.getLog(LambdaAvroToStorage.class);
  private LambdaClientProperties props;
  private FirehoseBatchWriter archiveAllRecords;
  private FirehoseBatchWriter dynamoErrors;
  private AvroToDynamoWriter dynamo;

  public void handler(KinesisEvent event) throws IOException {
    setup();
    handleEventInternal(event);
  }

  @VisibleForTesting
  @Override
  public void handleEventInternal(KinesisEvent event) throws IOException {
    for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
      ByteBuffer data = record.getKinesis().getData();
      this.archiveAllRecords.addToBatch(data);

      // convert the raw bytes to a GenericRecord and let the writer deal with writing it
      FirehoseRecordReader<GenericRecord> recordReader = FirehoseRecordReader.create(data);
      GenericRecord reuse = recordReader.next();
      while (reuse != null) {
        this.dynamo.write(reuse);
        reuse = recordReader.next(reuse);
      }
    }

    // flush the records to the appropriate firehose location
    this.archiveAllRecords.flush();

    // get any failed writes and flush them into the right firehose for failures
    MultiWriteFailures failures = this.dynamo.flush();
    if (failures.any()) {
      FirehoseRecordWriter writer = new FirehoseRecordWriter();
      for (GenericRecord failed : failures.getFailedRecords()) {
        dynamoErrors.addToBatch(writer.write(failed));
      }
      dynamoErrors.flush();
    }
  }

  private void setup() throws IOException {
    props = LambdaClientProperties.load();

    this.archiveAllRecords = getFirehose(props.getFirehoseStagedStreamName());
    this.dynamoErrors = getFirehose(props.getFirehoseStagedDyanmoErrorsName());

    this.dynamo = AvroToDynamoWriter.create(props);
  }

  private FirehoseBatchWriter getFirehose(String name) {
    return new FirehoseBatchWriter(props, ByteBuffer::duplicate, name);
  }

  @VisibleForTesting
  public void setupForTesting(FirehoseBatchWriter records, FirehoseBatchWriter errors,
    AvroToDynamoWriter dynamo){
    this.archiveAllRecords = records;
    this.dynamoErrors = errors;
    this.dynamo = dynamo;
  }
}
