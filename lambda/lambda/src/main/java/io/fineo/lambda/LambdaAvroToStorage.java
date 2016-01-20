package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.lambda.dynamo.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

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
public class LambdaAvroToStorage extends BaseLambda {

  private FirehoseBatchWriter archiveAllRecords;
  private FirehoseBatchWriter dynamoErrorsForTesting;
  private AvroToDynamoWriter dynamo;

  @Override
  void handleEvent(KinesisEvent.KinesisEventRecord record) throws IOException {
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

  @Override
  MultiWriteFailures<GenericRecord> commit() throws IOException {
    // flush the records to the appropriate firehose location
    this.archiveAllRecords.flush();

    // get any failed writes and flush them into the right firehose for failures
    return this.dynamo.flush();
  }

  @Override
  protected void setup() throws IOException {
    this.archiveAllRecords =
      props.lazyFirehoseBatchWriter(props.getFirehoseStagedArchiveStreamName()).get();
    this.dynamo = AvroToDynamoWriter.create(props);
  }

  @Override
  Supplier<FirehoseBatchWriter> getFailedEventHandler() {
    return dynamoErrorsForTesting != null ?
           () -> dynamoErrorsForTesting :
           props.lazyFirehoseBatchWriter(props.getFirehoseStagedDyanmoErrorStreamName());
  }

  @Override
  Supplier<FirehoseBatchWriter> getErrorEventHandler() {
    return dynamoErrorsForTesting != null ?
           () -> dynamoErrorsForTesting :
           props.lazyFirehoseBatchWriter(props.getFirehoseStagedFailedStreamName());
  }


  @VisibleForTesting
  public void setupForTesting(LambdaClientProperties props, FirehoseBatchWriter records,
    FirehoseBatchWriter errors,
    AvroToDynamoWriter dynamo) {
    this.props = props;
    this.archiveAllRecords = records;
    this.dynamoErrorsForTesting = errors;
    this.dynamo = dynamo;
  }
}