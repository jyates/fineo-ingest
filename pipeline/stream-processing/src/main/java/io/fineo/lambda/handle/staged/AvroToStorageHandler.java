package io.fineo.lambda.handle.staged;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.KinesisHandler;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_ARCHIVE_STREAM;
import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM;
import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM;

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
public class AvroToStorageHandler extends KinesisHandler {

  private AvroToDynamoWriter dynamo;

  @Inject
  public AvroToStorageHandler(
    @Named(FIREHOSE_ARCHIVE_STREAM) Provider<FirehoseBatchWriter> archive,
    @Named(FIREHOSE_MALFORMED_RECORDS_STREAM) Provider<FirehoseBatchWriter> processErrors,
    @Named(FIREHOSE_COMMIT_ERROR_STREAM) Provider<FirehoseBatchWriter> commitFailures,
    AvroToDynamoWriter dynamo) {
    super(LambdaClientProperties.STAGED_PREFIX, archive, processErrors, commitFailures);
    this.dynamo = dynamo;
  }

  @Override
  public void handleEvent(KinesisEvent.KinesisEventRecord record) throws IOException {
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
  public MultiWriteFailures<GenericRecord> commit() throws IOException {
    // get any failed writes and flush them into the right firehose for failures
    return this.dynamo.flush();
  }
}
