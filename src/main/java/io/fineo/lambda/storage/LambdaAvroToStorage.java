package io.fineo.lambda.storage;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.avro.FirehoseBatchWriter;
import io.fineo.lambda.avro.FirehoseClientProperties;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.TranslatedSeekableInput;
import org.apache.avro.generic.GenericRecord;

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
public class LambdaAvroToStorage {

  private FirehoseClientProperties props;
  private FirehoseBatchWriter firehose;
  private FirehoseBatchWriter dynamoErrors;
  private AvroToDynamoWriter dynamo;

  public void handler(KinesisEvent event) throws IOException {
    setup();
    handleEvent(event);
  }

  private void handleEvent(KinesisEvent event) throws IOException {
    for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
      ByteBuffer data = record.getKinesis().getData();
      this.firehose.addToBatch(data);

      // convert the raw bytes to a GenericRecord and let the writer deal with writing it
      FirehoseRecordReader<GenericRecord> recordReader = new FirehoseRecordReader<>(
        new TranslatedSeekableInput(data.arrayOffset(), data.limit(),
          new SeekableByteArrayInput(data.array())));
      GenericRecord reuse = recordReader.next();
      while (reuse != null) {
        try {
          this.dynamo.write(reuse);
          dynamoErrors.addToBatch(data);
        } catch (Exception e) {

        }
        reuse = recordReader.next(reuse);
      }
    }
    this.firehose.flush();
    this.dynamo.flush();
  }

  private void setup() throws IOException {
    props = FirehoseClientProperties.load();

    this.firehose = getFirehose(props.getFirehoseStagedStreamName());
    this.dynamoErrors = getFirehose(props.getFirehoseStagedDyanmoErrorsName());

    this.dynamo = AvroToDynamoWriter.create(props);
  }

  private FirehoseBatchWriter getFirehose(String name) {
    return new FirehoseBatchWriter(props, ByteBuffer::duplicate, name);
  }
}
