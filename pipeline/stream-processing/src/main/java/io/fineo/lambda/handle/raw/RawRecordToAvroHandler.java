package io.fineo.lambda.handle.raw;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.KinesisHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;

import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_ARCHIVE_STREAM;
import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM;
import static io.fineo.lambda.configure.firehose.FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM;


/**
 * Lamba function to transform a raw record to an avro schema.
 * <p>
 * Records that are parseable are sent to the Kinesis 'parsed' stream. There may be multiple
 * different types of records in the same event, but they will all be based on the
 * {@link io.fineo.internal.customer.BaseRecord}, allowing access to standard and new fields +
 * mapping.
 * Each record can then be deserialized via the usual {@link org.apache.avro.file.DataFileReader}.
 * </p>
 * <p>
 * Records that are not parsable via the usual schema mechanisms are sent to the 'malformed
 * records' Firehose Kinesis stream.
 * </p>
 */
public class RawRecordToAvroHandler extends KinesisHandler {

  private static final Log LOG = LogFactory.getLog(RawRecordToAvroHandler.class);
  private final JsonParser parser;
  private final RawJsonToRecordHandler jsonHandler;

  @Inject
  public RawRecordToAvroHandler(
    @Named(FIREHOSE_ARCHIVE_STREAM) Provider<FirehoseBatchWriter> archive,
    @Named(FIREHOSE_MALFORMED_RECORDS_STREAM) Provider<FirehoseBatchWriter> processErrors,
    @Named(FIREHOSE_COMMIT_ERROR_STREAM) Provider<FirehoseBatchWriter> commitFailures,
    RawJsonToRecordHandler jsonHandler,
    JsonParser parser) {
    super(archive, processErrors, commitFailures);
    this.parser = parser;
    this.jsonHandler = jsonHandler;
  }


  @VisibleForTesting
  @Override
  public void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException {
    for (Map<String, Object> values : parser
      .parse(new ByteBufferBackedInputStream(rec.getKinesis().getData()))) {
      this.jsonHandler.handle(values);
    }
  }

  public MultiWriteFailures<GenericRecord> commit() throws IOException {
    return this.jsonHandler.commit();
  }
}
