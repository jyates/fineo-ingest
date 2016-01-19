package io.fineo.lambda.avro;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/**
 * Similar amazon's {@link com.amazonaws.services.kinesis.producer.KinesisProducer}, but uses the
 * standard {@link com.amazonaws.services.kinesis.AmazonKinesisClient} to make requests, rather
 * than leveraging the KPL. This has two advantages:
 * <ol>
 * <li>Works in Lambda functions because it doesn't require access to /tmp</li>
 * <li>Has a much smaller startup time, a huge boon to Lambda functions</li>
 * </ol>
 * <p>
 * All writes are accumulated until a call to {@link #flush()}. That same flush can then be
 * sychronously blocked on by calling {@link #sync()}. This class is <b>not thread-safe</b>, so
 * two successive calls to flush, only the second will be blocked on when calling {@link #sync()}.
 * </p>
 */
public class KinesisProducer {
  private static final Log LOG = LogFactory.getLog(KinesisProducer.class);
  private final AmazonKinesisAsyncClient client;
  PutRecordRequest request;
  private Future<PutRecordResult> written;

  public KinesisProducer(AmazonKinesisAsyncClient client, String streamName) {
    this.client = client;
    request = new PutRecordRequest().withStreamName(streamName);
  }

  public void add(ByteBuffer data, String hashKey, String partitonKey){
    this.written = client.putRecordAsync(request);
  }

  public void flush() {
  }

  public void sync() throws Exception{
  }
}
