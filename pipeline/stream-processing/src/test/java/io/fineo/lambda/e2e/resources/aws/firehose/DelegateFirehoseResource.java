package io.fineo.lambda.e2e.resources.aws.firehose;

import com.google.inject.Injector;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.manager.collector.OutputCollector;
import io.fineo.lambda.e2e.resources.manager.IFirehoseResource;
import io.fineo.lambda.e2e.resources.manager.ManagerBuilder;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.schema.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

public class DelegateFirehoseResource implements IFirehoseResource {

  private FirehoseResource firehose;

  @Override
  public void init(Injector injector) {
    this.firehose = injector.getInstance(FirehoseResource.class);
  }

  @Override
  public void cleanup(FutureWaiter waiter) {
    firehose.cleanup(waiter);
  }

  public static void add(ManagerBuilder builder) {
    add(builder,
      new FirehoseStreams(2 * TestProperties.ONE_MINUTE, "s3",
        TestProperties.Firehose.S3_BUCKET_NAME));
  }

  public static void add(ManagerBuilder builder, FirehoseStreams stream) {
    builder.withFirehose(new DelegateFirehoseResource(), instanceModule(stream));
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    return this.firehose.read(streamName);
  }

  @Override
  public void ensureNoDataStored() {
    firehose.ensureNoDataStored();
  }

  @Override
  public void clone(List<Pair<String, StreamType>> toClone, OutputCollector dir)
    throws IOException {
    this.firehose.clone(toClone, dir);
  }

  @Override
  public FirehoseBatchWriter getWriter(String prefix, StreamType type) {
    throw new UnsupportedOperationException("AWS Firehoses cannot provide firehose batch writer!");
  }
}
