package io.fineo.lambda.e2e.manager;

import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public interface IFirehoseResource extends IResource {
  List<ByteBuffer> getFirehoseWrites(String streamName);

  void ensureNoDataStored();

  void clone(List<Pair<String,StreamType>> toClone, OutputCollector dir) throws IOException;


  public FirehoseBatchWriter getWriter(String prefix, StreamType type);
}
