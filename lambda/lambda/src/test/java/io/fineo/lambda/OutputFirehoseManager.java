package io.fineo.lambda;

import com.google.common.base.Preconditions;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OutputFirehoseManager {
  private final FirehoseBatchWriter archive;
  private FirehoseBatchWriter processing;
  private FirehoseBatchWriter commit;

  public OutputFirehoseManager(FirehoseBatchWriter... writers) {
    Preconditions.checkArgument(writers.length < 4);
    this.archive = writers.length > 0 ? writers[0] : mock();
    this.processing = writers.length > 1 ? writers[1] : null;
    this.commit = writers.length > 2 ? writers[2] : null;
  }

  public List<ByteBuffer> listenForProcesssingErrors() {
    return listenForErrors(processing);
  }

  public List<ByteBuffer> listenForCommitErrors() {
    return listenForErrors(commit);
  }

  private List<ByteBuffer> listenForErrors(FirehoseBatchWriter writer) {
    List<ByteBuffer> malformedRequests = new ArrayList<>();
    Mockito.doAnswer(invocationOnMock -> malformedRequests.add(
      ((ByteBuffer) invocationOnMock.getArguments()[0]).duplicate())).when(writer).addToBatch
      (Mockito.any());
    return malformedRequests;
  }

  public OutputFirehoseManager withProcessingErrors() {
    this.processing = mock();
    return this;
  }

  public OutputFirehoseManager withCommitErrors() {
    this.commit = mock();
    return this;
  }

  public void verifyErrors() throws IOException {
    verifyErrors(processing);
    verifyErrors(commit);
  }

  private void verifyErrors(FirehoseBatchWriter mock) throws IOException {
    if (mock == null) {
      return;
    }
    Mockito.verify(mock).addToBatch(Mockito.any());
    Mockito.verify(mock).flush();
  }

  private FirehoseBatchWriter mock() {
    return Mockito.mock(FirehoseBatchWriter.class);
  }

  public FirehoseBatchWriter archive() {
    return this.archive;
  }

  public FirehoseBatchWriter process() {
    return this.processing;
  }

  public FirehoseBatchWriter commit() {
    return this.commit;
  }
}
