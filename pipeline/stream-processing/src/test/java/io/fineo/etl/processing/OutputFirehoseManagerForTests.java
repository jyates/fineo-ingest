package io.fineo.etl.processing;

import com.google.common.base.Preconditions;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;


public class OutputFirehoseManagerForTests {
  private FirehoseBatchWriter processing = mock();
  private FirehoseBatchWriter commit = mock();

  public OutputFirehoseManagerForTests(FirehoseBatchWriter... writers) {
    Preconditions.checkArgument(writers.length < 4);
    this.processing = writers.length > 0 ? writers[0] : this.processing;
    this.commit = writers.length > 1 ? writers[1] : this.commit;
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

  public void verifyNoErrors(int writeCount) throws IOException {
    Mockito.verify(process(), times(writeCount)).flush();
    Mockito.verify(process(), never()).addToBatch(Mockito.any());
    Mockito.verifyZeroInteractions(commit());
  }

  public void verifyErrors(FirehoseBatchWriter mock, int failedRecordCount) throws IOException {
    Mockito.verify(mock, times(failedRecordCount)).addToBatch(Mockito.any());
    Mockito.verify(mock).flush();
  }

  private FirehoseBatchWriter mock() {
    return Mockito.mock(FirehoseBatchWriter.class);
  }

  public FirehoseBatchWriter process() {
    return this.processing;
  }

  public FirehoseBatchWriter commit() {
    return this.commit;
  }
}
