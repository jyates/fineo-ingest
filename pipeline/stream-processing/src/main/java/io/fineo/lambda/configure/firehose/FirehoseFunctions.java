package io.fineo.lambda.configure.firehose;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.function.Function;

import static java.util.function.Function.identity;

/**
 * Functions used by the {@link io.fineo.lambda.firehose.FirehoseBatchWriter} when copying bytes
 * to the output streams
 */
public class FirehoseFunctions extends AbstractModule implements Serializable {

  private static final Function<ByteBuffer, ByteBuffer> COPY = ByteBuffer::duplicate;

  private Function archive = COPY;
  // these are populated in the stream processor with the 'correct' data
  private Function malformed = identity();
  private Function commit = identity();

  @Override
  protected void configure() {
    bind(byteFunc()).annotatedWith(Names.named(FirehoseModule.FIREHOSE_ARCHIVE_FUNCTION))
                    .toInstance(archive);
    bind(byteFunc()).annotatedWith(Names.named(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_FUNCTION))
                    .toInstance(malformed);
    bind(byteFunc()).annotatedWith(Names.named(FirehoseModule.FIREHOSE_COMMIT_ERROR_FUNCTION))
                    .toInstance(commit);
  }

  private TypeLiteral<Function<ByteBuffer, ByteBuffer>> byteFunc() {
    return new TypeLiteral<Function<ByteBuffer, ByteBuffer>>() {
    };
  }

  public void setArchive(Function archive) {
    this.archive = archive;
  }

  public void setMalformed(Function malformed) {
    this.malformed = malformed;
  }

  public void setCommit(Function commit) {
    this.commit = commit;
  }
}
