package io.fineo.lambda.configure.firehose;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.google.inject.name.Named;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 */
public class TestFirehoseModule {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Bind
  @Named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME)
  private String field = "archive";

  @Bind
  @Named(FineoProperties.FIREHOSE_URL)
  private String url = "https://firehose.us-east-1.amazonaws.com";

  @Bind
  AWSCredentialsProvider provider = new StaticCredentialsProvider(new BasicAWSCredentials
    ("key", "secret"));

  @Test
  public void testLazyLoadingModules() throws Exception {
    // its not actually a valid firehose, so the construction will fail with an AWS exception,
    // rather than a setup exception
    thrown.expect(ProvisionException.class);
    thrown.expectCause(new BaseMatcher<Throwable>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof RuntimeException) {
          return ((RuntimeException) item).getCause() instanceof AmazonServiceException;
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("cause should be an ").appendValue(RuntimeException.class)
                   .appendText(" caused by ").appendValue(AmazonServiceException.class);
      }
    });
    Guice.createInjector(BoundFieldModule.of(this),
      new FirehoseModule().withArchive(),
      new FirehoseFunctions())
         .getInstance(Archive.class);
  }

  public static class Archive {
    private final IFirehoseBatchWriter writer;

    @Inject
    public Archive(@Named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM) IFirehoseBatchWriter writer) {
      this.writer = writer;
    }
  }
}
