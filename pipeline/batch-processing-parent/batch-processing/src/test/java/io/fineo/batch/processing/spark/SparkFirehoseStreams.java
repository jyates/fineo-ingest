package io.fineo.batch.processing.spark;

import io.fineo.lambda.e2e.aws.firehose.FirehoseStreams;

import java.util.Map;

/**
 * A {@link FirehoseStreams} that hosts some output locally and some in S3
 */
public class SparkFirehoseStreams extends FirehoseStreams {
  private final Map<String, StreamLookup> streamToLookup;

  public SparkFirehoseStreams(long waitTime, Map<String, StreamLookup> mapping) {
    super(waitTime, null, null);
    this.streamToLookup = mapping;
  }

  @Override
  public String getBucket(String stream) {
    return streamToLookup.get(stream).bucket;
  }

  @Override
  public String getAuthority(String stream) {
    return streamToLookup.get(stream).authority;
  }

  public static class StreamLookup {
    public final String authority;
    public final String bucket;

    public StreamLookup(String authority, String bucket) {
      this.authority = authority;
      this.bucket = bucket;
    }
  }
}
