package io.fineo.batch.processing.spark;

import io.fineo.lambda.e2e.resources.aws.firehose.FirehoseStreams;

import java.util.Map;

/**
 * A {@link FirehoseStreams} that hosts some output locally and some in S3
 */
public class SparkFirehoseStreams extends FirehoseStreams {
  private final Map<String, StreamLookup> streamToLookup;

  public SparkFirehoseStreams(long waitTime, Map<String, StreamLookup> mapping) {
    super(waitTime, null, null);
    this.streamToLookup =mapping;
  }

  @Override
  public String getBucket(String stream) {
    return streamToLookup.get(stream).bucket;
  }

  @Override
  public String getAuthority(String stream) {
    return streamToLookup.get(stream).authority;
  }

  @Override
  public String getPath(String streamName) {
    return streamToLookup.get(streamName).prefix;
  }

  public static class StreamLookup{
    public final String authority;
    public final String bucket;
    public final String prefix;

    public StreamLookup(String authority, String bucket, String prefix) {
      this.authority = authority;
      this.bucket = bucket;
      this.prefix = prefix;
    }
  }
}
