package io.fineo.lambda.e2e.aws.firehose;

import com.google.common.base.Preconditions;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.util.TestProperties;
import io.fineo.lambda.util.Join;
import io.fineo.schema.Pair;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Manage the firehose streams and s3 locations.
 * <p>
 * This class is thread-safe
 * </p>
 */
public class FirehoseStreams {

  private final String bucket;
  private Map<Pair<String, StreamType>, FirehoseInfo> streams =
    new ConcurrentHashMap<>();
  private Map<String, S3LocationInfo> s3LocationReferenceCounter = new ConcurrentHashMap<>();
  private long waitTime;
  private String authority;

  public FirehoseStreams(long waitTime, String authority, String bucket) {
    this.waitTime = waitTime;
    this.authority = authority;
    this.bucket = bucket;
  }

  public void store(Pair<String, StreamType> stream, String streamName,
    String s3Path) {
    FirehoseInfo info = new FirehoseInfo(streamName, s3Path);
    assert streams.putIfAbsent(stream, info) == null :
      "Someone already put a firehose info for " + stream;

    // increment the counter on the location
    S3LocationInfo ref = s3LocationReferenceCounter.get(s3Path);
    // no one else stored a location, so attempt to create one
    if (ref == null) {
      ref = new S3LocationInfo();
      // try to put this location. However, someone may have put one already, so we grab that, if
      // its already been put
      if (s3LocationReferenceCounter.putIfAbsent(s3Path, ref) != null) {
        ref = s3LocationReferenceCounter.get(s3Path);
      }
    }
    ref.claim();
  }

  public String getPath(String streamName) {
    FirehoseInfo fh = null;
    for (FirehoseInfo entry : this.streams.values()) {
      if (entry.streamName.equals(streamName)) {
        fh = entry;
        break;
      }
    }
    Preconditions.checkNotNull(fh, "No matching entry for stream: " + streamName);
    return fh.s3Path;
  }

  /**
   * Determine what timeout we should used based on the stage. Each stage should be waited
   * independently, since records may have not made it to the next stage.
   * <p>
   * Firehose takes a minute to flush results to s3. We wait doublee that, just in case, the
   * first time, but after that all writes should have been flushed, so we just read it
   * immediately, for each stage
   * </p>
   *
   * @param s3Path
   */
  public long getTimeout(String s3Path) {
    S3LocationInfo location = s3LocationReferenceCounter.get(s3Path);
    Preconditions.checkNotNull(location, "Don't have a location reference for s3: " + s3Path);
    long timeout = location.read ? TestProperties.ONE_SECOND : waitTime;
    location.read = true;
    return timeout;
  }

  /**
   * @return the names of all the other firehoses
   */
  public Collection<String> firehoseNames() {
    return this.streams.values().stream().map(fh -> fh.streamName).collect(Collectors.toSet());
  }

  /**
   * Remove a claim to an S3 location made by given stream
   *
   * @param s3Path s3 location
   * @return <tt>true</tt> if there are no more claims to that path and you can delete it without
   * stepping on another stream's output location
   */
  public boolean remove(String s3Path) {
    S3LocationInfo loc = this.s3LocationReferenceCounter.get(s3Path);
    Preconditions.checkNotNull(loc, "Got a null location reference for: " + s3Path);
    return loc.release();
  }

  /**
   * @return string describing all the streams' s3 locations that are still claimed because they
   * weren't released by calls to {@link #remove(String)}
   */
  public String claimedToString() {
    return streams.entrySet().stream().filter(entry -> {
      FirehoseInfo info = entry.getValue();
      S3LocationInfo s3 = s3LocationReferenceCounter.get(info.s3Path);
      return s3.referenceCounter.get() == 0;
    }).map(entry -> {
      Pair<String, StreamType> streamId = entry.getKey();
      FirehoseInfo info = entry.getValue();
      S3LocationInfo s3 = s3LocationReferenceCounter.get(info.s3Path);
      return "(" + streamId + ") -> (" + info.streamName + ", " + info.s3Path + ": Cnt: " + s3
        .referenceCounter + ")";
    }).reduce(
      null, Join.on(",\n"));
  }

  public String getBucket(String stream) {
    return this.bucket;
  }

  public String getAuthority(String streamName) {
    return this.authority;
  }

  private class FirehoseInfo {
    private String streamName;
    private String s3Path;

    public FirehoseInfo(String stream, String s3Path) {
      this.streamName = stream;
      this.s3Path = s3Path;
    }

    @Override
    public String toString() {
      return "{" +
             "streamName='" + streamName + '\'' +
             ", s3Path='" + s3Path + '\'' +
             '}';
    }
  }

  private class S3LocationInfo {
    private AtomicInteger referenceCounter = new AtomicInteger(0);
    private boolean read = false;

    public void claim() {
      this.referenceCounter.incrementAndGet();
    }

    public boolean release() {
      return this.referenceCounter.decrementAndGet() <= 0;
    }
  }
}
