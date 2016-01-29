package io.fineo.lambda.resources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper to delete things from s3
 */
public class S3Delete {
  private static final Log LOG = LogFactory.getLog(S3Delete.class);
  private final AmazonS3Client s3;
  private String bucket;

  public S3Delete(AWSCredentialsProvider cred) {
    this.s3 = new AmazonS3Client(cred);
  }

  public S3Delete withBucket(String bucket) {
    this.bucket = bucket;
    return this;
  }

  public void delete(String prefix) {
    // list all the objects in the prefix
    LOG.info("Attempting to delete: " + bucket + "/" + prefix);
    ObjectListing objects = s3.listObjects(bucket, prefix);
    DeleteObjectsRequest del = new DeleteObjectsRequest(bucket);
    del.withKeys(objects.getObjectSummaries().stream()
                        .map(summary -> summary.getKey())
                        .peek(name -> LOG.info("Deleting: " + name))
                        .toArray(size -> new String[size]));
    if (del.getKeys().size() == 0) {
      LOG.info("No objects to delete - done!");
      return;
    }
    s3.deleteObjects(del);

    //ensure there are no more left
    ObjectListing meta = s3.listObjects(bucket, prefix);
    assert meta.getObjectSummaries().size() == 0 :
      "Got some left over entries: " + meta.getObjectSummaries();
  }
}