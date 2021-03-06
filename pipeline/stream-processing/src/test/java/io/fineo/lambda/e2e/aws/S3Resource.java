package io.fineo.lambda.e2e.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Helper to manager interations with s3
 */
public class S3Resource {
  private static final Log LOG = LogFactory.getLog(S3Resource.class);
  private final AmazonS3Client s3;
  private String bucket;

  public S3Resource(AWSCredentialsProvider cred) {
    this.s3 = new AmazonS3Client(cred);
  }

  public S3Resource withBucket(String bucket) {
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

  public void clone(String prefix, File localDir) throws IOException {
    ObjectListing objects = s3.listObjects(bucket, prefix);
    int index = 0;
    for (S3ObjectSummary summary : objects.getObjectSummaries()) {
      File file = new File(localDir, "file-" + (index++));
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
      // copy the s3 file into the local file
      S3Object object = s3.getObject(bucket, summary.getKey());
      IOUtils.copy(object.getObjectContent(), out);
      out.flush();
      out.close();
    }
  }
}
