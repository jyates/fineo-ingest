package io.fineo.lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import io.fineo.aws.rule.AwsCredentialResource;
import org.junit.ClassRule;
import org.junit.Test;

/**
 *
 */
public class TestTmp {

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  private String s3BucketName = "test.fineo.io";
  @Test
  public void readS3() throws Exception{
    AmazonS3 s3 = new AmazonS3Client(awsCredentials.getProvider());
    ObjectListing listing = s3.listObjects(s3BucketName);
    System.out.println("Listing: "+listing);
  }
}
