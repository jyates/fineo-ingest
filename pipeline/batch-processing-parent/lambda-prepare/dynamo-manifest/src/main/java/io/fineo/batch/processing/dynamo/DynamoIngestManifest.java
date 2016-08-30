package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.HashSet;
import java.util.Set;

import static io.fineo.batch.processing.dynamo.DynamoIngestManifest.DEFAULT_NAME;

@DynamoDBTable(tableName = DEFAULT_NAME)
public class DynamoIngestManifest {

  public static final String DEFAULT_NAME = "ingest-batch-manifest";

  private String orgID;
  private Set<String> files = new HashSet<>();

  @DynamoDBHashKey(attributeName="id")
  public String getOrgID() {
    return orgID;
  }
  public void setOrgID(String orgID) {
    this.orgID = orgID;
  }
  public Set<String> getFiles() {
    return files;
  }

  @DynamoDBAttribute
  public void setFiles(Set<String> files) {
    this.files = files;
  }

  @Override
  public String toString() {
    return "DynamoIngestManifest{" +
           "orgID='" + orgID + '\'' +
           ", files=" + files +
           '}';
  }
}
