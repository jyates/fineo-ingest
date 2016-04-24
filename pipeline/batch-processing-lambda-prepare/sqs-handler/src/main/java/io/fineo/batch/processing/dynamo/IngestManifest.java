package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.HashSet;
import java.util.Set;

@DynamoDBTable(tableName = "ingest-manifest")
public class IngestManifest {

  private String orgID;
  private Set<String> files = new HashSet<>();

  @DynamoDBHashKey(attributeName="Id")
  public String getOrgID() {
    return orgID;
  }
  public void setOrgID(String orgID) {
    this.orgID = orgID;
  }

  @DynamoDBAttribute(attributeName = "Files")
  public Set<String> getFiles() {
    return files;
  }
  public void setFiles(Set<String> files) {
    this.files = files;
  }
}
