package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.fineo.batch.processing.dynamo.DynamoIngestManifest.DEFAULT_NAME;

@DynamoDBTable(tableName = DEFAULT_NAME)
public class DynamoIngestManifest {

  public static final String DEFAULT_NAME = "unspecified-manifest-table-name";

  private String orgID;
  private Set<String> files = new HashSet<>();
  private List<String> errors = new ArrayList<>();

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

  @DynamoDBAttribute
  public void setErrorMessage(List<String> errors){
    this.errors = errors;
  }

  public List<String> getErrors() {
    return errors;
  }

  @Override
  public String toString() {
    return "DynamoIngestManifest{" +
           "orgID='" + orgID + '\'' +
           ", files=" + files +
           '}';
  }
}
