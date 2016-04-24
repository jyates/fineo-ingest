package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoIngestManifest {

  private Map<String, IngestManifest> manifests = new HashMap<>();
  private final DynamoDBMapper mapper;

  public DynamoIngestManifest(DynamoDBMapper mapper) {
    this.mapper = mapper;
  }

  public void add(String orgId, String s3location) {
    IngestManifest manifest = manifests.get(orgId);
    if (manifest == null) {
      manifest = new IngestManifest();
      manifest.setOrgID(orgId);
      manifests.put(orgId, manifest);
    }
    manifest.getFiles().add(s3location);
  }

  public void flush() {
    List<DynamoDBMapper.FailedBatch> failed = mapper.batchWrite(manifests.values(), null);
    if (failed.size() > 0) {
      throw new RuntimeException("Failed to write all the batches! Failed: " + failed);
    }
  }
}
