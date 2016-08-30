package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;

/**
 * Manifest for tracking ingest. It can only be used for adding files ({@link #add(String,
 * String)} or removing files {@link #remove(String, String...)}, but not both. {@link #flush()}
 * will take just one of the operation type (default: add, any remove operation with turn on
 * 'remove' mode) and use that for all updates. Calls to {@link #flush()} will cause the operation
 * mode to reset
 */
public class IngestManifest {

  private static final Logger LOG = LoggerFactory.getLogger(IngestManifest.class);

  private boolean addMode = true;
  private final String table;
  private final AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> async;
  private Map<String, DynamoIngestManifest> manifests = new HashMap<>();
  private final DynamoDBMapper mapper;

  public IngestManifest(DynamoDBMapper mapper, String tableName, AwsAsyncSubmitter submitter) {
    this.mapper = mapper;
    this.table = tableName;
    this.async = submitter;
  }

  public void add(String orgId, String s3location) {
    DynamoIngestManifest manifest = manifests.get(orgId);
    if (manifest == null) {
      manifest = new DynamoIngestManifest();
      manifest.setOrgID(orgId);
      manifests.put(orgId, manifest);
    }
    manifest.getFiles().add(s3location);
  }

  public void flush() {
    // mapper.batchSave uses PutItem which overwrites the set value, even if we set the "update"
    // flag. Thus, we need to manually update the rows ourselves. Fortunately, this isn't too
    // hard as we only have one attribute. Unfortunately, there is no batch 'update item' spec,
    // so we have to leverage our AwsAsyncSubmitter for the hard work.
    // manually (which, fortunately, isn't too bad).
    for (DynamoIngestManifest manifest : manifests.values()) {
      UpdateItemRequest request = new UpdateItemRequest();
      request.setTableName(table);
      request.addKeyEntry("id", new AttributeValue(manifest.getOrgID()));
      updateRequestForMode(request);

      Map<String, AttributeValue> values = new HashMap<>();
      request.withExpressionAttributeValues(values);
      values.put(":0", new AttributeValue(newArrayList(manifest.getFiles())));
      request.withExpressionAttributeValues(values);
      async.submit(new AwsAsyncRequest<>(manifest, request));
    }

    MultiWriteFailures<DynamoIngestManifest> failed = async.flush();
    if (failed.any()) {
      throw new RuntimeException("Failed to write all the batches! Failed: " + failed.getActions());
    }
    manifests.clear();
    this.addMode = true;
  }

  private void updateRequestForMode(UpdateItemRequest request) {
    if (addMode) {
      request.withUpdateExpression("ADD files :0");
    } else {
      request.withUpdateExpression("DELETE files :0");
    }
  }

  public void load() {
    try {
      PaginatedList<DynamoIngestManifest> loaded =
        this.mapper.parallelScan(DynamoIngestManifest.class,
          new DynamoDBScanExpression().withConsistentRead(true), 5);
      loaded.stream().forEach(manifest -> manifests.put(manifest.getOrgID(), manifest));
    } catch (ResourceNotFoundException e) {
      LOG.warn("Ingest manifest table " + table + " does not exist!");
    }
  }

  public Multimap<String, String> files() {
    Multimap<String, String> files = ArrayListMultimap.create();
    for (DynamoIngestManifest manifest : manifests.values()) {
      files.putAll(manifest.getOrgID(), manifest.getFiles());
    }
    return files;
  }

  public void remove(String org, String... files) {
    remove(org, asList(files));
  }

  public void remove(String org, Iterable<String> files) {
    addMode = false;
    for (String file : files) {
      add(org, file);
    }
  }
}
