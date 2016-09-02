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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    DynamoIngestManifest manifest = manifest(orgId);
    Set<String> files = manifest.getFiles();
    if (files == null) {
      files = new HashSet<>();
      manifest.setFiles(files);
    }
    files.add(s3location);
  }

  public void addFailure(FailedIngestFile failure) {
    DynamoIngestManifest manifest = manifest(failure.getOrg());
    Map<String, List<String>> messages = manifest.getErrors();
    List<String> fileMessages = messages.get(failure.getFile());
    if (fileMessages == null) {
      fileMessages = new ArrayList<>();
      messages.put(failure.getFile(), fileMessages);
    }
    fileMessages.add(failure.getMessage());
  }

  private DynamoIngestManifest manifest(String orgId) {
    DynamoIngestManifest manifest = manifests.get(orgId);
    if (manifest == null) {
      manifest = new DynamoIngestManifest();
      manifest.setOrgID(orgId);
      manifests.put(orgId, manifest);
    }
    return manifest;
  }

  public void flush() {
    // mapper.batchSave uses PutItem which overwrites the set value, even if we set the "update"
    // flag. Thus, we need to manually update the rows ourselves. Fortunately, this isn't too
    // hard as we only have one attribute. Unfortunately, there is no batch 'update item' spec,
    // so we have to leverage our AwsAsyncSubmitter for the hard work.
    // manually (which, fortunately, isn't too bad).
    for (DynamoIngestManifest manifest : manifests.values()) {
      if (manifest.getFiles() != null && manifest.getFiles().size() > 0) {
        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(table);
        request.addKeyEntry("id", new AttributeValue(manifest.getOrgID()));
        updateRequestForMode(request);

        Map<String, AttributeValue> values = new HashMap<>();
        request.withExpressionAttributeValues(values);
        values.put(":0", new AttributeValue(newArrayList(manifest.getFiles())));
        request.withExpressionAttributeValues(values);
        async.submit(new AwsAsyncRequest<>(manifest, request));
        manifest.setFiles(null);
      }
      mapper.save(manifest);
    }

    MultiWriteFailures<DynamoIngestManifest, UpdateItemRequest> failed = async.flush();
    if (failed.any()) {
      throw new RuntimeException("Failed to write all the batches! Failed: " + failed.getActions());
    }

    clear();
  }

  public void clear() {
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

  public Multimap<String, String> files() {
    this.load();
    Multimap<String, String> files = ArrayListMultimap.create();
    for (DynamoIngestManifest manifest : manifests.values()) {
      if (manifest.getFiles() == null) {
        continue;
      }
      files.putAll(manifest.getOrgID(), manifest.getFiles());
    }
    return files;
  }

  public Multimap<String, FailedIngestFile> failures(boolean load) {
    if (load) {
      this.load();
    }

    Multimap<String, FailedIngestFile> files = ArrayListMultimap.create();
    for (DynamoIngestManifest manifest : manifests.values()) {
      // for an org, list the errors we got for each file
      for (Map.Entry<String, List<String>> error : manifest.getErrors().entrySet()) {
        for (String msg : error.getValue()) {
          files.put(manifest.getOrgID(), new FailedIngestFile(manifest.getOrgID(), error
            .getKey(), msg));
        }
      }
    }
    return files;
  }

  private void load() {
    try {
      PaginatedList<DynamoIngestManifest> loaded =
        this.mapper.parallelScan(DynamoIngestManifest.class,
          new DynamoDBScanExpression().withConsistentRead(true), 5);
      loaded.stream().forEach(manifest -> manifests.put(manifest.getOrgID(), manifest));
    } catch (ResourceNotFoundException e) {
      LOG.warn("Ingest manifest table " + table + " does not exist!");
      throw new RuntimeException(e);
    }
  }

  public void remove(String org, String... files) {
    remove(org, asList(files));
  }

  public void remove(String org, Collection<String> files) {
    addMode = false;
    for (String file : files) {
      add(org, file);
    }
  }
}
