package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import com.google.common.collect.Maps;
import com.google.inject.Provider;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.TableUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.SS;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class IngestManifest {

  private static final Log LOG = LogFactory.getLog(IngestManifest.class);

  private final AmazonDynamoDBClient client;
  private final DynamoDB dynamo;
  private final Provider<ProvisionedThroughput> throughput;
  private final AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> async;
  private Map<String, DynamoIngestManifest> manifests = new HashMap<>();
  private final DynamoDBMapper mapper;
  private CreateTableRequest create;

  public IngestManifest(DynamoDBMapper mapper, AmazonDynamoDBClient dynamo,
    Provider<ProvisionedThroughput> throughput, AwsAsyncSubmitter submitter) {
    this.mapper = mapper;
    this.client = dynamo;
    this.dynamo = new DynamoDB(client);
    this.throughput = throughput;
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
    ensureTable();

    // mapper.batchSave uses PutItem which overwrites the set value, even if we set the "update"
    // flag. Thus, we need to manually update the rows ourselves. Fortunately, this isn't too
    // hard as we only have one attribute. Unfortunately, there is no batch 'update item' spec,
    // so we have to leverage our AwsAsyncSubmitter for the hard work.
    // manually (which, fortunately, isn't too bad).
    for (DynamoIngestManifest manifest : manifests.values()) {
      UpdateItemRequest request = new UpdateItemRequest();
      request.setTableName(getCreate().getTableName());
      request.addKeyEntry("id", new AttributeValue(manifest.getOrgID()));
      request.withUpdateExpression("ADD files :0");

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
  }

  public void load() {
    try {
      PaginatedList<DynamoIngestManifest> loaded =
        this.mapper.parallelScan(DynamoIngestManifest.class,
          new DynamoDBScanExpression().withConsistentRead(true), 5);
      loaded.stream().forEach(manifest -> manifests.put(manifest.getOrgID(), manifest));
    } catch (ResourceNotFoundException e) {
      LOG.warn("Ingest manifest table " + getCreate().getTableName() + " does not exist!");
    }
  }

  public Collection<String> files() {
    return manifests.values()
                    .stream()
                    .flatMap(manifest -> manifest.getFiles().stream())
                    .collect(toList());
  }

  public void remove(String org, String ... files){
    remove(org, asList(files));
  }

  private void remove(String org, List<String> files) {

  }

  private void ensureTable() {
    CreateTableRequest create = getCreate();
    try {
      client.describeTable(create.getTableName());
    } catch (ResourceNotFoundException e) {
      create.setProvisionedThroughput(throughput.get());
      TableUtils.createTable(dynamo, create);
    }
  }

  private CreateTableRequest getCreate() {
    if (this.create == null) {
      this.create = mapper.generateCreateTableRequest(DynamoIngestManifest.class);
    }
    return this.create;
  }
}
