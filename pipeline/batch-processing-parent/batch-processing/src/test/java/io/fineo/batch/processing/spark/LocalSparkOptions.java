package io.fineo.batch.processing.spark;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.base.Supplier;
import com.google.inject.Guice;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.Pair;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

public class LocalSparkOptions extends BatchOptions{

  private final String writePrefix;
  private final String schemaTable;
  private final String dynamoUrl;

  public LocalSparkOptions(String dynamoUrl, String writePrefix, String schemaTable) {
    this.dynamoUrl = dynamoUrl;
    this.writePrefix = writePrefix;
    this.schemaTable = schemaTable;
  }

  public void setInput(Pair<String, String>... ingestPaths) {
    IngestManifest manifest = getManifest();
    for(Pair<String, String> path :ingestPaths) {
      manifest.add(path.getKey(), path.getValue());
    }
    manifest.flush();
  }

  @Override
  public IngestManifest getManifest() {
    return Guice.createInjector(
      IngestManifestModule.createForTesting(),
      instanceModule(getDynamo()),
      namedInstance(IngestManifestModule.READ_LIMIT, 1l),
      namedInstance(IngestManifestModule.WRITE_LIMIT, 1l)
    ).getInstance(IngestManifest.class);
  }

  @Override
  public RecordToDynamoHandler getDynamoHandler() {
    AmazonDynamoDBAsyncClient client = getDynamo();
    return new RecordToDynamoHandler(new AvroToDynamoWriter(client, 1, new DynamoTableCreator
      (new DynamoTableTimeManager(client, writePrefix), new DynamoDB(client), 1, 1)));
  }

  @Override
  public IFirehoseBatchWriter getFirehoseWriter() {
    return new IFirehoseBatchWriter() {

      @Override
      public void addToBatch(ByteBuffer record) {
        System.out.println("Writing batch record with " + record.remaining() + " bytes");
      }

      @Override
      public void flush() throws IOException {
        //noop
      }
    };
  }

  @Override
  public RawJsonToRecordHandler getRawJsonToRecordHandler(IKinesisProducer queue) {
    DynamoDBRepository repo = new DynamoDBRepository(ValidatorFactory.EMPTY, getDynamo(),
      schemaTable);
    return new RawJsonToRecordHandler("archive", new SchemaStore(repo), queue);
  }

  private AmazonDynamoDBAsyncClient getDynamo() {
    StaticCredentialsProvider provider = new StaticCredentialsProvider(new BasicAWSCredentials
      ("AKIAIZFKPYAKBFDZPAEA",
        "18S1bF4bpjCKZP2KRgbqOn7xJLDmqmwSXqq5GAWq"));
    return new AmazonDynamoDBAsyncClient(provider).withEndpoint(dynamoUrl);
  }
}
