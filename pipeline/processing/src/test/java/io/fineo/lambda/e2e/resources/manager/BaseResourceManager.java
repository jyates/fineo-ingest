package io.fineo.lambda.e2e.resources.manager;

import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;

import java.util.Map;

public abstract class BaseResourceManager implements ResourceManager {

  protected final LambdaKinesisConnector connector;

  protected BaseResourceManager(LambdaKinesisConnector connector) {
    this.connector = connector;
  }

  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    byte[] start = LambdaTestUtils.asBytes(json);
    this.connector.write(start);
    return start;
  }
}
