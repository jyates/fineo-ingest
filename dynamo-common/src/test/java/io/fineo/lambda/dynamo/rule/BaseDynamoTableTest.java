package io.fineo.lambda.dynamo.rule;

import org.junit.ClassRule;
import org.junit.Rule;

/**
 * Helper base class that creates a local Dynamo cluster and cleans up tables after the test run.
 * Notethat you must use the <tt>maven-dependency-plugin</tt> to copy the depdendencies to the
 * <tt>target/</tt> directory when using a local Dynamo instance.
 */
public class BaseDynamoTableTest {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

}
