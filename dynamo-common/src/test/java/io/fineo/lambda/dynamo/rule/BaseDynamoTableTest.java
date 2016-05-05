package io.fineo.lambda.dynamo.rule;

import com.google.inject.Module;
import org.junit.ClassRule;
import org.junit.Rule;

import java.util.List;

import static java.util.Arrays.asList;

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


  protected List<Module> getDynamoModules(){
    return asList(dynamo.getCredentialsModule(), tables.getDynamoModule());
  }
}
