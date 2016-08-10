package io.fineo.lambda.dynamo;

/**
 * Utility class to manage place holders for writing to dynamo, in accordance with <a
 * href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html#ExpressionAttributeNames"> AWS Dynamo Expressions</a>.
 * <p>
 * Note that we don't need to modify the expression name because we already create a 'valid'
 * name with the standard {@link io.fineo.schema.avro.SchemaNameGenerator}
 * </p>
 */
public class DynamoExpressionPlaceHolders {

  private DynamoExpressionPlaceHolders() {
  }

  public static String asExpressionName(String name){
    int num = name.hashCode();
    if (num < 0) {
      num = -num;
    }
    return "#n" + num;
  }

  /**
   * Convert the value to a valid expression value placeholder
   */
  public static String asExpressionAttributeValue(String name) {
    int num = name.hashCode();
    if (num < 0) {
      num = -num;
    }
    return ":e" + num;
  }
}
