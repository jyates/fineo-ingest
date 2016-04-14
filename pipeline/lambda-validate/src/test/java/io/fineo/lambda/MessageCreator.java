package io.fineo.lambda;

import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

/**
 * Create base64 encoded messages for manual lambda testing
 */
public class MessageCreator {

  private static final Log LOG = LogFactory.getLog(MessageCreator.class);

  private static final Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];

  static {
    // put any other properties your want here
  }

  public static void main(String[] args) throws IOException {
    MessageCreator creator = new MessageCreator();
    LOG.info("With data: " + creator.create(json));
  }

  public String create(Map<String, Object> json) throws IOException {
    byte[] array = LambdaTestUtils.asBytes(json);
    return Base64.getEncoder().encodeToString(array);
  }
}
