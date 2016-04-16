package io.fineo.lambda.dynamo;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders.asExpressionAttributeValue;
import static org.junit.Assert.assertTrue;

public class TestDynamoExpressionPlaceHolders {

  private final Random random = new Random();
  private final String chars = "qwertyuiop[]asdfghjkl;\\'zxcvbnm,./'`1234567890-=~!@#$%^&*()_+";
  private final int charlength = 10;

  @Test
  public void testExpressionNameConversion() {
    for (int i = 0; i < 100; i++) {
      validate(random());
    }
  }


  @Test
  public void testExpressionWithNegativeHashCode() throws Exception{
    String negativeHash = "v&9!-`n%15";
    assertTrue(negativeHash.hashCode() < 0);
    validate(negativeHash);
  }

  private void validate(String value) {
    String attrib = asExpressionAttributeValue(random());
    String suffix = "\n Start: " + value + "\n Attrib: " + attrib;
    assertTrue("Conversion does not start properly." + suffix, attrib.startsWith(":e"));
    char[] rest = attrib.substring(2).toCharArray();
    for (int j = 0; j < rest.length; j++) {
        assertTrue("Conversion is not alphanumer after start." + suffix, Character.isLetterOrDigit
          (rest[j]));
    }
  }

  private String random() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < charlength; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }
}
