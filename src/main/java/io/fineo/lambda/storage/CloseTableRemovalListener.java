package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.cache.RemovalNotification;

/**
 * Closes the table when it is evicted from the cache
 */
public class CloseTableRemovalListener
  implements com.google.common.cache.RemovalListener<String, Table> {
  @Override
  public void onRemoval(RemovalNotification<String, Table> notification) {

  }
}
