package io.fineo.drill.rule;

import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.common.config.DrillConfig;
import org.junit.rules.ExternalResource;

/**
 * Clean and destroy a single-node zookeeper cluster
 */
public class ZookeeperClusterRule extends ExternalResource {
  private ZookeeperHelper zkHelper;

  @Override
  protected void before() throws Throwable {
    zkHelper = new ZookeeperHelper();
    zkHelper.startZookeeper(1);
  }

  @Override
  protected void after() {
    zkHelper.stopZookeeper();
  }

  public DrillConfig getConfig() {
    return zkHelper.getConfig();
  }
}
