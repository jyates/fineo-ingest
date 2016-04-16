package io.fineo.drill.rule;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.server.Drillbit;
import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.Properties;

/**
 * Create and destory a drill cluster
 */
public class DrillClusterRule extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(DrillClusterRule.class);
  private final int serverCount;
  private final ZookeeperClusterRule zk = new ZookeeperClusterRule();
  private List<Drillbit> servers;

  private final Properties props = new Properties();

  {
    props.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    props.put(ExecConstants.HTTP_ENABLE, "false");
  }

  public DrillClusterRule(int serverCount) {
    this.serverCount = serverCount;
  }

  @Override
  protected void before() throws Throwable {
    zk.before();

    // turn off the HTTP server to avoid port conflicts between the drill bits
    System.setProperty(ExecConstants.HTTP_ENABLE, "false");
    ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
    for (int i = 0; i < serverCount; i++) {
      servers.add(Drillbit.start(zk.getConfig()));
    }
    this.servers = servers.build();
  }


//  @Override
//  protected void after() {
//    DrillMetrics.resetMetrics();
//
//    if (servers != null) {
//      for (Drillbit server : servers) {
//        try {
//          server.close();
//        } catch (Exception e) {
//          LOG.error("Error shutting down Drillbit", e);
//        }
//      }
//    }
//
//    zk.after();
//  }

  public DrillClient getClient() {
    return new DrillClient(zk.getConfig());
  }
}
