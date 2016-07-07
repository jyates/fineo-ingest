package io.fineo.drill;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.apache.drill.jdbc.SingleConnectionCachingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

/**
 *
 */
public class LocalDrillCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDrillCluster.class);
  private ZookeeperHelper zkHelper;
  private final int serverCount;
  private final SingleConnectionCachingFactory factory;
  private final Properties props = new Properties();
  private List<Drillbit> servers;

  {
    props.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    props.put(ExecConstants.HTTP_ENABLE, "false");
  }

  public LocalDrillCluster(int serverCount) {
    this.serverCount = serverCount;
    this.factory =  new SingleConnectionCachingFactory(new ConnectionFactory() {
      @Override
      public Connection getConnection(ConnectionInfo info) throws Exception {
        Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
      }
    });
    zkHelper = new ZookeeperHelper();
  }

  public void setup() throws Throwable {
    zkHelper.startZookeeper(1);

    // turn off the HTTP server to avoid port conflicts between the drill bits
    System.setProperty(ExecConstants.HTTP_ENABLE, "false");
    ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
    for (int i = 0; i < serverCount; i++) {
      servers.add(Drillbit.start(zkHelper.getConfig()));
    }
    this.servers = servers.build();
  }


  public void shutdown() {
    DrillMetrics.resetMetrics();

    if (servers != null) {
      for (Drillbit server : servers) {
        try {
          server.close();
        } catch (Exception e) {
          LOG.error("Error shutting down Drillbit", e);
        }
      }
    }

    zkHelper.stopZookeeper();
  }

  public Connection getConnection() throws Exception {
    String zkConnection = zkHelper.getConfig().getString("drill.exec.zk.connect");
    String url = format("jdbc:drill:zk=%s", zkConnection);
    return factory.getConnection(new ConnectionInfo(url, new Properties()));
  }
}
