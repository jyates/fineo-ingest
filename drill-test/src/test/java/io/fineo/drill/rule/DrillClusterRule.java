package io.fineo.drill.rule;

import com.google.common.collect.ImmutableList;
import io.fineo.drill.LocalDrillCluster;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.apache.drill.jdbc.SingleConnectionCachingFactory;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

/**
 * Create and destroy a drill cluster as a junit rule
 */
public class DrillClusterRule extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(DrillClusterRule.class);
  private final LocalDrillCluster drill;

  public DrillClusterRule(int serverCount) {
    drill = new LocalDrillCluster(serverCount);
  }

  @Override
  protected void before() throws Throwable {
    drill.setup();
  }


  @Override
  protected void after() {
    drill.shutdown();
  }

  public Connection getConnection() throws Exception {
    return drill.getConnection();
  }
}
