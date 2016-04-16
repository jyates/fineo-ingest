package io.fineo.drill;

import io.fineo.drill.rule.DrillClusterRule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.client.DrillClient;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;

import static java.lang.String.format;

/**
 *
 */
public class TestReadSparkOutput {

  private static final Log LOG = LogFactory.getLog(TestReadSparkOutput.class);
  private static final String DIR_PROPERTY = "fineo.spark.dir";
  private static final String INFO_FILE = "info.json";
  private static final String DATA_DIR = "data";

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Test
  public void testReadRows() throws Exception {
    String outputDir = System.getProperty(DIR_PROPERTY);
    if(outputDir == null){
      LOG.info(format("Skipping read test - dir not set in property '%s'", DIR_PROPERTY));
      return;
    }

    File info = new File(outputDir, INFO_FILE);
    try (DrillClient client = drill.getClient()) {
      String select = String.format("SELECT count(*) FROM dfs.`%s`", info.getPath());
    }
  }
}
