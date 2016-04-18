package io.fineo.drill;

import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.etl.processing.JsonParser;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITReadSparkOutput {

  private static final Log LOG = LogFactory.getLog(ITReadSparkOutput.class);
  private static final String DIR_PROPERTY = "fineo.spark.dir";
  private static final String INFO_FILE = "info.json";
  private static final String DATA_DIR = "output";

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Test
  public void testReadRows() throws Exception {
    String outputDir = System.getProperty(DIR_PROPERTY);
    if (outputDir == null) {
      LOG.info(format("Skipping read test - dir not set in property '%s'", DIR_PROPERTY));
      return;
    }

    File info = new File(outputDir, INFO_FILE);
    File data = new File(outputDir, DATA_DIR);
    JsonParser parser = new JsonParser();
    List<Map<String, Object>> translatedRecords =
      newArrayList(parser.parse(new FileInputStream(info)));
    try (Connection conn = drill.getConnection()) {
      setSession(conn, "`exec.errors.verbose` = true");
      setSession(conn, "`store.format`='parquet'");
      String from = format("FROM dfs.`%s`", data.getPath());
      ResultSet count = conn.createStatement().executeQuery("SELECT count(*) " + from);
      assertTrue("No data (but the data dir exists) from: " + data.getPath(), count.next());
      assertEquals(translatedRecords.size(), count.getInt(1));
      count.close();

      for (Map<String, Object> record : translatedRecords) {
        String select =
          format("SELECT " + on(",").join(limit(cycle("`%s`"), record.size())),
            record.keySet().toArray(new String[0]));
//        select = "SELECT `metrictype`,`n313150832`,`companykey`,`timestamp` from dfs
// .`/var/tmp/drill/output/`";
        select = select + " " + from;
        select =
          select + format(" WHERE companykey='%s'", record.get(AvroSchemaEncoder.ORG_ID_KEY));
        ResultSet fields = conn.createStatement().executeQuery(select);
        assertTrue(fields.next());
        int i = 1;
        for (Map.Entry<String, Object> field : record.entrySet()) {
          assertEquals(fields.getObject(field.getKey()), field.getValue());
        }
        assertFalse("Still more rows for query: " + select, fields.next());
      }
    }
  }

  private void setSession(Connection conn, String stmt) throws SQLException {
    conn.createStatement().execute("ALTER SESSION SET " + stmt);
  }
}
