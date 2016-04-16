package io.fineo.drill.rule;

import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.test.rule.TestOutput;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.SingleConnectionCachingFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple test to ensure that drill rules work as expected
 */
public class TestDrill {

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  @BeforeClass
  public static void setup() {
    Driver.load();
  }

  @Test
  public void testReadWrite() throws Exception {
    // writer a simple json file
    Map<String, Object> json = new HashMap<>();
    json.put("a", "c");

    File tmp = folder.newFolder("drill");
    File out = new File(tmp, "test.json");
    JSON j = JSON.std;
    j.write(json, out);

    try (Connection conn = drill.getConnection()) {
      conn.createStatement().execute("ALTER SESSION SET `store.format`='json'");
      String select = String.format("SELECT * FROM dfs.`%s`", out.getPath());
      ResultSet results = conn.createStatement().executeQuery(select);
      assertTrue(results.next());
      assertEquals(json.get("a"), results.getString("a"));
      assertEquals(1, results.getMetaData().getColumnCount());
    }
  }
}
