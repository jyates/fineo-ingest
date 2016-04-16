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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Simple test to ensure that drill rules work as expected
 */
public class TestDrill {

  private static final long NUM_RECORDS = 1;
  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);
  private BufferAllocator allocator;

  @Test
  public void testReadWrite() throws Exception {
    // writer a simple json file
    Map<String, Object> json = new HashMap<>();
    json.put("a", "b");

    File tmp = folder.newFolder("drill");
    File out = new File(tmp, "test.json");
    JSON j = JSON.std;
    j.write(j, out);

    try (DrillClient client = drill.getClient()) {
      String select = String.format("SELECT count(*) FROM dfs.`%s`", out.getPath());
      List<QueryDataBatch> results = client.runQuery(UserBitShared.QueryType.SQL, select);
      RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator(client));

      for (QueryDataBatch batch : results) {
        batchLoader.load(batch.getHeader().getDef(), batch.getData());

        if (batchLoader.getRecordCount() <= 0) {
          continue;
        }

        BigIntVector countV =
          (BigIntVector) batchLoader.getValueAccessorById(BigIntVector.class, 0).getValueVector();
        assertTrue("Total of " + NUM_RECORDS + " records expected in count",
          countV.getAccessor().get(0) == NUM_RECORDS);

        batchLoader.clear();
        batch.release();
      }
    }
  }

  private BufferAllocator getAllocator(DrillClient client) {
    if (this.allocator == null) {
      allocator = RootAllocatorFactory.newRoot(client.getConfig());
    }
    return this.allocator;
  }
}
