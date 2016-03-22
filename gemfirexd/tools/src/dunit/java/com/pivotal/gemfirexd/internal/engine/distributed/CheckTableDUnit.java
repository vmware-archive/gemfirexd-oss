package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import io.snappydata.test.dunit.VM;

/**
 * Created by shirishd on 15/3/16.
 */
public class CheckTableDUnit extends DistributedSQLTestBase {

  public CheckTableDUnit(String name) {
    super(name);
  }

//  @Override
//  public String reduceLogging() {
//    return "fine";
//  }


  public void testLocalIndexConsistency() throws Exception {
    // start some servers
    startVMs(1, 2, 0, null, null);

    // Start network server on the VMs
    final int netPort1 = startNetworkServer(1, null, null);

    Connection conn = TestUtil.getNetConnection(netPort1, null, null);
    // for partitioned table
    testLocalIndexConsistency(conn, false);
    // for replicated table
    testLocalIndexConsistency(conn, true);
  }

  private void testLocalIndexConsistency(Connection conn,
      boolean isReplicated) throws SQLException {
    Statement st = conn.createStatement();
    String createTableDDL = null;

    if (isReplicated) {
      createTableDDL = "CREATE TABLE TEST.TABLE1 (COL1 INT, COL2 INT," +
          " COL3 VARCHAR(10)) REPLICATE PERSISTENT";
    } else {
      createTableDDL = "CREATE TABLE TEST.TABLE1 (COL1 INT, COL2 INT," +
          " COL3 VARCHAR(10)) PARTITION BY RANGE (COL1) (VALUES BETWEEN" +
          " 0 AND 50, VALUES BETWEEN 50 AND 100) REDUNDANCY 1";
    }

    st.execute(createTableDDL);
    st.execute("CREATE INDEX TEST.IDX1 ON TEST.TABLE1(COL2)");
    PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST.TABLE1" +
        " VALUES(?, ?, ?)");
    for (int i = 0; i < 100; i++) {
      ps.setInt(1, i);
      ps.setInt(2, i);
      ps.setString(3, "" + i);
      ps.addBatch();
    }
    ps.executeBatch();

    ResultSet rs0 = st.executeQuery("select count(*) from test.TABLE1");
    assertTrue(rs0.next());
    assertEquals(100, rs0.getInt(1));
    rs0.close();

    // should not throw exception
    rs0 = st.executeQuery("VALUES SYS.CHECK_TABLE_EX('TEST', 'TABLE1')");

    VM serverVM1 = serverVMs.get(0);
    VM serverVM2 = serverVMs.get(1);

    // delete an entry from the index to make it inconsistent with the base table
    invokeInVM(serverVM1, CheckTableDUnit.class, "deleteEntryFromIndex",
        new Object[]{"/TEST/TABLE1", "IDX1"});
    invokeInVM(serverVM2, CheckTableDUnit.class, "deleteEntryFromIndex",
        new Object[]{"/TEST/TABLE1", "IDX1"});
    try {
      ResultSet rs1 = st.executeQuery("VALUES SYS.CHECK_TABLE_EX('TEST', " +
          "'TABLE1')");
      fail("SYS.CHECK_TABLE_EX should have thrown an exception");
    } catch (SQLException se1) {
      if (!se1.getSQLState().equals("X0Y55")) {
        throw se1;
      }
    }

    st.execute("DROP INDEX TEST.IDX1");

    // create another index
    st.execute("CREATE INDEX TEST.IDX2 ON TEST.TABLE1(COL2)");
    // update an entry in index and put a incorrect value to make index
    // inconsistent with the base table
    invokeInVM(serverVM1, CheckTableDUnit.class, "updateEntryInIndex",
        new Object[]{"/TEST/TABLE1", "IDX2"} );
    invokeInVM(serverVM2, CheckTableDUnit.class, "updateEntryInIndex",
        new Object[]{"/TEST/TABLE1", "IDX2"} );
    try {
      ResultSet rs1 = st.executeQuery("VALUES SYS.CHECK_TABLE_EX('TEST'," +
          " 'TABLE1')");
      fail("SYS.CHECK_TABLE_EX should have thrown an exception");
    } catch (SQLException se2) {
      if (!se2.getSQLState().equals("X0X61")) {
        throw se2;
      }
    }

    st.execute("DROP TABLE TEST.TABLE1");
  }

  public static void logSize(String regionPath, String index) {
    LocalRegion r = (LocalRegion)Misc.getRegion(regionPath, false, false);
    GfxdIndexManager sqlim = (GfxdIndexManager)r.getIndexUpdater();
    List<GemFireContainer> list = sqlim.getAllIndexes();
    getGlobalLogger().info(
        "list of index containers are: " + Arrays.toString(list.toArray()));
    for (GemFireContainer gfc : list) {
      if (((String)gfc.getName()).contains(index)) {
        getGlobalLogger().info("size of the index is " + gfc.getSkipListMap().size());
      }
    }
  }

  // deletes an entry from the local index to make it inconsistent with the
  // base table
  public static void deleteEntryFromIndex(String regionPath, String index) {
    LocalRegion r = (LocalRegion)Misc.getRegion(regionPath, false, false);
    GfxdIndexManager sqlim = (GfxdIndexManager)r.getIndexUpdater();
    List<GemFireContainer> list = sqlim.getAllIndexes();
    getGlobalLogger().info(
        "list of index containers are: " + Arrays.toString(list.toArray()));
    for (GemFireContainer gfc : list) {
      if (((String)gfc.getName()).contains(index)) {
        // just delete any entry
        Iterator<Object> keys = gfc.getSkipListMap().keySet().iterator();
        Object k = keys.next();
        gfc.getSkipListMap().remove(k);
      }
    }
  }

  // updates an entry in the local index such that value associated with a
  // key is incorrect
  public static void updateEntryInIndex(String regionPath, String index) {
    LocalRegion r = (LocalRegion)Misc.getRegion(regionPath, false, false);
    GfxdIndexManager sqlim = (GfxdIndexManager)r.getIndexUpdater();
    List<GemFireContainer> list = sqlim.getAllIndexes();
    getGlobalLogger().info(
        "list of index containers are: " + Arrays.toString(list.toArray()));
    for (GemFireContainer gfc : list) {
      if (((String)gfc.getName()).contains(index)) {
        // just corrupt an entry
        Map.Entry first = gfc.getSkipListMap().firstEntry();
        Map.Entry last = gfc.getSkipListMap().lastEntry();

        getGlobalLogger().info("e1 =" + first + " e2 =" + last);
        gfc.getSkipListMap().put(first.getKey(), last.getValue());
      }
    }
  }


}
