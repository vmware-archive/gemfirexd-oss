/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.pivotal.pxf.fragmenters;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.gfxd.util.TestDataHelper;
import com.pivotal.pxf.gfxd.util.TestGemFireXDManager;
import com.pivotal.pxf.plugins.gemfirexd.util.GemFireXDManager;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;

public class FragmenterJUnitTest extends TestCase {

  public FragmenterJUnitTest() {
  }

  public FragmenterJUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
  }

  public void tearDown() throws Exception {
    shutdownGemFireXDLoner(null);
  }

  public void testDummy() throws Exception {
    
  }

  public void _testGetFragmentsWithTestInputFormat() throws Exception {
    int tableIndex = 0;
    String tableName = TestDataHelper.TABLE_NAMES[tableIndex];
    //String inputDataPath = 
    InputSplit[] fs = TestDataHelper.getSplits(tableName);
/*
    InputData inputData = new InputData(TestDataHelper.populateInputDataParam(tableIndex), null);
    GemFireXDFragmenter fragmenter = new GemFireXDFragmenter(inputData,
        new TestGemFireXDManager(inputData));

    FragmentsOutput fragments = fragmenter.GetFragments();
    verifyFragments(fs, fragments);*/
  }

  /**
   * TODO
   * @throws Exception
   */
  public void _testGetFragmentsWithInputFormat() throws Exception {
    GemFireXDManager hs = null;
    try {
      int tableIndex = 2;
/*      GemFireXDFragmenter fragmenter = new GemFireXDFragmenter(new InputData(
          TestDataHelper.populateInputDataParam(tableIndex), null));
      FragmentsOutput fragments = fragmenter.GetFragments();
      verifyFragments(
          TestDataHelper.getSplits(TestDataHelper.TABLE_NAMES[tableIndex]),
          fragments);*/
    } finally {
      if (hs != null) {
        hs.shutdown();
      }
    }
  }

  private void verifyFragments(InputSplit[] fs, List<Fragment> fragments) throws Exception {
    log("Total fragments [expected, actual]: " + fs.length + ", " + fragments.size());
    assertEquals(fs.length, fragments.size());

    for (int i = 0; i < fs.length; i++) {
      CombineFileSplit split = (CombineFileSplit) fs[i];
      Fragment frag = fragments.get(i);

      log("Number of hosts hosting the fragment [expected, actual]: " + fs[i].getLocations().length + ",  " +  frag.getReplicas().length);
      assertEquals(fs[i].getLocations().length, frag.getReplicas().length);

      log("Fragment source name [expected, actual]: " + split.getPath(0).toString() +  ",  " + frag.getSourceName());
      assertEquals(split.getPath(0).toString(), "/" + frag.getSourceName());

      for (int j = 0; j < frag.getReplicas().length; j++) {
        log("Fragment host [expected, actual]: " + fs[i].getLocations()[j] + ",  " + frag.getReplicas()[j]);
        assertEquals(fs[i].getLocations()[j], frag.getReplicas()[j]);

        log(" User data [expected, actual]: " + null + ",  " + frag.getUserData());
        assertEquals(null, frag.getUserData());
      }
    }
  }

  private static void log(String str) {
    System.out.println(str);    
  }

  public static boolean shutdownGemFireXDLoner(Properties props) {
    try {
      if (props == null) {
        props = new Properties();
        props.setProperty("mcast-port", "0");
      }
      FabricService service = FabricServiceManager
          .currentFabricServiceInstance();
      if (service != null) {
        service.stop(props);
      }
    } catch (SQLException sqle) {
      // TODO:
      return false;
    }
    return true;
  }
}
