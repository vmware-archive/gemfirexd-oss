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
package com.pivotal.gemfirexd.internal.engine.management;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.RegionMXBean;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;

import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.VM;

/**
 * 
 * @author rishim
 * @since  gemfireXD1.3
 */
public class HDFSRegionManagementDUnit extends DistributedSQLTestBase {

  private static final long serialVersionUID = 1L;
  
  private static final String REGION_NAME = "/APP/BIGTABLE1";

  public HDFSRegionManagementDUnit(String name) {
    super(name);
  }

  public void testHDFSRegionCount() throws Exception {

    // Start one client a two servers
    Properties serverInfo = new Properties();
    serverInfo.setProperty("gemfire.enable-time-statistics", "true");
    serverInfo.setProperty("gemfirexd.tableAnalyticsUpdateIntervalSeconds", "1");
    serverInfo.setProperty("statistic-sample-rate", "100");
    serverInfo.setProperty("statistic-sampling-enabled", "true");

    startServerVMs(1, 0, GfxdMemberMBeanDUnit.grp1, serverInfo);

  
    final VM serverVM0 = this.serverVMs.get(0); // Server Started as a
    

    Properties info = new Properties();
    info.setProperty("host-data", "false");
    info.setProperty("gemfire.enable-time-statistics", "true");
    
    // start a client, register the driver.
    startClientVMs(1, 0, null, info);

    final File homeDirFile = new File(".", "myhdfs");
    final String homeDir = homeDirFile.getAbsolutePath();

    // enable StatementStats for all connections in this VM
    System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

    Connection conn = TestUtil.getConnection(info);
    Statement st = conn.createStatement();
    ResultSet rs = null;

    checkDirExistence(homeDir);
    st.execute("create hdfsstore myhdfs namenode 'localhost' homedir '" +
        homeDir + "' BatchTimeInterval 100 milliseconds ");
    st.execute("create table app.bigtable1 (big_id INT NOT NULL PRIMARY KEY , big_date DATE NOT NULL, big_data VARCHAR(2000)) "
        + "PARTITION BY PRIMARY KEY EVICTION BY CRITERIA ( big_id < 300000 ) EVICTION FREQUENCY 180 SECONDS hdfsstore (myhdfs)");

  
    for (int i = 0; i < 1000; i++) {
      st.execute("insert into app.bigtable1 values (" + i + "," + "'1/2/2012'" + ","+ "'XXXX')");
    }
    
    for (int i = 0; i < 500; i++) {
      st.execute("delete from app.bigtable1 where big_id="+i);
    }
    
    pause(10 * 1000);
    
    
    long estimate2 = (Long)serverVM0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return estimateRegionSize(true);
      }
    });

    
    st.execute("drop table app.bigtable1");
    st.execute("drop hdfsstore myhdfs");
    delete(homeDirFile);
    stopVMNums(1, -1);
  }

  
  private long estimateRegionSize(boolean validate){
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    RegionMXBean bean = service.getLocalRegionMBean(REGION_NAME);
    
    long entryCount = bean.getEntryCount();
    Region r = cache.getRegion(REGION_NAME);
    LocalRegion lr = (LocalRegion) r;
    long estimate = 0;
    // Looping for 11 times to remove the probablilty of skipping stats
    // update. See
    // getEstimatedSizeForHDFSRegion.getEstimatedSizeForHDFSRegion for the
    // skip code.
    for (int i = 0; i < 11; i++) {
      estimate = bean.getEstimatedSizeForHDFSRegion();
    }
    //estimate = lr.sizeEstimate();
    System.out.println(" Estimate = "+estimate);
    if(validate){
     /* double err = Math.abs(estimate - 500) / (double) 500;
      assertTrue(err < 0.2);*/
      
      assertTrue(estimate > 0 );
    }

    return estimate;
  }


  // Assume no other thread creates the directory at the same time
  private void checkDirExistence(String path) {
    File dir = new File(path);
    if (dir.exists()) {
      delete(dir);
    }
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}
