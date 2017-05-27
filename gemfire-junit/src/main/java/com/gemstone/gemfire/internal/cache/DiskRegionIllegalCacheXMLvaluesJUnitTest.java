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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;

/**
 * This test tests Illegal arguements being passed to 
 * create disk regions. The creation of the DWA object should
 * throw a relevant exception if the arguements specified are incorrect.
 * 
 * @author mbid
 *
 */
public class DiskRegionIllegalCacheXMLvaluesJUnitTest extends TestCase
{

  
  
  public DiskRegionIllegalCacheXMLvaluesJUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }


  public void createRegion(String path)
  {
    DistributedSystem ds = null;
    try {
      boolean exceptionOccured = false;
      File dir = new File("testingDirectoryForXML");
      dir.mkdir();
      dir.deleteOnExit();
      Properties props = new Properties();
      int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
      props.setProperty("mcast-port", String.valueOf(unusedPort));
      String localPath =  System.getProperty("JTESTS")+"/../../"+path;
      
      props.setProperty("cache-xml-file", localPath);
      ds = DistributedSystem.connect(props);
      try {
       
        CacheFactory.create(ds);
      }
      catch (IllegalArgumentException ex) {
        exceptionOccured = true;
        System.out.println("Received expected IllegalArgumentException:"+ex.getMessage());
      }
      catch (CacheXmlException ex) {
         exceptionOccured = true;
         System.out.println("Received expected CacheXmlException:"+ex.getMessage());
      }
      catch (Exception e) {
        e.printStackTrace();
        fail("test failed due to " + e);
      }

      if (!exceptionOccured) {
        fail(" exception did not occur although was expected");
      }
    }
    finally {
      if (ds != null && ds.isConnected()) {
        ds.disconnect();
        ds = null;
      }
    }
  }
 
  
  /**
   * test Illegal max oplog size
   */

  public void testMaxOplogSize()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_max_oplog_size.xml");
  }

  public void testSynchronous()
  {}

  public void testIsRolling()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_roll_oplogs_value.xml");
  }

  public void testDiskDirSize()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_dir_size.xml");
  }

  public void testDiskDirs()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_dir.xml");
  }

  public void testBytesThreshold()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_bytes_threshold.xml");
  }

  public void testTimeInterval()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/incorrect_time_interval.xml");
  }

  public void testMixedDiskStoreWithDiskDir()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/mixed_diskstore_diskdir.xml");
  }
  public void testMixedDiskStoreWithDWA()
  {
    createRegion("tests/lib/faultyDiskXMLsForTesting/mixed_diskstore_diskwriteattrs.xml");
  }
}
