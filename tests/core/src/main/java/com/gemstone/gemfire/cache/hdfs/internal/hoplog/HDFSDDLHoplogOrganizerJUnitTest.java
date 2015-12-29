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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.ArrayList;

import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * Tests the DDLHoplog using internal classes
 * 
 * @author hemantb
 *
 */
public class HDFSDDLHoplogOrganizerJUnitTest extends BaseHoplogTestCase {
  
  /**
   * Tests flush operation. Flushes the data a couple of times and checks its validity
   * Also verifies the API getCurrentHoplogTimeStamp
   */
  public void testFlush() throws Exception {
    
    long timeBeforeFirstHoplog = System.currentTimeMillis();
    DDLHoplogOrganizer organizer = new DDLHoplogOrganizer(regionManager.getStore());
    
    // flush and create hoplog
    ArrayList<byte[]> values = new ArrayList<byte[]>();
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    for (int i = 0; i < 2; i++) {
      values.add(BlobHelper.serializeToBlob("sqlText-" + i));
      keys.add(BlobHelper.serializeToBlob( i));
    }
    
    organizer.flush(keys.iterator(), values.iterator());
    
    ArrayList<byte[]> listddl = organizer.getDDLStatementsForReplay().getDDLStatements() ;
    
    assertEquals(2, listddl.size());
    assertEquals("sqlText-0", BlobHelper.deserializeBlob(listddl.get(0)));
    assertEquals("sqlText-1", BlobHelper.deserializeBlob(listddl.get(1)));
    
    // flush and create hoplog
    values = new ArrayList<byte[]>();
    keys = new ArrayList<byte[]>();
    for (int i = 2; i < 4; i++) {
      values.add(BlobHelper.serializeToBlob("sqlText-" + i));
      keys.add(BlobHelper.serializeToBlob( i));
    }
      
    long timeBeforeSecondHoplog = System.currentTimeMillis();
    organizer.flush(keys.iterator(), values.iterator());
     
    long timeAfterSecondHoplog = System.currentTimeMillis();
    
    listddl = organizer.getDDLStatementsForReplay().getDDLStatements() ;
      
    assertEquals(2, listddl.size());
    assertEquals("sqlText-2", BlobHelper.deserializeBlob(listddl.get(0)));
    assertEquals("sqlText-3", BlobHelper.deserializeBlob(listddl.get(1)));
    
    long currentHoplogTime  = organizer.getCurrentHoplogTimeStamp();
    
    assertTrue(currentHoplogTime <= timeAfterSecondHoplog);
    assertTrue(currentHoplogTime >= timeBeforeFirstHoplog);
    assertTrue(currentHoplogTime >= timeBeforeSecondHoplog);
    
}
  
}

