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
package com.pivotal.pxf.accessors;

import java.util.List;

import junit.framework.TestCase;

import com.pivotal.pxf.fragmenters.FragmenterJUnitTest;
import com.pivotal.pxf.gfxd.TestRecordReader.KeyClass;
import com.pivotal.pxf.gfxd.TestRecordReader.ValueClass;
import com.pivotal.pxf.gfxd.util.TestDataHelper;
import com.pivotal.pxf.gfxd.util.TestGemFireXDManager;

public class AccessorJUnitTest extends TestCase {

  public AccessorJUnitTest() {
  }

  public AccessorJUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    
  }

  public void tearDown() throws Exception {
    FragmenterJUnitTest.shutdownGemFireXDLoner(null);
  }

  public void testDummy() throws Exception {
    
  }

  public void _testLoadNextObject() throws Exception {
    int tableIndex = 2, splitIndex = 0;
    String tableName = TestDataHelper.TABLE_NAMES[tableIndex];
/*    InputData inputData = new InputData(TestDataHelper.populateInputDataParam(
        tableIndex, 0, 1, String.valueOf(splitIndex)), null);

    List<Object[]> expectedRecords = TestDataHelper
        .getSplitRecords(TestDataHelper.getSplits(tableName)[splitIndex]);

    // Create an accessor instance
    GemFireXDAccessor acc = new GemFireXDAccessor(inputData,
        new TestGemFireXDManager(inputData));

    // call open
    acc.openForRead();

    OneRow row;
    int index = 0;
    // call readNextObject() in for loop
    while ((row = acc.readNextObject()) != null) {
      Object key = TestDataHelper.deserialize(tableName,
          (byte[]) ((KeyClass) row.getKey()).getValue());
      // verify row key
      assertEquals(expectedRecords.get(index)[0], key);
      Object[] fields = TestDataHelper.deserializeArray(tableName,
          (byte[]) ((ValueClass) row.getData()).getValue());
      int i = 0;
      for (Object field : fields) {
        System.out.print(field + " ");
        // verify row data
        assertEquals(expectedRecords.get(index)[i], field);
        ++i;
      }
      System.out.println("");
      ++index;
    }

    // call close
    acc.closeForRead();*/
  }
}
