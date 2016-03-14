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
package com.gemstone.gemfire.distributed.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import junit.framework.TestCase;

public class ProductUseLogJUnitTest extends TestCase {

  public void testBasics() throws Exception {
//    Properties p = new Properties();
//    int mcport = AvailablePortHelper.getRandomAvailableUDPPort();
//    p.put(DistributionConfig.MCAST_PORT_NAME, mcport);
//    p.put(DistributionConfig.MCAST_TTL_NAME, "0");
//    InternalDistributedSystem system = (InternalDistributedSystem)DistributedSystem.connect(p);
    File logFile = new File("ProductUseLogTest_testBasics.log");
    if (logFile.exists()) {
      logFile.delete();
    }
    ProductUseLog log = new ProductUseLog(logFile);
    assertTrue(logFile.exists());
    log.log("test message");
    log.close();
    log.log("shouldn't be logged");
    log.reopen();
    log.log("test message");
    log.close();
    BufferedReader reader = new BufferedReader(new  FileReader(logFile));
    try {
      String line = reader.readLine();
      assertTrue(line.length() == 0);
      line = reader.readLine();
      assertTrue("expected first line to contain 'test message'", line.contains("test message"));

      line = reader.readLine();
      assertTrue(line.length() == 0);
      line = reader.readLine();
      assertTrue("expected second line to contain 'test message'", line.contains("test message"));

      line = reader.readLine();
      assertTrue("expected only two non-empty lines in the file", line == null);
    } finally {
      reader.close();
    }
  }

  public void testSizeLimit() throws Exception {
    long oldMax = ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE;
    ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = 2000L;
    try {
      File logFile = new File("ProductUseLogTest_testSizeLimit.log");
      ProductUseLog log = new ProductUseLog(logFile);
      try {
        String logEntry = "log entry";
        for (long i=0; i<ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE; i++) {
          log.log(logEntry);
          assertTrue("expected " + logFile.getPath() + " to remain under "+ 
              ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE + " bytes in length",
              logFile.length() < ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE);
        }
      } finally {
        log.close();
      }
    } finally {
      ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = oldMax;
    }
  }
}
