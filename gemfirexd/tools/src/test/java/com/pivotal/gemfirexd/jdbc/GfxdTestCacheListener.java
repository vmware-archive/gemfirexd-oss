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
package com.pivotal.gemfirexd.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;

public class GfxdTestCacheListener implements EventCallback {

  Logger logger;

  public GfxdTestCacheListener() {
  }

  public void close() throws SQLException {
    System.out.println("GfxdTestCacheListener close called");
  }

  public void init(String initStr) throws SQLException {
    logger = Logger.getLogger(ClientSharedUtils.LOGGER_NAME);
  }

  public void onEvent(Event event) throws SQLException {
    LogRecord record = new LogRecord(Level.INFO, "JavaUtilLogger Hello {0}");
    record.setParameters(new Object[] {"Neeraj"});
    logger.log(record);
    List v = event.getNewRow();
    if (v != null) {
      int size = v.size();
      for(int i=0; i<size; i++) {
        Object o = v.get(i);
        record.setMessage("List.get( {0} ) = {1}");
        record.setParameters(new Object[] {Integer.valueOf(i), o});
        logger.log(record);
      }
    }
    v = event.getOldRow();
    if (v != null) {
      int size = v.size();
      for(int i=0; i<size; i++) {
        v.get(i);
      }
    }
    event.getResultSetMetaData();
    event.getType();
  }
}
