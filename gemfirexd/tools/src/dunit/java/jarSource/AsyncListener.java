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

package jarSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.callbacks.Event;

/**
 * @author kneeraj
 * 
 */
public class AsyncListener implements AsyncEventListener {

  private int eventCnt;

  private String schemaName;

  private String tableName;

  private ArrayList<Integer> list;

  private Throwable exception;

  public boolean processEvents(List<Event> events) {
    eventCnt = events.size();
    for (Event e : events) {
      if (schemaName == null) {
        schemaName = e.getSchemaName();
        tableName = e.getTableName();
      }
      ResultSet rs = e.getPrimaryKeysAsResultSet();
      try {
        rs.next();
        list.add(rs.getInt(1));
      } catch (Throwable t) {
        exception = t;
      }
    }
    return false;
  }

  public void close() {
  }

  public void init(String initParamStr) {
    list = new ArrayList<Integer>();
  }

  public void start() {
  }

  public ArrayList<Integer> getList() {
    return this.list;
  }

  public Throwable getThrowable() {
    return this.exception;
  }
  
  /* to build myAsyncJar.jar
  public String toString() {
    return "OLD";
  }*/

  /* to build myNewAsyncJar.jar */
  public String toString() {
    return "NEW";
  }
}
