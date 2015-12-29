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
package com.pivotal.gemfirexd.ddl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.internal.engine.Misc;

public class EventCallBackFactorImpl implements EventCallback {

  private String foreigntablename;
  private int factor;
  private Connection conn;
  private PreparedStatement ps;

  public EventCallBackFactorImpl() {
    
  }
  
  public void close() throws SQLException {
  }

  public void init(String initStr) throws SQLException {
    String[] arr = initStr.split(":");
    this.foreigntablename = arr[0];
    this.factor = Integer.parseInt(arr[1]);
    this.conn = TestUtil.getConnection();
    this.ps = this.conn.prepareStatement("insert into "+this.foreigntablename+" values(?, ?, ?)");
  }

  public void onEvent(Event event) throws SQLException {
    String dmlString = event.getDMLString();
    assert(dmlString != null);
    int size = event.getNewRow().size();
    if (size != 3) {
      throw new IllegalArgumentException("expected the size of the new row to be 3");
    }
    int one = ((Integer)event.getNewRow().get(0)).intValue() * this.factor;
    int two = ((Integer)event.getNewRow().get(1)).intValue() * this.factor;
    int three = ((Integer)event.getNewRow().get(2)).intValue() * this.factor;
    this.ps.setInt(1, one);
    this.ps.setInt(2, two);
    this.ps.setInt(3, three);
    
    try {
      this.ps.executeUpdate();   
    }
    catch(SQLException sqle) {
      if (!(event.getType() == Type.AFTER_INSERT)) {
        throw sqle;
      }
    }
  }

}
