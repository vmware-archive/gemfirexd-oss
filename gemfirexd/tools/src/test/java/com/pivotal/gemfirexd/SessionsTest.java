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
package com.pivotal.gemfirexd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

import org.w3c.dom.Element;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class SessionsTest extends JdbcTestBase {
  public SessionsTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(SessionsTest.class));
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testSessionCountAndDetails() throws Exception {
    
    Connection[] connections = new Connection[5];
    try {
      setupConnection();
      
      int port = TestUtil.startNetserverAndReturnPort();
      
      for(int i = connections.length - 1; i >=0; i--) {
        connections[i] = TestUtil.getNetConnection(port, null, null);
      }
      
      //Connection conn = getConnection();
      Connection conn = TestUtil.getNetConnection(port, null, null);
      
      ResultSet r = conn.createStatement().executeQuery("Select * from sys.sessions");
      Element resultElement = Misc.resultSetToXMLElement(r, false, false);
      String resultStr = Misc.serializeXML(resultElement);
      getLogger().info(resultStr);
      
    } finally {
      TestUtil.shutDown();
    }
  }
  
}
