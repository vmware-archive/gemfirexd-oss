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
//
//  ToursDBUtil.java
//
//  Created by Eric Zoerner on 2009-10-08.
package com.pivotal.gemfirexd;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import junit.framework.Assert;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Utilities for creating and loading the example ToursDB data
 * for use in unit tests and quickstart tests
 */
public class ToursDBUtil {
  
  /**
   * Create and load the ToursDB data
   */
  public static void createAndLoadToursDB(Connection conn) throws SQLException {
    String[] quickstartScripts = new String[] {
      "ToursDB_schema.sql",
      "create_colocated_schema.sql",
      "loadCOUNTRIES.sql",
      "loadCITIES.sql",
      "loadAIRLINES.sql",
      "loadFLIGHTS1.sql",
      "loadFLIGHTS2.sql",
      "loadFLIGHTAVAILABILITY1.sql",
      "loadFLIGHTAVAILABILITY2.sql",      
    };
    File qsDir = getQuickstartDir();
    
    String[] scriptPaths = new String[quickstartScripts.length];
    for (int i = 0; i < scriptPaths.length; i++) {
      scriptPaths[i] = new File(qsDir, quickstartScripts[i]).toString();
    }
    // [sumedh] the new executeSQLScripts in GemFireXDUtils does not require
    // an EmbedConnection
    /*
    Assert.assertTrue("connection expected to be an EmbedConnection but was"
                      + conn.getClass().getName(),
                      conn instanceof EmbedConnection);
    */
    executeSQLScripts(conn, scriptPaths);
    
    Statement s = conn.createStatement();
    Assert.assertEquals(87, JDBC.assertDrainResults(s.executeQuery("SELECT * FROM CITIES")));    
  }
  
  private static void executeSQLScripts(Connection conn, String[] scripts) {
    try {
      /*
       ((com.gemstone.gemfire.internal.PureLogWriter)TestUtil.getLogger()).setLevel(com.gemstone.gemfire.internal.LogWriterImpl.FINE_LEVEL);
       */
      // invoke a private method using reflection
      /*
      java.lang.reflect.Method method =
           EmbedConnection.class.getDeclaredMethod("executeSQLScripts",
                                                   String[].class,
                                                   com.gemstone.gemfire.LogWriter.class);
      method.setAccessible(true);
      method.invoke(conn, scripts, TestUtil.getLogger());
      */
      GemFireXDUtils
          .executeSQLScripts(conn, scripts, false, TestUtil.getLogger(), null, null, false);
      conn.commit();
    } catch (Exception e) {
      AssertionError ae = new AssertionError("unexpected exception");
      ae.initCause(e);
      throw ae;
    }
  }

  public static File getQuickstartDir() {
    String productDir = System.getProperty("GEMFIREXD");
    return new File(productDir, "quickstart");
  }
}
