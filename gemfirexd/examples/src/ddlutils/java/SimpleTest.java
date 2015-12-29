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
import java.sql.*;
import java.io.*;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.SqlBuilder;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.PlatformUtils;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.io.DatabaseIO;

public class SimpleTest {


  public static void main(String[] args) throws Exception {

    try {
	  java.util.Properties p = new java.util.Properties();
      Connection conn = null;
        conn =
 		  DriverManager.getConnection("jdbc:gemfirexd://localhost:1527", "app", "app");
// 		  DriverManager.getConnection("jdbc:gemfirexd://camus.vmware.com:1527", "app", "app");

// 		  DriverManager.getConnection("jdbc:timesten:client:sampledbCS_1121", "appuser", "appuser");
// 		  DriverManager.getConnection("jdbc:gemfirexd:;gemfire.mcast-port=33666;gemfire.roles=gemfirexd.client", p);
//		DriverManager.getConnection("jdbc:gemfire:gemfirexd:;gemfire.mcast-port=0;gemfire.locators=localhost[4444];gemfire.roles=gemfirexd.client", p);
//         DriverManager.getConnection("jdbc:gemfire:gemfirexd:;gemfire.roles=gemfirexd.client;gemfire.mcast-port=0;gemfire.locators=localhost[4444]", p);
//         DriverManager.getConnection("jdbc:gemfire:gemfirexd:;gemfire.mcast-port=0;gemfire.locators=localhost[4444]" + ";create=true", p);
      // Do something with the Connection


	  String dbtype = (new PlatformUtils()).determineDatabaseType("com.pivotal.gemfirexd.jdbc.ClientDriver", "jdbc:gemfirexd://localhost:1527");
	  System.out.println("DB TYPE = " + dbtype);
	  //Platform platform = PlatformFactory.createNewPlatformInstance("com.pivotal.gemfirexd.jdbc.ClientDriver", "jdbc:gemfirexd://localhost:1527");
	  Platform platform = PlatformFactory.createNewPlatformInstance("GemFireXD");

      Database db = platform.readModelFromDatabase(conn, "model");
      //new DatabaseIO().write(db, "DBModel");

          FileWriter writer = new FileWriter(new File("DBModel.sql"));

            platform.setScriptModeOn(true);
            if (platform.getPlatformInfo().isSqlCommentsSupported())
            {
                // we're generating SQL comments if possible
                platform.setSqlCommentsOn(true);
            }
            platform.getSqlBuilder().setWriter(writer);
            platform.getSqlBuilder().createTables(db, true);
            writer.close();
            System.out.println("Written schema SQL to DBModel.sql" );



    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }


}
